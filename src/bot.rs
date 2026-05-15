//! Main WechatIlinkClient.

use futures_core::Stream;
use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot, Mutex, Notify, RwLock};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::cdn::CdnClient;
use crate::crypto;
use crate::error::{Result, WechatIlinkError};
use crate::protocol::{self, ILinkClient, ILinkClientOptions};
use crate::types::*;
use md5::{Digest, Md5};
use rand::Rng;
use serde_json::json;
use uuid::Uuid;

const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Lifecycle events emitted by the WeChat iLink client.
#[derive(Debug, Clone)]
pub enum WechatEvent {
    /// A new context token was observed from an incoming wire message.
    ContextObserved(WechatContext),
    /// An incoming parsed message.
    Message(IncomingMessage),
    /// The polling cursor advanced after processing a batch of messages.
    CursorAdvanced { account_key: String, cursor: String },
    /// The current auth session has expired and re-login is required.
    AuthSessionExpired { account_key: String },
    /// The SDK observed that user interaction would refresh WeChat state.
    ///
    /// The SDK does not send a reminder. Applications decide whether to notify
    /// users and what wording/channel to use.
    UserInteractionRequested {
        account_key: String,
        user_id: Option<String>,
        reason: UserInteractionReason,
    },
}

/// Message ids produced by a send operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReceipt {
    pub message_ids: Vec<String>,
    pub visible_texts: Vec<String>,
}

impl SendReceipt {
    pub fn last_message_id(&self) -> Option<&str> {
        self.message_ids.last().map(String::as_str)
    }

    fn single(message_id: String) -> Self {
        Self {
            message_ids: vec![message_id],
            visible_texts: Vec::new(),
        }
    }

    fn empty() -> Self {
        Self {
            message_ids: Vec::new(),
            visible_texts: Vec::new(),
        }
    }

    fn append(&mut self, mut other: SendReceipt) {
        self.message_ids.append(&mut other.message_ids);
        self.visible_texts.append(&mut other.visible_texts);
    }
}

/// Why the SDK suggests asking a WeChat user to interact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserInteractionReason {
    /// A stored context is nearing expiry.
    ContextExpiring {
        observed_at: SystemTime,
        expires_at: SystemTime,
        remind_before: Duration,
    },
    /// Proactive outbound sends are close to the bot-wide iLink rate limit.
    OutboundRateLimitApproaching {
        sent_count: usize,
        window: Duration,
        threshold: usize,
    },
}

/// A stream of WeChat lifecycle events.
pub struct WechatEventStream<'a> {
    inner: Pin<Box<dyn Stream<Item = Result<WechatEvent>> + Send + 'a>>,
}

impl<'a> WechatEventStream<'a> {
    fn new(stream: impl Stream<Item = Result<WechatEvent>> + Send + 'a) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }

    pub async fn next(&mut self) -> Option<Result<WechatEvent>> {
        self.inner.next().await
    }
}

impl Stream for WechatEventStream<'_> {
    type Item = Result<WechatEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Events emitted during QR login.
#[derive(Debug)]
pub enum LoginQrEvent {
    /// A new QR code should be displayed or rendered by the application.
    QrCode { content: String },
    /// Login status changed.
    StatusChanged { status: String },
    /// WeChat requires a verification code before continuing.
    NeedVerifyCode {
        prompt: String,
        responder: VerifyCodeResponder,
    },
    /// Login completed and credentials were installed into the client.
    Confirmed { credentials: Credentials },
}

/// One-shot responder for [`LoginQrEvent::NeedVerifyCode`].
#[derive(Debug)]
pub struct VerifyCodeResponder {
    tx: oneshot::Sender<Option<String>>,
}

impl VerifyCodeResponder {
    pub fn send(self, code: impl Into<String>) -> std::result::Result<(), Option<String>> {
        let code = Some(code.into());
        self.tx.send(code)
    }

    pub fn cancel(self) -> std::result::Result<(), Option<String>> {
        self.tx.send(None)
    }
}

/// A stream of QR login events.
pub struct LoginQrStream<'a> {
    inner: Pin<Box<dyn Stream<Item = Result<LoginQrEvent>> + Send + 'a>>,
}

impl<'a> LoginQrStream<'a> {
    fn new(stream: impl Stream<Item = Result<LoginQrEvent>> + Send + 'a) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }

    pub async fn next(&mut self) -> Option<Result<LoginQrEvent>> {
        self.inner.next().await
    }
}

impl Stream for LoginQrStream<'_> {
    type Item = Result<LoginQrEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Rate-limit and context-refresh policy for iLink requests.
#[derive(Debug, Clone)]
pub struct WechatRateLimitOptions {
    /// Delay used before retrying `ret=-2` / `errcode=-2`.
    pub retry_after: Duration,
    /// Number of retries after the initial rate-limited attempt.
    pub retry_attempts: usize,
    /// Rolling account window used to request proactive user interaction.
    pub interaction_window: Duration,
    /// Emit interaction event at this send count.
    pub interaction_threshold: usize,
    /// Expected context lifetime after a user WeChat message.
    pub context_ttl: Duration,
    /// Emit context-expiring interaction event this long before expiry.
    pub context_remind_before: Duration,
}

impl Default for WechatRateLimitOptions {
    fn default() -> Self {
        Self {
            retry_after: protocol::DEFAULT_RATE_LIMIT_RETRY_AFTER,
            retry_attempts: 5,
            interaction_window: Duration::from_secs(5 * 60),
            interaction_threshold: 6,
            context_ttl: Duration::from_secs(24 * 60 * 60),
            context_remind_before: Duration::from_secs(30 * 60),
        }
    }
}

#[derive(Default)]
struct AccountRateLimitState {
    sent_at: VecDeque<Instant>,
    next_allowed_at: Option<Instant>,
}

struct ContextRefreshState {
    user_id: String,
    observed_at: SystemTime,
    expires_at: SystemTime,
    reminded: bool,
}

/// Builder for [`WechatIlinkClient`].
pub struct WechatIlinkClientBuilder {
    base_url: Option<String>,
    bot_agent: Option<String>,
    ilink_app_id: Option<String>,
    route_tag: Option<String>,
    markdown_filter: bool,
    rate_limit: WechatRateLimitOptions,
    credentials: Option<Credentials>,
}

impl Default for WechatIlinkClientBuilder {
    fn default() -> Self {
        Self {
            base_url: None,
            bot_agent: None,
            ilink_app_id: None,
            route_tag: None,
            markdown_filter: true,
            rate_limit: WechatRateLimitOptions::default(),
            credentials: None,
        }
    }
}

impl WechatIlinkClientBuilder {
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    pub fn bot_agent(mut self, bot_agent: impl Into<String>) -> Self {
        self.bot_agent = Some(bot_agent.into());
        self
    }

    pub fn ilink_app_id(mut self, ilink_app_id: impl Into<String>) -> Self {
        self.ilink_app_id = Some(ilink_app_id.into());
        self
    }

    pub fn route_tag(mut self, route_tag: impl Into<String>) -> Self {
        self.route_tag = Some(route_tag.into());
        self
    }

    pub fn markdown_filter(mut self, enabled: bool) -> Self {
        self.markdown_filter = enabled;
        self
    }

    pub fn rate_limit_options(mut self, options: WechatRateLimitOptions) -> Self {
        self.rate_limit = options;
        self
    }

    pub fn rate_limit_retry_after(mut self, retry_after: Duration) -> Self {
        self.rate_limit.retry_after = if retry_after.is_zero() {
            protocol::DEFAULT_RATE_LIMIT_RETRY_AFTER
        } else {
            retry_after
        };
        self
    }

    pub fn rate_limit_retry_attempts(mut self, retry_attempts: usize) -> Self {
        self.rate_limit.retry_attempts = retry_attempts;
        self
    }

    pub fn rate_limit_max_retries(mut self, max_retries: usize) -> Self {
        self.rate_limit.retry_attempts = max_retries;
        self
    }

    pub fn context_ttl(mut self, ttl: Duration) -> Self {
        if !ttl.is_zero() {
            self.rate_limit.context_ttl = ttl;
        }
        self
    }

    pub fn context_expiry_remind_before(mut self, remind_before: Duration) -> Self {
        self.rate_limit.context_remind_before = remind_before;
        self
    }

    pub fn rate_limit_interaction_window(mut self, window: Duration) -> Self {
        if !window.is_zero() {
            self.rate_limit.interaction_window = window;
        }
        self
    }

    pub fn rate_limit_interaction_threshold(mut self, threshold: usize) -> Self {
        self.rate_limit.interaction_threshold = threshold;
        self
    }

    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn build(self) -> WechatIlinkClient {
        WechatIlinkClient::from_builder(self)
    }
}

/// WechatIlinkClient is the main entry point for the WeChat iLink protocol.
pub struct WechatIlinkClient {
    client: Arc<ILinkClient>,
    cdn: CdnClient,
    credentials: RwLock<Option<Credentials>>,
    event_tx: broadcast::Sender<WechatEvent>,
    rate_limit_states: Mutex<HashMap<String, AccountRateLimitState>>,
    context_refresh_states: Mutex<HashMap<(String, String), ContextRefreshState>>,
    rate_limit_notify: Notify,
    base_url: RwLock<String>,
    stopped: RwLock<bool>,
    rate_limit: WechatRateLimitOptions,
}

impl WechatIlinkClient {
    /// Create a default client instance.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Create a client builder.
    pub fn builder() -> WechatIlinkClientBuilder {
        WechatIlinkClientBuilder::default()
    }

    fn from_builder(builder: WechatIlinkClientBuilder) -> Self {
        let base_url = builder
            .credentials
            .as_ref()
            .map(|creds| creds.base_url.clone())
            .or(builder.base_url)
            .unwrap_or_else(|| protocol::DEFAULT_BASE_URL.to_string());
        Self {
            client: Arc::new(ILinkClient::with_options(ILinkClientOptions {
                bot_agent: builder.bot_agent,
                route_tag: builder.route_tag,
                ilink_app_id: builder.ilink_app_id,
                markdown_filter: builder.markdown_filter,
            })),
            cdn: CdnClient::new(),
            credentials: RwLock::new(builder.credentials),
            event_tx: broadcast::channel(EVENT_CHANNEL_CAPACITY).0,
            rate_limit_states: Mutex::new(HashMap::new()),
            context_refresh_states: Mutex::new(HashMap::new()),
            rate_limit_notify: Notify::new(),
            base_url: RwLock::new(base_url),
            stopped: RwLock::new(false),
            rate_limit: builder.rate_limit,
        }
    }

    /// Maximum number of QR code refresh attempts before giving up.
    const MAX_QR_REFRESH: u32 = 3;
    /// Fixed API base URL for QR code requests.
    const FIXED_QR_BASE_URL: &'static str = "https://ilinkai.weixin.qq.com";

    /// Install externally loaded credentials into this client.
    pub async fn set_credentials(&self, creds: Credentials) {
        *self.base_url.write().await = creds.base_url.clone();
        *self.credentials.write().await = Some(creds);
    }

    /// Return the currently installed credentials, if any.
    pub async fn credentials(&self) -> Option<Credentials> {
        self.credentials.read().await.clone()
    }

    /// Start QR login and return a stream of login events.
    ///
    /// Applications display [`LoginQrEvent::QrCode`], optionally answer
    /// [`LoginQrEvent::NeedVerifyCode`], and persist credentials from
    /// [`LoginQrEvent::Confirmed`].
    pub fn login_qr_stream(&self) -> LoginQrStream<'_> {
        LoginQrStream::new(async_stream::stream! {
            let base_url = self.base_url.read().await.clone();
            let mut qr_refresh_count = 0u32;
            loop {
                qr_refresh_count += 1;
                if qr_refresh_count > Self::MAX_QR_REFRESH {
                    yield Err(WechatIlinkError::Auth(format!(
                        "QR code expired {} times — login aborted",
                        Self::MAX_QR_REFRESH
                    )));
                    return;
                }

                let qr = match self.client.get_qr_code(Self::FIXED_QR_BASE_URL).await {
                    Ok(qr) => qr,
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                };
                yield Ok(LoginQrEvent::QrCode {
                    content: qr.qrcode_img_content.clone(),
                });

                let mut last_status = String::new();
                let mut current_poll_base_url = Self::FIXED_QR_BASE_URL.to_string();
                let mut pending_verify_code: Option<String> = None;
                loop {
                    let status = match self
                        .client
                        .poll_qr_status_with_verify_code(
                            &current_poll_base_url,
                            &qr.qrcode,
                            pending_verify_code.as_deref(),
                        )
                        .await
                    {
                        Ok(status) => status,
                        Err(err) => {
                            yield Err(err);
                            return;
                        }
                    };

                    if status.status != last_status {
                        last_status = status.status.clone();
                        match status.status.as_str() {
                            "scaned" => info!("QR scanned — confirm in WeChat"),
                            "expired" => warn!("QR expired — requesting new one"),
                            "confirmed" => info!("Login confirmed"),
                            "need_verifycode" => info!("QR verification code required"),
                            "verify_code_blocked" => warn!("QR verification code blocked"),
                            "binded_redirect" => warn!("QR already bound"),
                            _ => {}
                        }
                        yield Ok(LoginQrEvent::StatusChanged {
                            status: status.status.clone(),
                        });
                    }

                    if status.status == "wait" {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    if status.status == "need_verifycode" {
                        let prompt = if pending_verify_code.is_some() {
                            "QR verification code mismatch"
                        } else {
                            "QR verification code required"
                        };
                        let (tx, rx) = oneshot::channel();
                        yield Ok(LoginQrEvent::NeedVerifyCode {
                            prompt: prompt.to_string(),
                            responder: VerifyCodeResponder { tx },
                        });
                        let code = match rx.await {
                            Ok(Some(value)) => value.trim().to_string(),
                            _ => String::new(),
                        };
                        if code.is_empty() {
                            yield Err(WechatIlinkError::Auth(
                                "QR verification code was not provided".into(),
                            ));
                            return;
                        }
                        pending_verify_code = Some(code);
                        continue;
                    }

                    if status.status == "verify_code_blocked" {
                        break;
                    }

                    if status.status == "binded_redirect" {
                        yield Err(WechatIlinkError::Auth(
                            "QR login is already bound to this app".into(),
                        ));
                        return;
                    }

                    if status.status == "confirmed" {
                        let token = match status.bot_token {
                            Some(token) => token,
                            None => {
                                yield Err(WechatIlinkError::Auth("missing bot_token".into()));
                                return;
                            }
                        };
                        let creds = Credentials {
                            token,
                            base_url: status.baseurl.unwrap_or_else(|| base_url.clone()),
                            account_id: status.ilink_bot_id.unwrap_or_default(),
                            user_id: status.ilink_user_id.unwrap_or_default(),
                            saved_at: Some(chrono_now()),
                        };
                        *self.credentials.write().await = Some(creds.clone());
                        *self.base_url.write().await = creds.base_url.clone();
                        yield Ok(LoginQrEvent::Confirmed { credentials: creds });
                        return;
                    }

                    if status.status == "scaned_but_redirect" {
                        if let Some(ref host) = status.redirect_host {
                            current_poll_base_url = format!("https://{}", host);
                            info!("IDC redirect, switching polling host to {}", host);
                        } else {
                            warn!("Received scaned_but_redirect but redirect_host is missing");
                        }
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }

                    if status.status == "scaned" && pending_verify_code.is_some() {
                        pending_verify_code = None;
                    }

                    if status.status == "expired" {
                        break;
                    }

                    sleep(Duration::from_secs(2)).await;
                }
            }
        })
    }

    /// Subscribe to events emitted by polling and outbound-send bookkeeping.
    pub fn event_stream(&self) -> WechatEventStream<'static> {
        event_stream_from_receiver(self.event_tx.subscribe())
    }

    /// Start polling from an externally persisted cursor and stream resulting events.
    pub fn stream_from_cursor(
        self: Arc<Self>,
        cursor: Option<String>,
    ) -> WechatEventStream<'static> {
        let mut rx = self.event_tx.subscribe();
        let runner = Arc::clone(&self);
        let mut task =
            tokio::spawn(async move { runner.run_loop(cursor.unwrap_or_default()).await });
        let abort_on_drop = AbortOnDrop(task.abort_handle());
        WechatEventStream::new(async_stream::stream! {
            let _abort_on_drop = abort_on_drop;
            loop {
                tokio::select! {
                    biased;
                    result = &mut task => {
                        match result {
                            Ok(Ok(())) => return,
                            Ok(Err(err)) => {
                                yield Err(err);
                                return;
                            }
                            Err(err) => {
                                yield Err(WechatIlinkError::Other(format!(
                                    "wechat poll task failed: {err}"
                                )));
                                return;
                            }
                        }
                    }
                    event = rx.recv() => match event {
                        Ok(event) => yield Ok(event),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            yield Err(WechatIlinkError::Other(format!(
                                "wechat event stream lagged by {skipped} events"
                            )));
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        })
    }

    /// Emit an event to active streams.
    fn emit_event(&self, event: WechatEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Reply to an incoming message using its observed context.
    ///
    /// Returns an error if the incoming message has no context (e.g. the
    /// context_token was empty in the wire message).
    pub async fn reply(&self, msg: &IncomingMessage, text: &str) -> Result<SendReceipt> {
        let context = msg
            .context
            .as_ref()
            .ok_or_else(|| WechatIlinkError::NoContext(msg.user_id.clone()))?;
        self.send_text_with_context(context, text).await
    }

    /// Send text using an explicit [`WechatContext`].
    ///
    /// The caller is responsible for supplying a valid context. This is the
    /// preferred way to send a reply when you already hold the context from an
    /// [`IncomingMessage`].
    pub async fn send_text_with_context(
        &self,
        context: &WechatContext,
        text: &str,
    ) -> Result<SendReceipt> {
        self.send_text(&context.user_id, text, &context.context_token)
            .await
    }

    /// Send media content using an explicit [`WechatContext`].
    ///
    /// The caller is responsible for supplying a valid context.
    pub async fn send_media_with_context(
        &self,
        context: &WechatContext,
        content: SendContent,
    ) -> Result<SendReceipt> {
        self.send_content(&context.user_id, &context.context_token, content)
            .await
    }

    /// Show "typing..." indicator using an explicit [`WechatContext`].
    ///
    /// The caller is responsible for supplying a valid context.
    pub async fn send_typing_with_context(&self, context: &WechatContext) -> Result<()> {
        let (base_url, token) = self.get_auth().await?;
        let config = self
            .retry_rate_limited(|| {
                self.client
                    .get_config(&base_url, &token, &context.user_id, &context.context_token)
            })
            .await?;
        if let Some(ticket) = config.typing_ticket {
            self.retry_rate_limited(|| {
                self.client
                    .send_typing(&base_url, &token, &context.user_id, &ticket, 1)
            })
            .await?;
        }
        Ok(())
    }

    /// Reply with media content using the incoming message's observed context.
    ///
    /// Returns an error if the incoming message has no context.
    pub async fn reply_media(
        &self,
        msg: &IncomingMessage,
        content: SendContent,
    ) -> Result<SendReceipt> {
        let context = msg
            .context
            .as_ref()
            .ok_or_else(|| WechatIlinkError::NoContext(msg.user_id.clone()))?;
        self.send_media_with_context(context, content).await
    }

    /// Download media from an incoming message.
    /// Returns None if the message has no media. Priority: image > file > video > voice.
    pub async fn download(&self, msg: &IncomingMessage) -> Result<Option<DownloadedMedia>> {
        if let Some(img) = msg.images.first() {
            if let Some(ref media) = img.media {
                let data = self.cdn.download(media, img.aes_key.as_deref()).await?;
                return Ok(Some(DownloadedMedia {
                    data,
                    media_type: "image".into(),
                    file_name: None,
                    format: None,
                }));
            }
        }
        if let Some(file) = msg.files.first() {
            if let Some(ref media) = file.media {
                require_download_aes_key("file", media)?;
                let data = self.cdn.download(media, None).await?;
                return Ok(Some(DownloadedMedia {
                    data,
                    media_type: "file".into(),
                    file_name: Some(file.file_name.clone().unwrap_or_else(|| "file.bin".into())),
                    format: None,
                }));
            }
        }
        if let Some(video) = msg.videos.first() {
            if let Some(ref media) = video.media {
                require_download_aes_key("video", media)?;
                let data = self.cdn.download(media, None).await?;
                return Ok(Some(DownloadedMedia {
                    data,
                    media_type: "video".into(),
                    file_name: None,
                    format: None,
                }));
            }
        }
        if let Some(voice) = msg.voices.first() {
            if let Some(ref media) = voice.media {
                require_download_aes_key("voice", media)?;
                let data = self.cdn.download(media, None).await?;
                return Ok(Some(DownloadedMedia {
                    data,
                    media_type: "voice".into(),
                    file_name: None,
                    format: Some("silk".into()),
                }));
            }
        }
        Ok(None)
    }

    /// Download and decrypt a raw CDN media reference.
    pub async fn download_raw(
        &self,
        media: &CDNMedia,
        aeskey_override: Option<&str>,
    ) -> Result<Vec<u8>> {
        self.cdn.download(media, aeskey_override).await
    }

    /// Upload data to WeChat CDN without sending a message.
    pub async fn upload(
        &self,
        data: &[u8],
        user_id: &str,
        media_type: i32,
    ) -> Result<UploadResult> {
        let (base_url, token) = self.get_auth().await?;
        self.cdn_upload(&base_url, &token, data, user_id, media_type)
            .await
    }

    async fn run_loop(&self, mut cursor: String) -> Result<()> {
        *self.stopped.write().await = false;
        info!("Long-poll loop started");
        let mut retry_delay = Duration::from_secs(1);

        if let Ok((base_url, token)) = self.get_auth().await {
            match self.client.notify_start(&base_url, &token).await {
                Ok(resp) if resp.ret.is_some_and(|ret| ret != 0) => warn!(
                    "notify_start returned ret={:?} errmsg={:?}",
                    resp.ret, resp.errmsg
                ),
                Ok(_) => {}
                Err(err) => warn!("notify_start failed: {}", err),
            }
        }

        loop {
            if *self.stopped.read().await {
                break;
            }

            let (base_url, token) = self.get_auth().await?;

            match self.client.get_updates(&base_url, &token, &cursor).await {
                Ok(updates) => {
                    let account_key = self.account_key().await;
                    let new_cursor = updates.get_updates_buf.clone();
                    let mut cursor_changed = false;
                    if !new_cursor.is_empty() && cursor.as_str() != new_cursor.as_str() {
                        cursor_changed = true;
                        cursor = new_cursor.clone();
                    }
                    retry_delay = Duration::from_secs(1);

                    for wire in &updates.msgs {
                        if let Some(incoming) =
                            IncomingMessage::from_wire_for_account(wire, &account_key)
                        {
                            self.reset_account_rate_limit(&account_key).await;
                            if let Some(context) = incoming.context.clone() {
                                self.observe_context_for_interaction(&context).await;
                                self.emit_event(WechatEvent::ContextObserved(context));
                            }
                            self.emit_event(WechatEvent::Message(incoming.clone()));
                        }
                    }
                    self.emit_due_context_interaction_requests().await;
                    if cursor_changed {
                        self.emit_event(WechatEvent::CursorAdvanced {
                            account_key: account_key.clone(),
                            cursor: new_cursor.clone(),
                        });
                    }
                }
                Err(e) if e.is_session_expired() => {
                    warn!("Session expired — re-login required");
                    let account_key = self.account_key().await;
                    self.emit_event(WechatEvent::AuthSessionExpired {
                        account_key: account_key.clone(),
                    });
                    cursor.clear();
                    break;
                }
                Err(e) if e.is_rate_limited() => {
                    self.report_error(&e);
                    sleep(self.rate_limit.retry_after).await;
                    retry_delay = Duration::from_secs(1);
                    continue;
                }
                Err(e) => {
                    self.report_error(&e);
                    sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(10));
                    continue;
                }
            }
        }

        info!("Long-poll loop stopped");
        if let Ok((base_url, token)) = self.get_auth().await {
            match self.client.notify_stop(&base_url, &token).await {
                Ok(resp) if resp.ret.is_some_and(|ret| ret != 0) => warn!(
                    "notify_stop returned ret={:?} errmsg={:?}",
                    resp.ret, resp.errmsg
                ),
                Ok(_) => {}
                Err(err) => warn!("notify_stop failed: {}", err),
            }
        }
        Ok(())
    }

    async fn retry_rate_limited<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let account_key = self.account_key().await;
        if let Some(wait) = self.active_rate_limit_backoff(&account_key).await {
            return Err(rate_limited_error(wait));
        }

        let mut retries = 0usize;
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_rate_limited() => {
                    let err = with_rate_limit_retry_after(err, self.rate_limit.retry_after);
                    self.defer_account_rate_limit(&account_key, self.rate_limit.retry_after)
                        .await;
                    if retries >= self.rate_limit.retry_attempts {
                        return Err(err);
                    }
                    retries += 1;
                    self.wait_for_rate_limit_reset_or_timeout(
                        &account_key,
                        self.rate_limit.retry_after,
                    )
                    .await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn active_rate_limit_backoff(&self, account_key: &str) -> Option<Duration> {
        if account_key.is_empty() {
            return None;
        }
        let mut states = self.rate_limit_states.lock().await;
        let state = states.entry(account_key.to_string()).or_default();
        match state.next_allowed_at {
            Some(next_allowed_at) => match next_allowed_at.checked_duration_since(Instant::now()) {
                Some(wait) if !wait.is_zero() => Some(wait),
                _ => {
                    state.next_allowed_at = None;
                    None
                }
            },
            None => None,
        }
    }

    async fn wait_for_rate_limit_reset_or_timeout(&self, account_key: &str, wait: Duration) {
        if account_key.is_empty() || wait.is_zero() {
            return;
        }
        let _ = tokio::time::timeout(wait, self.rate_limit_notify.notified()).await;
    }

    async fn defer_account_rate_limit(&self, account_key: &str, retry_after: Duration) {
        if account_key.is_empty() {
            return;
        }
        let next_allowed_at = Instant::now() + retry_after;
        let mut states = self.rate_limit_states.lock().await;
        let state = states.entry(account_key.to_string()).or_default();
        if state
            .next_allowed_at
            .map_or(true, |current| current < next_allowed_at)
        {
            state.next_allowed_at = Some(next_allowed_at);
        }
    }

    async fn reset_account_rate_limit(&self, account_key: &str) {
        if account_key.is_empty() {
            return;
        }
        let mut states = self.rate_limit_states.lock().await;
        let state = states.entry(account_key.to_string()).or_default();
        state.sent_at.clear();
        state.next_allowed_at = None;
        drop(states);
        self.rate_limit_notify.notify_waiters();
    }

    async fn observe_context_for_interaction(&self, context: &WechatContext) {
        let observed_at = SystemTime::now();
        let expires_at = observed_at + self.rate_limit.context_ttl;
        self.context_refresh_states.lock().await.insert(
            (context.account_key.clone(), context.user_id.clone()),
            ContextRefreshState {
                user_id: context.user_id.clone(),
                observed_at,
                expires_at,
                reminded: false,
            },
        );
    }

    async fn emit_due_context_interaction_requests(&self) {
        if self.rate_limit.context_ttl.is_zero() {
            return;
        }
        let now = SystemTime::now();
        let mut events = Vec::new();
        {
            let mut contexts = self.context_refresh_states.lock().await;
            contexts.retain(|_, state| !(state.reminded && now > state.expires_at));
            for ((account_key, _), state) in contexts.iter_mut() {
                if state.reminded {
                    continue;
                }
                let remind_at = state
                    .expires_at
                    .checked_sub(self.rate_limit.context_remind_before)
                    .unwrap_or(state.observed_at);
                if now >= remind_at {
                    state.reminded = true;
                    events.push(WechatEvent::UserInteractionRequested {
                        account_key: account_key.clone(),
                        user_id: Some(state.user_id.clone()),
                        reason: UserInteractionReason::ContextExpiring {
                            observed_at: state.observed_at,
                            expires_at: state.expires_at,
                            remind_before: self.rate_limit.context_remind_before,
                        },
                    });
                }
            }
        }
        for event in events {
            self.emit_event(event);
        }
    }

    async fn record_successful_message_send(&self) {
        let account_key = self.account_key().await;
        if account_key.is_empty() || self.rate_limit.interaction_threshold == 0 {
            return;
        }

        let now = Instant::now();
        let mut event = None;
        {
            let mut states = self.rate_limit_states.lock().await;
            let state = states.entry(account_key.clone()).or_default();
            while let Some(sent_at) = state.sent_at.front().copied() {
                if now.duration_since(sent_at) > self.rate_limit.interaction_window {
                    state.sent_at.pop_front();
                } else {
                    break;
                }
            }
            state.sent_at.push_back(now);
            let sent_count = state.sent_at.len();
            if sent_count == self.rate_limit.interaction_threshold {
                event = Some(WechatEvent::UserInteractionRequested {
                    account_key,
                    user_id: None,
                    reason: UserInteractionReason::OutboundRateLimitApproaching {
                        sent_count,
                        window: self.rate_limit.interaction_window,
                        threshold: self.rate_limit.interaction_threshold,
                    },
                });
            }
        }

        if let Some(event) = event {
            self.emit_event(event);
        }
    }

    /// Stop the bot.
    pub async fn stop(&self) {
        *self.stopped.write().await = true;
    }

    // --- internal media ---

    fn send_content<'a>(
        &'a self,
        user_id: &'a str,
        context_token: &'a str,
        content: SendContent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SendReceipt>> + Send + 'a>> {
        Box::pin(async move {
            let (base_url, token) = self.get_auth().await?;
            match content {
                SendContent::Text(text) => self.send_text(user_id, &text, context_token).await,
                SendContent::Image { data, caption } => {
                    let result = self
                        .cdn_upload(&base_url, &token, &data, user_id, 1)
                        .await?;
                    let mut receipt = SendReceipt::empty();
                    if let Some(cap) = caption {
                        receipt.append(self.send_text(user_id, &cap, context_token).await?);
                    }
                    let item = json!({"type": 2, "image_item": {
                        "media": cdn_media_json(&result.media),
                        "mid_size": result.encrypted_file_size,
                    }});
                    receipt.append(
                        self.send_media_item(&base_url, &token, user_id, context_token, item)
                            .await?,
                    );
                    Ok(receipt)
                }
                SendContent::Video { data, caption } => {
                    let result = self
                        .cdn_upload(&base_url, &token, &data, user_id, 2)
                        .await?;
                    let mut receipt = SendReceipt::empty();
                    if let Some(cap) = caption {
                        receipt.append(self.send_text(user_id, &cap, context_token).await?);
                    }
                    let item = json!({"type": 5, "video_item": {
                        "media": cdn_media_json(&result.media),
                        "video_size": result.encrypted_file_size,
                    }});
                    receipt.append(
                        self.send_media_item(&base_url, &token, user_id, context_token, item)
                            .await?,
                    );
                    Ok(receipt)
                }
                SendContent::File {
                    data,
                    file_name,
                    caption,
                } => {
                    let cat = categorize_by_extension(&file_name);
                    match cat {
                        "image" => {
                            self.send_content(
                                user_id,
                                context_token,
                                SendContent::Image { data, caption },
                            )
                            .await
                        }
                        "video" => {
                            self.send_content(
                                user_id,
                                context_token,
                                SendContent::Video { data, caption },
                            )
                            .await
                        }
                        _ => {
                            let mut receipt = SendReceipt::empty();
                            if let Some(cap) = caption {
                                receipt.append(self.send_text(user_id, &cap, context_token).await?);
                            }
                            let data_len = data.len();
                            let result = self
                                .cdn_upload(&base_url, &token, &data, user_id, 3)
                                .await?;
                            let item = json!({"type": 4, "file_item": {
                                "media": cdn_media_json(&result.media),
                                "file_name": file_name,
                                "len": data_len.to_string(),
                            }});
                            receipt.append(
                                self.send_media_item(
                                    &base_url,
                                    &token,
                                    user_id,
                                    context_token,
                                    item,
                                )
                                .await?,
                            );
                            Ok(receipt)
                        }
                    }
                }
            }
        })
    }

    async fn send_media_item(
        &self,
        base_url: &str,
        token: &str,
        user_id: &str,
        context_token: &str,
        item: serde_json::Value,
    ) -> Result<SendReceipt> {
        let client_id = Uuid::new_v4().to_string();
        let msg = protocol::build_media_message_with_client_id(
            user_id,
            context_token,
            vec![item],
            &client_id,
        );
        self.retry_rate_limited(|| self.client.send_message(base_url, token, &msg))
            .await?;
        self.record_successful_message_send().await;
        Ok(SendReceipt::single(client_id))
    }

    async fn cdn_upload(
        &self,
        base_url: &str,
        token: &str,
        data: &[u8],
        user_id: &str,
        media_type: i32,
    ) -> Result<UploadResult> {
        let aes_key = crypto::generate_aes_key();
        let ciphertext = crypto::encrypt_aes_ecb(data, &aes_key);

        let mut filekey_buf = [0u8; 16];
        rand::rng().fill_bytes(&mut filekey_buf);
        let filekey = hex::encode(filekey_buf);

        let raw_md5 = hex::encode(Md5::digest(data));

        let params = protocol::GetUploadUrlParams {
            filekey: filekey.clone(),
            media_type,
            to_user_id: user_id.to_string(),
            rawsize: data.len(),
            rawfilemd5: raw_md5,
            filesize: ciphertext.len(),
            thumb_rawsize: None,
            thumb_rawfilemd5: None,
            thumb_filesize: None,
            no_need_thumb: true,
            aeskey: crypto::encode_aes_key_hex(&aes_key),
        };

        let upload_resp = self.client.get_upload_url(base_url, token, &params).await?;
        let upload_url = resolve_cdn_upload_url(&upload_resp, &filekey)?;

        let encrypted_file_size = ciphertext.len();

        let encrypt_query_param = self.client.upload_to_cdn(&upload_url, &ciphertext).await?;

        Ok(UploadResult {
            media: CDNMedia {
                encrypt_query_param: Some(encrypt_query_param),
                aes_key: Some(crypto::encode_aes_key_base64(&aes_key)),
                encrypt_type: Some(1),
                full_url: None,
            },
            aes_key,
            encrypted_file_size,
        })
    }

    // --- internal text ---

    async fn send_text(
        &self,
        user_id: &str,
        text: &str,
        context_token: &str,
    ) -> Result<SendReceipt> {
        let (base_url, token) = self.get_auth().await?;
        let mut message_ids = Vec::new();
        let mut visible_texts = Vec::new();
        for chunk in chunk_text(text, 4000) {
            let client_id = Uuid::new_v4().to_string();
            let msg = self.client.build_text_message_with_client_id(
                user_id,
                context_token,
                &chunk,
                &client_id,
            );
            self.retry_rate_limited(|| self.client.send_message(&base_url, &token, &msg))
                .await?;
            self.record_successful_message_send().await;
            message_ids.push(client_id);
            visible_texts.push(chunk);
        }
        Ok(SendReceipt {
            message_ids,
            visible_texts,
        })
    }

    async fn get_auth(&self) -> Result<(String, String)> {
        let creds = self.credentials.read().await;
        let creds = creds
            .as_ref()
            .ok_or_else(|| WechatIlinkError::Auth("not logged in".into()))?;
        Ok((creds.base_url.clone(), creds.token.clone()))
    }

    async fn account_key(&self) -> String {
        let creds = self.credentials.read().await;
        match creds.as_ref() {
            Some(c) => context_account_key(c).to_string(),
            None => String::new(),
        }
    }

    fn report_error(&self, err: &WechatIlinkError) {
        error!("{}", err);
    }
}

fn event_stream_from_receiver(
    mut rx: broadcast::Receiver<WechatEvent>,
) -> WechatEventStream<'static> {
    WechatEventStream::new(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => yield Ok(event),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    yield Err(WechatIlinkError::Other(format!(
                        "wechat event stream lagged by {skipped} events"
                    )));
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn rate_limited_error(retry_after: Duration) -> WechatIlinkError {
    WechatIlinkError::RateLimited {
        retry_after,
        message: "ret=-2".to_string(),
        http_status: 200,
        errcode: -2,
    }
}

fn with_rate_limit_retry_after(err: WechatIlinkError, retry_after: Duration) -> WechatIlinkError {
    match err {
        WechatIlinkError::RateLimited {
            message,
            http_status,
            errcode,
            ..
        } => WechatIlinkError::RateLimited {
            retry_after,
            message,
            http_status,
            errcode,
        },
        other => other,
    }
}

/// Content to send via reply_media / send_media_with_context.
pub enum SendContent {
    Text(String),
    Image {
        data: Vec<u8>,
        caption: Option<String>,
    },
    Video {
        data: Vec<u8>,
        caption: Option<String>,
    },
    File {
        data: Vec<u8>,
        file_name: String,
        caption: Option<String>,
    },
}

fn cdn_media_json(media: &CDNMedia) -> serde_json::Value {
    let mut v = json!({});
    if let Some(param) = &media.encrypt_query_param {
        v["encrypt_query_param"] = json!(param);
    }
    if let Some(key) = &media.aes_key {
        v["aes_key"] = json!(key);
    }
    if let Some(et) = media.encrypt_type {
        v["encrypt_type"] = json!(et);
    }
    if let Some(url) = &media.full_url {
        v["full_url"] = json!(url);
    }
    v
}

fn resolve_cdn_upload_url(
    response: &protocol::GetUploadUrlResponse,
    filekey: &str,
) -> Result<String> {
    if let Some(upload_full_url) = response
        .upload_full_url
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return Ok(upload_full_url.to_string());
    }
    if let Some(upload_param) = response
        .upload_param
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return Ok(protocol::build_cdn_upload_url(
            protocol::CDN_BASE_URL,
            upload_param,
            filekey,
        ));
    }
    Err(WechatIlinkError::Media(
        "getuploadurl did not return upload_full_url or upload_param".into(),
    ))
}

fn require_download_aes_key(media_type: &str, media: &CDNMedia) -> Result<()> {
    if media
        .aes_key
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        return Ok(());
    }
    Err(WechatIlinkError::Media(format!(
        "{} CDN media missing AES key",
        media_type
    )))
}

fn categorize_by_extension(filename: &str) -> &'static str {
    let ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "svg" => "image",
        "mp4" | "mov" | "webm" | "mkv" | "avi" => "video",
        _ => "file",
    }
}

fn chunk_text(text: &str, limit: usize) -> Vec<String> {
    if text.len() <= limit {
        return vec![text.to_string()];
    }
    let mut chunks = Vec::new();
    let mut remaining = text;
    while !remaining.is_empty() {
        if remaining.len() <= limit {
            chunks.push(remaining.to_string());
            break;
        }
        let window_end = char_boundary_at_or_before(remaining, limit);
        let window = &remaining[..window_end];
        let cut = window
            .rfind("\n\n")
            .filter(|&i| i > window_end * 3 / 10)
            .map(|i| i + 2)
            .or_else(|| {
                window
                    .rfind('\n')
                    .filter(|&i| i > window_end * 3 / 10)
                    .map(|i| i + 1)
            })
            .or_else(|| {
                window
                    .rfind(' ')
                    .filter(|&i| i > window_end * 3 / 10)
                    .map(|i| i + 1)
            })
            .unwrap_or(window_end);
        chunks.push(remaining[..cut].to_string());
        remaining = &remaining[cut..];
    }
    if chunks.is_empty() {
        vec![String::new()]
    } else {
        chunks
    }
}

fn char_boundary_at_or_before(text: &str, limit: usize) -> usize {
    let mut end = limit.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn context_account_key(creds: &Credentials) -> &str {
    if !creds.account_id.is_empty() {
        &creds.account_id
    } else {
        &creds.user_id
    }
}

fn chrono_now() -> String {
    // Lightweight timestamp string without a chrono dependency.
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}Z", dur.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_text_short() {
        let chunks = chunk_text("hello", 100);
        assert_eq!(chunks, vec!["hello"]);
    }

    #[test]
    fn chunk_text_empty() {
        let chunks = chunk_text("", 100);
        assert_eq!(chunks, vec![""]);
    }

    #[test]
    fn chunk_text_splits_on_paragraph() {
        let text = "aaaa\n\nbbbb";
        let chunks = chunk_text(text, 7);
        assert_eq!(chunks, vec!["aaaa\n\n", "bbbb"]);
    }

    #[test]
    fn chunk_text_splits_on_newline() {
        let text = "aaaa\nbbbb";
        let chunks = chunk_text(text, 7);
        assert_eq!(chunks, vec!["aaaa\n", "bbbb"]);
    }

    #[test]
    fn chunk_text_exact_limit() {
        let text = "abcdef";
        let chunks = chunk_text(text, 6);
        assert_eq!(chunks, vec!["abcdef"]);
    }

    #[test]
    fn chunk_text_does_not_split_utf8_char_boundary() {
        let mut text = "a".repeat(3999);
        text.push('启');
        text.push_str("tail");

        let chunks = chunk_text(&text, 4000);

        assert_eq!(chunks.concat(), text);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 4000));
    }

    #[test]
    fn categorize_image_extensions() {
        assert_eq!(categorize_by_extension("photo.png"), "image");
        assert_eq!(categorize_by_extension("photo.JPG"), "image");
        assert_eq!(categorize_by_extension("anim.gif"), "image");
        assert_eq!(categorize_by_extension("pic.webp"), "image");
    }

    #[test]
    fn categorize_video_extensions() {
        assert_eq!(categorize_by_extension("clip.mp4"), "video");
        assert_eq!(categorize_by_extension("clip.MOV"), "video");
        assert_eq!(categorize_by_extension("movie.webm"), "video");
    }

    #[test]
    fn categorize_file_extensions() {
        assert_eq!(categorize_by_extension("report.pdf"), "file");
        assert_eq!(categorize_by_extension("data.csv"), "file");
        assert_eq!(categorize_by_extension("noext"), "file");
    }

    #[test]
    fn cdn_media_json_with_encrypt_type() {
        let media = CDNMedia {
            encrypt_query_param: Some("param=1".to_string()),
            aes_key: Some("key123".to_string()),
            encrypt_type: Some(1),
            full_url: None,
        };
        let j = cdn_media_json(&media);
        assert_eq!(j["encrypt_query_param"], "param=1");
        assert_eq!(j["aes_key"], "key123");
        assert_eq!(j["encrypt_type"], 1);
    }

    #[test]
    fn cdn_media_json_without_encrypt_type() {
        let media = CDNMedia {
            encrypt_query_param: Some("p".to_string()),
            aes_key: Some("k".to_string()),
            encrypt_type: None,
            full_url: None,
        };
        let j = cdn_media_json(&media);
        assert!(j.get("encrypt_type").is_none());
    }

    #[test]
    fn upload_url_prefers_full_url() {
        let response = protocol::GetUploadUrlResponse {
            upload_param: Some("param".to_string()),
            thumb_upload_param: None,
            upload_full_url: Some("https://cdn.example/upload".to_string()),
        };

        let upload_url = resolve_cdn_upload_url(&response, "file-key").unwrap();
        assert_eq!(upload_url, "https://cdn.example/upload");
    }

    #[test]
    fn upload_url_falls_back_to_upload_param() {
        let response = protocol::GetUploadUrlResponse {
            upload_param: Some("param value".to_string()),
            thumb_upload_param: None,
            upload_full_url: None,
        };

        let upload_url = resolve_cdn_upload_url(&response, "file key").unwrap();
        assert_eq!(
            upload_url,
            format!(
                "{}/upload?encrypted_query_param=param%20value&filekey=file%20key",
                protocol::CDN_BASE_URL
            )
        );
    }

    #[test]
    fn upload_url_requires_full_url_or_upload_param() {
        let response = protocol::GetUploadUrlResponse {
            upload_param: None,
            thumb_upload_param: None,
            upload_full_url: None,
        };

        let err = resolve_cdn_upload_url(&response, "file-key").unwrap_err();
        assert!(matches!(err, WechatIlinkError::Media(_)));
    }

    #[test]
    fn non_image_download_requires_media_aes_key() {
        let media = CDNMedia {
            encrypt_query_param: Some("param".to_string()),
            aes_key: None,
            encrypt_type: Some(1),
            full_url: None,
        };

        let err = require_download_aes_key("voice", &media).unwrap_err();
        assert!(matches!(err, WechatIlinkError::Media(_)));
    }

    #[test]
    fn non_image_download_accepts_media_aes_key() {
        let media = CDNMedia {
            encrypt_query_param: Some("param".to_string()),
            aes_key: Some("key".to_string()),
            encrypt_type: Some(1),
            full_url: None,
        };

        require_download_aes_key("file", &media).unwrap();
    }

    #[tokio::test]
    async fn stream_from_cursor_reports_missing_auth_without_storing_cursor() {
        let client = Arc::new(WechatIlinkClient::new());
        let mut events = client.stream_from_cursor(Some("external-cursor".to_string()));

        let err = events
            .next()
            .await
            .expect("stream item")
            .expect_err("missing auth should be surfaced through stream");

        assert!(matches!(err, WechatIlinkError::Auth(_)));
    }

    #[test]
    fn rate_limit_builder_defaults_and_overrides_are_available() {
        let default_options = WechatRateLimitOptions::default();
        assert_eq!(default_options.retry_after, Duration::from_secs(90));
        assert_eq!(default_options.retry_attempts, 5);
        assert_eq!(default_options.interaction_window, Duration::from_secs(300));
        assert_eq!(default_options.interaction_threshold, 6);
        assert_eq!(
            default_options.context_ttl,
            Duration::from_secs(24 * 60 * 60)
        );

        let client = WechatIlinkClient::builder()
            .rate_limit_retry_after(Duration::from_secs(60))
            .rate_limit_max_retries(2)
            .rate_limit_interaction_window(Duration::from_secs(120))
            .rate_limit_interaction_threshold(4)
            .context_ttl(Duration::from_secs(3600))
            .context_expiry_remind_before(Duration::from_secs(300))
            .build();

        assert_eq!(client.rate_limit.retry_after, Duration::from_secs(60));
        assert_eq!(client.rate_limit.retry_attempts, 2);
        assert_eq!(
            client.rate_limit.interaction_window,
            Duration::from_secs(120)
        );
        assert_eq!(client.rate_limit.interaction_threshold, 4);
        assert_eq!(client.rate_limit.context_ttl, Duration::from_secs(3600));
        assert_eq!(
            client.rate_limit.context_remind_before,
            Duration::from_secs(300)
        );
    }

    #[tokio::test]
    async fn outbound_threshold_emits_interaction_event_and_incoming_resets_window() {
        let client = WechatIlinkClient::builder()
            .rate_limit_interaction_threshold(3)
            .rate_limit_interaction_window(Duration::from_secs(300))
            .build();
        client.set_credentials(test_credentials()).await;
        let mut events = client.event_stream();

        client.record_successful_message_send().await;
        client.record_successful_message_send().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), events.next())
                .await
                .is_err()
        );
        client.record_successful_message_send().await;
        assert_rate_limit_event(events.next().await, "account-1", 3, 3);
        client.record_successful_message_send().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), events.next())
                .await
                .is_err()
        );

        client.reset_account_rate_limit("account-1").await;
        client.record_successful_message_send().await;
        client.record_successful_message_send().await;
        client.record_successful_message_send().await;
        assert_rate_limit_event(events.next().await, "account-1", 3, 3);
    }

    #[tokio::test]
    async fn context_expiry_emits_user_interaction_event_and_new_context_resets_it() {
        let client = WechatIlinkClient::builder()
            .context_ttl(Duration::from_millis(5))
            .context_expiry_remind_before(Duration::from_millis(5))
            .build();
        let mut events = client.event_stream();

        let context = WechatContext {
            account_key: "account-1".to_string(),
            user_id: "user-1".to_string(),
            context_token: "ctx-1".to_string(),
            observed_at_unix_ms: 1,
            source_message_id: Some("msg-1".to_string()),
        };
        client.observe_context_for_interaction(&context).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        client.emit_due_context_interaction_requests().await;
        assert_context_expiring_event(events.next().await, "account-1", "user-1");

        client.observe_context_for_interaction(&context).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        client.emit_due_context_interaction_requests().await;
        assert_context_expiring_event(events.next().await, "account-1", "user-1");
    }

    #[tokio::test]
    async fn retry_rate_limited_uses_configured_retry_count() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let client = WechatIlinkClient::builder()
            .rate_limit_retry_after(Duration::from_millis(1))
            .rate_limit_max_retries(2)
            .build();
        client.set_credentials(test_credentials()).await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_operation = Arc::clone(&attempts);

        let err = client
            .retry_rate_limited(move || {
                let attempts_for_operation = Arc::clone(&attempts_for_operation);
                async move {
                    attempts_for_operation.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(rate_limited_error(Duration::from_millis(1)))
                }
            })
            .await
            .expect_err("rate limit should remain after configured retries");

        assert!(err.is_rate_limited());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn active_rate_limit_backoff_returns_error_without_queueing_new_request() {
        let client = WechatIlinkClient::builder()
            .rate_limit_retry_after(Duration::from_secs(90))
            .build();
        client.set_credentials(test_credentials()).await;
        client
            .defer_account_rate_limit("account-1", Duration::from_secs(90))
            .await;

        let mut called = false;
        let err = client
            .retry_rate_limited(|| {
                called = true;
                async { Ok::<_, WechatIlinkError>(()) }
            })
            .await
            .expect_err("active backoff should return immediately");

        assert!(!called);
        assert!(err.is_rate_limited());
    }

    #[tokio::test]
    async fn event_stream_accepts_context_observed_and_cursor_events() {
        let client = WechatIlinkClient::new();
        let mut events = client.event_stream();

        client.emit_event(WechatEvent::ContextObserved(WechatContext {
            account_key: "account-1".to_string(),
            user_id: "user-1".to_string(),
            context_token: "ctx-1".to_string(),
            observed_at_unix_ms: 1,
            source_message_id: Some("msg-1".to_string()),
        }));
        client.emit_event(WechatEvent::CursorAdvanced {
            account_key: "account-1".to_string(),
            cursor: "cursor-2".to_string(),
        });

        assert_eq!(format_event(events.next().await), "context:user-1");
        assert_eq!(format_event(events.next().await), "cursor:cursor-2");
    }

    fn assert_rate_limit_event(
        event: Option<Result<WechatEvent>>,
        expected_account: &str,
        expected_sent_count: usize,
        expected_threshold: usize,
    ) {
        match event.expect("event").expect("ok event") {
            WechatEvent::UserInteractionRequested {
                account_key,
                reason:
                    UserInteractionReason::OutboundRateLimitApproaching {
                        sent_count,
                        threshold,
                        ..
                    },
                ..
            } => {
                assert_eq!(account_key, expected_account);
                assert_eq!(sent_count, expected_sent_count);
                assert_eq!(threshold, expected_threshold);
            }
            other => panic!("expected rate-limit interaction event, got {other:?}"),
        }
    }

    fn assert_context_expiring_event(
        event: Option<Result<WechatEvent>>,
        expected_account: &str,
        expected_user: &str,
    ) {
        match event.expect("event").expect("ok event") {
            WechatEvent::UserInteractionRequested {
                account_key,
                user_id,
                reason: UserInteractionReason::ContextExpiring { .. },
            } => {
                assert_eq!(account_key, expected_account);
                assert_eq!(user_id.as_deref(), Some(expected_user));
            }
            other => panic!("expected context-expiring interaction event, got {other:?}"),
        }
    }

    fn format_event(event: Option<Result<WechatEvent>>) -> String {
        match event.expect("event").expect("ok event") {
            WechatEvent::ContextObserved(ctx) => format!("context:{}", ctx.user_id),
            WechatEvent::CursorAdvanced { cursor, .. } => format!("cursor:{cursor}"),
            WechatEvent::Message(msg) => format!("message:{}", msg.user_id),
            WechatEvent::AuthSessionExpired { account_key } => format!("auth:{account_key}"),
            WechatEvent::UserInteractionRequested { reason, .. } => match reason {
                UserInteractionReason::OutboundRateLimitApproaching { sent_count, .. } => {
                    format!("rate-limit:{sent_count}")
                }
                UserInteractionReason::ContextExpiring { .. } => "context-expiring".into(),
            },
        }
    }

    fn test_credentials() -> Credentials {
        Credentials {
            token: "token-1".into(),
            base_url: "https://example.invalid".into(),
            account_id: "account-1".into(),
            user_id: "bot-1".into(),
            saved_at: None,
        }
    }

    #[tokio::test]
    async fn credentials_are_supplied_by_caller() {
        let client = WechatIlinkClient::new();
        client.set_credentials(test_credentials()).await;

        let creds = client.credentials().await.unwrap();
        assert_eq!(creds.account_id, "account-1");
    }
}
