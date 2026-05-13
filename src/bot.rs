//! Main WechatIlinkClient.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
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

/// Message handler callback type.
pub type MessageHandler = Box<dyn Fn(&IncomingMessage) + Send + Sync>;

/// Event handler callback type.
pub type EventHandler = Box<dyn Fn(&WechatEvent) + Send + Sync>;

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

/// Builder for [`WechatIlinkClient`].
pub struct WechatIlinkClientBuilder {
    base_url: Option<String>,
    bot_agent: Option<String>,
    ilink_app_id: Option<String>,
    route_tag: Option<String>,
    markdown_filter: bool,
    credentials: Option<Credentials>,
    on_qr_url: Option<Box<dyn Fn(&str) + Send + Sync>>,
    on_verify_code: Option<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>,
    on_error: Option<Box<dyn Fn(&WechatIlinkError) + Send + Sync>>,
}

impl Default for WechatIlinkClientBuilder {
    fn default() -> Self {
        Self {
            base_url: None,
            bot_agent: None,
            ilink_app_id: None,
            route_tag: None,
            markdown_filter: true,
            credentials: None,
            on_qr_url: None,
            on_verify_code: None,
            on_error: None,
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

    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn on_qr_url<F>(mut self, on_qr_url: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.on_qr_url = Some(Box::new(on_qr_url));
        self
    }

    pub fn on_verify_code<F>(mut self, on_verify_code: F) -> Self
    where
        F: Fn(&str) -> Option<String> + Send + Sync + 'static,
    {
        self.on_verify_code = Some(Box::new(on_verify_code));
        self
    }

    pub fn on_error<F>(mut self, on_error: F) -> Self
    where
        F: Fn(&WechatIlinkError) + Send + Sync + 'static,
    {
        self.on_error = Some(Box::new(on_error));
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
    handlers: Mutex<Vec<MessageHandler>>,
    event_handlers: Mutex<Vec<EventHandler>>,
    cursor: RwLock<String>,
    base_url: RwLock<String>,
    stopped: RwLock<bool>,
    on_qr_url: Option<Box<dyn Fn(&str) + Send + Sync>>,
    on_verify_code: Option<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>,
    on_error: Option<Box<dyn Fn(&WechatIlinkError) + Send + Sync>>,
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
            handlers: Mutex::new(Vec::new()),
            event_handlers: Mutex::new(Vec::new()),
            cursor: RwLock::new(String::new()),
            base_url: RwLock::new(base_url),
            stopped: RwLock::new(false),
            on_qr_url: builder.on_qr_url,
            on_verify_code: builder.on_verify_code,
            on_error: builder.on_error,
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

    /// Login via QR code. Returns credentials on success.
    ///
    /// The caller owns persistence of returned credentials.
    pub async fn login_qr(&self) -> Result<Credentials> {
        let base_url = self.base_url.read().await.clone();

        // QR code login flow
        let mut qr_refresh_count = 0u32;
        loop {
            qr_refresh_count += 1;
            if qr_refresh_count > Self::MAX_QR_REFRESH {
                return Err(WechatIlinkError::Auth(format!(
                    "QR code expired {} times — login aborted",
                    Self::MAX_QR_REFRESH
                )));
            }

            let qr = self.client.get_qr_code(Self::FIXED_QR_BASE_URL).await?;

            if let Some(ref cb) = self.on_qr_url {
                cb(&qr.qrcode_img_content);
            } else {
                eprintln!("[wechat-ilink] Scan: {}", qr.qrcode_img_content);
            }

            let mut last_status = String::new();
            let mut current_poll_base_url = Self::FIXED_QR_BASE_URL.to_string();
            let mut pending_verify_code: Option<String> = None;
            loop {
                let status = self
                    .client
                    .poll_qr_status_with_verify_code(
                        &current_poll_base_url,
                        &qr.qrcode,
                        pending_verify_code.as_deref(),
                    )
                    .await?;

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
                }

                if status.status == "wait" {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                if status.status == "need_verifycode" {
                    let Some(ref on_verify_code) = self.on_verify_code else {
                        return Err(WechatIlinkError::Auth(
                            "QR verification code required".into(),
                        ));
                    };
                    let prompt = if pending_verify_code.is_some() {
                        "QR verification code mismatch"
                    } else {
                        "QR verification code required"
                    };
                    let code = on_verify_code(prompt)
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            WechatIlinkError::Auth("QR verification code was not provided".into())
                        })?;
                    pending_verify_code = Some(code);
                    continue;
                }

                if status.status == "verify_code_blocked" {
                    break;
                }

                if status.status == "binded_redirect" {
                    return Err(WechatIlinkError::Auth(
                        "QR login is already bound to this app".into(),
                    ));
                }

                if status.status == "confirmed" {
                    let token = status
                        .bot_token
                        .ok_or_else(|| WechatIlinkError::Auth("missing bot_token".into()))?;
                    let creds = Credentials {
                        token,
                        base_url: status.baseurl.unwrap_or_else(|| base_url.clone()),
                        account_id: status.ilink_bot_id.unwrap_or_default(),
                        user_id: status.ilink_user_id.unwrap_or_default(),
                        saved_at: Some(chrono_now()),
                    };
                    *self.credentials.write().await = Some(creds.clone());
                    *self.base_url.write().await = creds.base_url.clone();
                    return Ok(creds);
                }

                // Handle IDC redirect
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
    }

    /// Register a message handler.
    pub async fn on_message(&self, handler: MessageHandler) {
        self.handlers.lock().await.push(handler);
    }

    /// Register an event handler.
    ///
    /// Event handlers receive lifecycle events emitted during polling:
    /// [`WechatEvent::ContextObserved`], [`WechatEvent::Message`],
    /// [`WechatEvent::CursorAdvanced`], and [`WechatEvent::AuthSessionExpired`].
    pub async fn on_event(&self, handler: EventHandler) {
        self.event_handlers.lock().await.push(handler);
    }

    /// Emit an event to all registered event handlers.
    async fn emit_event(&self, event: WechatEvent) {
        let handlers = self.event_handlers.lock().await;
        for handler in handlers.iter() {
            handler(&event);
        }
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
            .client
            .get_config(&base_url, &token, &context.user_id, &context.context_token)
            .await?;
        if let Some(ticket) = config.typing_ticket {
            self.client
                .send_typing(&base_url, &token, &context.user_id, &ticket, 1)
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

    /// Start the long-poll loop with an explicit starting cursor.
    ///
    /// Uses a process-local cursor for this run. The cursor, observed contexts,
    /// and reminder metadata are not persisted by this method; the caller is
    /// responsible for saving state from emitted events.
    pub async fn run_from_cursor(&self, cursor: Option<String>) -> Result<()> {
        self.run_loop(cursor.unwrap_or_default(), false).await
    }

    /// Start the long-poll loop. Blocks until stopped.
    pub async fn run(&self) -> Result<()> {
        let cursor = self.cursor.read().await.clone();
        self.run_loop(cursor, true).await
    }

    async fn run_loop(&self, mut cursor: String, persist_legacy_state: bool) -> Result<()> {
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
                        if persist_legacy_state {
                            *self.cursor.write().await = new_cursor.clone();
                        }
                    }
                    retry_delay = Duration::from_secs(1);

                    for wire in &updates.msgs {
                        if let Some(incoming) =
                            IncomingMessage::from_wire_for_account(wire, &account_key)
                        {
                            if let Some(context) = incoming.context.clone() {
                                self.emit_event(WechatEvent::ContextObserved(context)).await;
                            }
                            self.emit_event(WechatEvent::Message(incoming.clone()))
                                .await;
                            let handlers = self.handlers.lock().await;
                            for handler in handlers.iter() {
                                handler(&incoming);
                            }
                        }
                    }
                    if cursor_changed {
                        self.emit_event(WechatEvent::CursorAdvanced {
                            account_key: account_key.clone(),
                            cursor: new_cursor.clone(),
                        })
                        .await;
                    }
                }
                Err(e) if e.is_session_expired() => {
                    warn!("Session expired — re-login required");
                    let account_key = self.account_key().await;
                    self.emit_event(WechatEvent::AuthSessionExpired {
                        account_key: account_key.clone(),
                    })
                    .await;
                    cursor.clear();
                    if persist_legacy_state {
                        *self.cursor.write().await = String::new();
                    }
                    if let Err(e) = self.login_qr().await {
                        self.report_error(&e);
                    }
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
        self.client.send_message(base_url, token, &msg).await?;
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
            self.client.send_message(&base_url, &token, &msg).await?;
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
        if let Some(ref cb) = self.on_error {
            cb(err);
        }
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
    // Simple ISO 8601 without chrono dependency
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
    async fn cursor_is_process_local_until_adapter_store_migrates() {
        let root = std::env::temp_dir().join(format!("wechat-ilink-test-{}", Uuid::new_v4()));

        let bot = WechatIlinkClient::new();
        *bot.cursor.write().await = "cursor-1".to_string();
        assert!(!root.join("cursors.json").exists());

        let restored = WechatIlinkClient::new();
        assert_eq!(restored.cursor.read().await.as_str(), "");

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn run_from_cursor_does_not_mutate_stored_cursor_before_polling() {
        let client = WechatIlinkClient::new();
        *client.cursor.write().await = "stored-cursor".to_string();

        let result = client
            .run_from_cursor(Some("external-cursor".to_string()))
            .await;

        assert!(matches!(result, Err(WechatIlinkError::Auth(_))));
        assert_eq!(&*client.cursor.read().await, "stored-cursor");
    }

    #[tokio::test]
    async fn event_handler_accepts_context_observed_and_cursor_events() {
        let client = WechatIlinkClient::new();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_for_handler = Arc::clone(&seen);

        client
            .on_event(Box::new(move |event| {
                seen_for_handler.lock().unwrap().push(match event {
                    WechatEvent::ContextObserved(ctx) => format!("context:{}", ctx.user_id),
                    WechatEvent::CursorAdvanced { cursor, .. } => format!("cursor:{cursor}"),
                    WechatEvent::Message(msg) => format!("message:{}", msg.user_id),
                    WechatEvent::AuthSessionExpired { account_key } => {
                        format!("auth:{account_key}")
                    }
                });
            }))
            .await;

        client
            .emit_event(WechatEvent::ContextObserved(WechatContext {
                account_key: "account-1".to_string(),
                user_id: "user-1".to_string(),
                context_token: "ctx-1".to_string(),
                observed_at_unix_ms: 1,
                source_message_id: Some("msg-1".to_string()),
            }))
            .await;
        client
            .emit_event(WechatEvent::CursorAdvanced {
                account_key: "account-1".to_string(),
                cursor: "cursor-2".to_string(),
            })
            .await;

        assert_eq!(
            &*seen.lock().unwrap(),
            &["context:user-1", "cursor:cursor-2"]
        );
    }

    #[tokio::test]
    async fn credentials_are_supplied_by_caller() {
        let client = WechatIlinkClient::new();
        client
            .set_credentials(Credentials {
                token: "token-1".into(),
                base_url: "https://example.invalid".into(),
                account_id: "account-1".into(),
                user_id: "bot-1".into(),
                saved_at: None,
            })
            .await;

        let creds = client.credentials().await.unwrap();
        assert_eq!(creds.account_id, "account-1");
    }
}
