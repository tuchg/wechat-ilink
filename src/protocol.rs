//! Raw iLink Bot API HTTP calls.

use base64::Engine;
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

use crate::error::{Result, WechatIlinkError};
use crate::markdown_filter::filter_markdown;
#[allow(unused_imports)]
use crate::types::*;

pub const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
pub const CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
pub const CHANNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DEFAULT_BOT_AGENT: &str = "OpenClaw";
pub const DEFAULT_ILINK_APP_ID: &str = "bot";
pub const DEFAULT_RATE_LIMIT_RETRY_AFTER: Duration = Duration::from_secs(90);

const RATE_LIMIT_ERRCODE: i32 = -2;

/// Common request metadata attached to every CGI request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaseInfo {
    pub channel_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bot_agent: Option<String>,
}

/// Low-level iLink client options.
#[derive(Debug, Clone)]
pub struct ILinkClientOptions {
    pub bot_agent: Option<String>,
    pub route_tag: Option<String>,
    pub ilink_app_id: Option<String>,
    pub markdown_filter: bool,
}

impl Default for ILinkClientOptions {
    fn default() -> Self {
        Self {
            bot_agent: None,
            route_tag: None,
            ilink_app_id: None,
            markdown_filter: true,
        }
    }
}

/// Build iLink-App-ClientVersion from the crate version (0x00MMNNPP).
fn build_client_version() -> String {
    let version = env!("CARGO_PKG_VERSION");
    let parts: Vec<u32> = version.split('.').filter_map(|p| p.parse().ok()).collect();
    let major = parts.first().copied().unwrap_or(0) & 0xff;
    let minor = parts.get(1).copied().unwrap_or(0) & 0xff;
    let patch = parts.get(2).copied().unwrap_or(0) & 0xff;
    let num = (major << 16) | (minor << 8) | patch;
    num.to_string()
}

/// Generate the X-WECHAT-UIN header value.
pub fn random_wechat_uin() -> String {
    let mut buf = [0u8; 4];
    rand::rng().fill_bytes(&mut buf);
    let val = u32::from_be_bytes(buf);
    base64::engine::general_purpose::STANDARD.encode(val.to_string())
}

/// QR code response.
#[derive(Debug, Deserialize)]
pub struct QrCodeResponse {
    pub qrcode: String,
    pub qrcode_img_content: String,
}

/// QR status response.
#[derive(Debug, Deserialize)]
pub struct QrStatusResponse {
    pub status: String,
    pub bot_token: Option<String>,
    pub ilink_bot_id: Option<String>,
    pub ilink_user_id: Option<String>,
    pub baseurl: Option<String>,
    /// New host to redirect polling to when status is "scaned_but_redirect".
    pub redirect_host: Option<String>,
}

/// Get updates response.
#[derive(Debug, Deserialize)]
pub struct GetUpdatesResponse {
    #[serde(default)]
    pub ret: i32,
    #[serde(default)]
    pub msgs: Vec<WireMessage>,
    pub sync_buf: Option<String>,
    #[serde(default)]
    pub get_updates_buf: String,
    pub longpolling_timeout_ms: Option<i64>,
    pub errcode: Option<i32>,
    pub errmsg: Option<String>,
}

/// Get config response.
#[derive(Debug, Deserialize)]
pub struct GetConfigResponse {
    pub ret: Option<i32>,
    pub errmsg: Option<String>,
    pub typing_ticket: Option<String>,
}

/// Notify start response.
#[derive(Debug, Deserialize)]
pub struct NotifyStartResponse {
    pub ret: Option<i32>,
    pub errmsg: Option<String>,
}

/// Notify stop response.
#[derive(Debug, Deserialize)]
pub struct NotifyStopResponse {
    pub ret: Option<i32>,
    pub errmsg: Option<String>,
}

/// Low-level iLink API client.
#[derive(Debug)]
pub struct ILinkClient {
    http: Client,
    options: ILinkClientOptions,
}

fn default_http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap()
}

impl ILinkClient {
    pub fn new() -> Self {
        Self::with_options(ILinkClientOptions::default())
    }

    pub fn with_options(options: ILinkClientOptions) -> Self {
        Self::with_http_client_and_options(default_http_client(), options)
    }

    pub fn with_http_client(http: Client) -> Self {
        Self::with_http_client_and_options(http, ILinkClientOptions::default())
    }

    pub fn with_http_client_and_options(http: Client, options: ILinkClientOptions) -> Self {
        Self { http, options }
    }

    pub async fn get_qr_code(&self, base_url: &str) -> Result<QrCodeResponse> {
        self.get_qr_code_with_local_tokens(base_url, &[]).await
    }

    pub async fn get_qr_code_with_local_tokens(
        &self,
        base_url: &str,
        local_token_list: &[String],
    ) -> Result<QrCodeResponse> {
        let url = format!("{}/ilink/bot/get_bot_qrcode?bot_type=3", base_url);
        let resp = self
            .apply_common_headers(self.http.post(&url))
            .json(&json!({ "local_token_list": local_token_list }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    pub async fn poll_qr_status(&self, base_url: &str, qrcode: &str) -> Result<QrStatusResponse> {
        self.poll_qr_status_with_verify_code(base_url, qrcode, None)
            .await
    }

    pub async fn poll_qr_status_with_verify_code(
        &self,
        base_url: &str,
        qrcode: &str,
        verify_code: Option<&str>,
    ) -> Result<QrStatusResponse> {
        let url = format!(
            "{}{}",
            base_url,
            build_qr_status_endpoint(qrcode, verify_code)
        );
        let resp = match self
            .apply_common_headers(self.http.get(&url))
            .timeout(Duration::from_secs(35))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) if err.is_timeout() => return Ok(wait_qr_status_response()),
            Err(err) => return Err(err.into()),
        };
        Ok(resp.json().await?)
    }

    pub async fn get_updates(
        &self,
        base_url: &str,
        token: &str,
        cursor: &str,
    ) -> Result<GetUpdatesResponse> {
        let body = json!({
            "get_updates_buf": cursor,
            "base_info": self.base_info()
        });
        let resp = match self
            .api_post(base_url, "/ilink/bot/getupdates", token, &body, 45)
            .await
        {
            Ok(resp) => resp,
            Err(WechatIlinkError::Transport(err)) if err.is_timeout() => {
                return Ok(empty_updates_response(cursor));
            }
            Err(err) => return Err(err),
        };
        log_raw_getupdates_response(&resp);
        let result: GetUpdatesResponse = serde_json::from_value(resp)?;
        if result.ret != 0 || result.errcode.is_some_and(|c| c != 0) {
            let code = result.errcode.unwrap_or(result.ret);
            let msg = result
                .errmsg
                .unwrap_or_else(|| format!("ret={}", result.ret));
            return Err(api_error(msg, 200, code));
        }
        Ok(result)
    }

    pub async fn send_message(&self, base_url: &str, token: &str, msg: &Value) -> Result<()> {
        let body = json!({
            "msg": msg,
            "base_info": self.base_info()
        });
        self.api_post(base_url, "/ilink/bot/sendmessage", token, &body, 15)
            .await?;
        Ok(())
    }

    pub async fn get_config(
        &self,
        base_url: &str,
        token: &str,
        user_id: &str,
        context_token: &str,
    ) -> Result<GetConfigResponse> {
        let body = json!({
            "ilink_user_id": user_id,
            "context_token": context_token,
            "base_info": self.base_info()
        });
        let resp = self
            .api_post(base_url, "/ilink/bot/getconfig", token, &body, 15)
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn send_typing(
        &self,
        base_url: &str,
        token: &str,
        user_id: &str,
        ticket: &str,
        status: i32,
    ) -> Result<()> {
        let body = json!({
            "ilink_user_id": user_id,
            "typing_ticket": ticket,
            "status": status,
            "base_info": self.base_info()
        });
        self.api_post(base_url, "/ilink/bot/sendtyping", token, &body, 15)
            .await?;
        Ok(())
    }

    pub async fn notify_start(&self, base_url: &str, token: &str) -> Result<NotifyStartResponse> {
        let body = json!({ "base_info": self.base_info() });
        let resp = self
            .api_post(base_url, "/ilink/bot/msg/notifystart", token, &body, 10)
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn notify_stop(&self, base_url: &str, token: &str) -> Result<NotifyStopResponse> {
        let body = json!({ "base_info": self.base_info() });
        let resp = self
            .api_post(base_url, "/ilink/bot/msg/notifystop", token, &body, 10)
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    fn base_info(&self) -> BaseInfo {
        BaseInfo {
            channel_version: CHANNEL_VERSION.to_string(),
            bot_agent: Some(sanitize_bot_agent(self.options.bot_agent.as_deref())),
        }
    }

    fn apply_common_headers(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let app_id = self
            .options
            .ilink_app_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_ILINK_APP_ID);
        let request = request
            .header("iLink-App-Id", app_id)
            .header("iLink-App-ClientVersion", build_client_version());
        if let Some(route_tag) = self
            .options
            .route_tag
            .as_deref()
            .filter(|tag| !tag.is_empty())
        {
            request.header("SKRouteTag", route_tag)
        } else {
            request
        }
    }

    async fn api_post(
        &self,
        base_url: &str,
        endpoint: &str,
        token: &str,
        body: &Value,
        timeout_secs: u64,
    ) -> Result<Value> {
        let url = format!("{}{}", base_url, endpoint);
        let request = self
            .apply_common_headers(
                self.http
                    .post(&url)
                    .timeout(Duration::from_secs(timeout_secs))
                    .header("Content-Type", "application/json")
                    .header("AuthorizationType", "ilink_bot_token")
                    .header("Authorization", format!("Bearer {}", token))
                    .header("X-WECHAT-UIN", random_wechat_uin()),
            )
            .json(body);
        let resp = request.send().await?;

        let status = resp.status().as_u16();
        let text = resp.text().await?;
        let value: Value = serde_json::from_str(&text).unwrap_or(json!({}));

        if status >= 400 {
            let code = value["errcode"].as_i64().unwrap_or(0) as i32;
            return Err(api_error(
                api_error_message(&value, &text, code),
                status,
                code,
            ));
        }

        let api_error_code = value["errcode"]
            .as_i64()
            .filter(|code| *code != 0)
            .or_else(|| value["ret"].as_i64().filter(|code| *code != 0));
        if let Some(code) = api_error_code {
            let code = code as i32;
            return Err(api_error(
                api_error_message(&value, &text, code),
                status,
                code,
            ));
        }

        Ok(value)
    }
}

fn api_error(message: String, http_status: u16, errcode: i32) -> WechatIlinkError {
    if errcode == RATE_LIMIT_ERRCODE {
        WechatIlinkError::RateLimited {
            retry_after: DEFAULT_RATE_LIMIT_RETRY_AFTER,
            message,
            http_status,
            errcode,
        }
    } else {
        WechatIlinkError::Api {
            message,
            http_status,
            errcode,
        }
    }
}

fn api_error_message(value: &Value, raw_body: &str, code: i32) -> String {
    value["errmsg"]
        .as_str()
        .or_else(|| value["message"].as_str())
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            if code != 0 || raw_body.trim().is_empty() || raw_body.trim() == "{}" {
                format!("ret={code}")
            } else {
                raw_body.to_string()
            }
        })
}

fn log_raw_getupdates_response(resp: &Value) {
    let Some(msgs) = resp.get("msgs").and_then(Value::as_array) else {
        return;
    };
    if msgs.is_empty() {
        return;
    }
    let raw = resp.to_string();
    let truncated = raw.chars().count() > 24_000;
    let raw = if truncated {
        raw.chars().take(24_000).collect::<String>()
    } else {
        raw
    };
    debug!(
        target: "wechat_ilink::protocol",
        msg_count = msgs.len(),
        bytes = raw.len(),
        truncated,
        raw_json = %raw,
        "raw getupdates response"
    );
}

fn sanitize_bot_agent(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return DEFAULT_BOT_AGENT.to_string();
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return DEFAULT_BOT_AGENT.to_string();
    }

    let raw_tokens = raw.split_whitespace().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < raw_tokens.len() {
        let token = raw_tokens[i];
        if token.starts_with('(') && !token.ends_with(')') {
            let mut comment = token.to_string();
            while i + 1 < raw_tokens.len() && !comment.ends_with(')') {
                i += 1;
                comment.push(' ');
                comment.push_str(raw_tokens[i]);
            }
            tokens.push(comment);
        } else {
            tokens.push(token.to_string());
        }
        i += 1;
    }

    let mut accepted = Vec::new();
    let mut pending_product: Option<String> = None;
    for token in tokens {
        if token.starts_with('(') && token.ends_with(')') {
            let comment = &token[1..token.len() - 1];
            if let Some(product) = pending_product.take() {
                if is_bot_agent_comment(comment) {
                    accepted.push(format!("{product} ({comment})"));
                } else {
                    accepted.push(product);
                }
            }
            continue;
        }
        if let Some(product) = pending_product.take() {
            accepted.push(product);
        }
        if is_bot_agent_product(&token) {
            pending_product = Some(token);
        }
    }
    if let Some(product) = pending_product {
        accepted.push(product);
    }
    if accepted.is_empty() {
        return DEFAULT_BOT_AGENT.to_string();
    }

    let mut truncated = Vec::new();
    let mut len = 0;
    for token in accepted {
        let add = if truncated.is_empty() { 0 } else { 1 } + token.len();
        if len + add > 256 {
            break;
        }
        len += add;
        truncated.push(token);
    }
    if truncated.is_empty() {
        DEFAULT_BOT_AGENT.to_string()
    } else {
        truncated.join(" ")
    }
}

fn is_bot_agent_product(token: &str) -> bool {
    let Some((name, version)) = token.split_once('/') else {
        return false;
    };
    !name.is_empty()
        && !version.is_empty()
        && name.len() <= 32
        && version.len() <= 32
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
        && version
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '+' | '-'))
}

fn is_bot_agent_comment(comment: &str) -> bool {
    !comment.is_empty()
        && comment.len() <= 64
        && comment
            .chars()
            .all(|ch| ch.is_ascii() && (' '..='~').contains(&ch) && ch != '(' && ch != ')')
}

fn empty_updates_response(cursor: &str) -> GetUpdatesResponse {
    GetUpdatesResponse {
        ret: 0,
        msgs: Vec::new(),
        sync_buf: None,
        get_updates_buf: cursor.to_string(),
        longpolling_timeout_ms: None,
        errcode: None,
        errmsg: None,
    }
}

fn build_qr_status_endpoint(qrcode: &str, verify_code: Option<&str>) -> String {
    let mut endpoint = format!(
        "/ilink/bot/get_qrcode_status?qrcode={}",
        urlencoding::encode(qrcode)
    );
    if let Some(code) = verify_code.filter(|value| !value.is_empty()) {
        endpoint.push_str("&verify_code=");
        endpoint.push_str(&urlencoding::encode(code));
    }
    endpoint
}

fn wait_qr_status_response() -> QrStatusResponse {
    QrStatusResponse {
        status: "wait".to_string(),
        bot_token: None,
        ilink_bot_id: None,
        ilink_user_id: None,
        baseurl: None,
        redirect_host: None,
    }
}

/// Build a media message payload.
pub fn build_media_message(user_id: &str, context_token: &str, item_list: Vec<Value>) -> Value {
    build_media_message_with_client_id(
        user_id,
        context_token,
        item_list,
        &Uuid::new_v4().to_string(),
    )
}

/// Build a media message payload with a caller-supplied client id.
pub fn build_media_message_with_client_id(
    user_id: &str,
    context_token: &str,
    item_list: Vec<Value>,
    client_id: &str,
) -> Value {
    let item_list = item_list
        .into_iter()
        .map(|mut item| {
            if let Some(object) = item.as_object_mut() {
                object
                    .entry("msg_id")
                    .or_insert_with(|| Value::String(client_id.to_string()));
            }
            item
        })
        .collect::<Vec<_>>();
    json!({
        "from_user_id": "",
        "to_user_id": user_id,
        "client_id": client_id,
        "message_type": 2,
        "message_state": 2,
        "context_token": context_token,
        "item_list": item_list
    })
}

/// GetUploadUrl request parameters.
pub struct GetUploadUrlParams {
    pub filekey: String,
    pub media_type: i32,
    pub to_user_id: String,
    pub rawsize: usize,
    pub rawfilemd5: String,
    pub filesize: usize,
    pub thumb_rawsize: Option<usize>,
    pub thumb_rawfilemd5: Option<String>,
    pub thumb_filesize: Option<usize>,
    pub no_need_thumb: bool,
    pub aeskey: String,
}

/// GetUploadUrl response.
#[derive(Debug, Deserialize)]
pub struct GetUploadUrlResponse {
    pub upload_param: Option<String>,
    pub thumb_upload_param: Option<String>,
    pub upload_full_url: Option<String>,
}

impl ILinkClient {
    /// Get a pre-signed CDN upload URL.
    pub async fn get_upload_url(
        &self,
        base_url: &str,
        token: &str,
        params: &GetUploadUrlParams,
    ) -> Result<GetUploadUrlResponse> {
        let mut body = json!({
            "filekey": params.filekey,
            "media_type": params.media_type,
            "to_user_id": params.to_user_id,
            "rawsize": params.rawsize,
            "rawfilemd5": params.rawfilemd5,
            "filesize": params.filesize,
            "no_need_thumb": params.no_need_thumb,
            "aeskey": params.aeskey,
            "base_info": self.base_info()
        });
        if let Some(value) = params.thumb_rawsize {
            body["thumb_rawsize"] = json!(value);
        }
        if let Some(value) = &params.thumb_rawfilemd5 {
            body["thumb_rawfilemd5"] = json!(value);
        }
        if let Some(value) = params.thumb_filesize {
            body["thumb_filesize"] = json!(value);
        }
        let resp = self
            .api_post(base_url, "/ilink/bot/getuploadurl", token, &body, 15)
            .await?;
        Ok(serde_json::from_value(resp)?)
    }

    /// Upload encrypted bytes to CDN with retry (up to 3 attempts).
    /// Returns the download encrypted_query_param from the x-encrypted-param header.
    pub async fn upload_to_cdn(&self, cdn_url: &str, ciphertext: &[u8]) -> Result<String> {
        const MAX_RETRIES: u32 = 3;
        let mut last_err = None;

        for attempt in 1..=MAX_RETRIES {
            match self
                .http
                .post(cdn_url)
                .header("Content-Type", "application/octet-stream")
                .body(ciphertext.to_vec())
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if status >= 400 && status < 500 {
                        let err_msg = resp
                            .headers()
                            .get("x-error-message")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("client error")
                            .to_string();
                        return Err(WechatIlinkError::Media(format!(
                            "CDN upload client error {}: {}",
                            status, err_msg
                        )));
                    }
                    if status != 200 {
                        let err_msg = resp
                            .headers()
                            .get("x-error-message")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("server error")
                            .to_string();
                        last_err = Some(WechatIlinkError::Media(format!(
                            "CDN upload server error {}: {}",
                            status, err_msg
                        )));
                        continue;
                    }
                    match resp
                        .headers()
                        .get("x-encrypted-param")
                        .and_then(|v| v.to_str().ok())
                    {
                        Some(param) => return Ok(param.to_string()),
                        None => {
                            last_err = Some(WechatIlinkError::Media(
                                "CDN upload response missing x-encrypted-param header".into(),
                            ));
                            continue;
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(WechatIlinkError::Other(format!(
                        "CDN upload network error: {}",
                        e
                    )));
                    if attempt < MAX_RETRIES {
                        continue;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            WechatIlinkError::Media(format!("CDN upload failed after {} attempts", MAX_RETRIES))
        }))
    }

    /// Build a text message payload with this client's text normalization options.
    pub fn build_text_message_with_client_id(
        &self,
        user_id: &str,
        context_token: &str,
        text: &str,
        client_id: &str,
    ) -> Value {
        let text = if self.options.markdown_filter {
            filter_markdown(text)
        } else {
            text.to_string()
        };
        build_text_message_payload(user_id, context_token, &text, client_id)
    }
}

/// Build a CDN upload URL from params.
pub fn build_cdn_upload_url(cdn_base_url: &str, upload_param: &str, filekey: &str) -> String {
    format!(
        "{}/upload?encrypted_query_param={}&filekey={}",
        cdn_base_url,
        urlencoding::encode(upload_param),
        urlencoding::encode(filekey)
    )
}

/// Build a text message payload.
pub fn build_text_message(user_id: &str, context_token: &str, text: &str) -> Value {
    build_text_message_with_client_id(user_id, context_token, text, &Uuid::new_v4().to_string())
}

/// Build a text message payload with a caller-supplied client id.
pub fn build_text_message_with_client_id(
    user_id: &str,
    context_token: &str,
    text: &str,
    client_id: &str,
) -> Value {
    let text = filter_markdown(text);
    build_text_message_payload(user_id, context_token, &text, client_id)
}

fn build_text_message_payload(
    user_id: &str,
    context_token: &str,
    text: &str,
    client_id: &str,
) -> Value {
    json!({
        "from_user_id": "",
        "to_user_id": user_id,
        "client_id": client_id,
        "message_type": 2,
        "message_state": 2,
        "context_token": context_token,
        "item_list": [{ "type": 1, "msg_id": client_id, "text_item": { "text": text } }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ilink_client_accepts_caller_provided_reqwest_client() {
        let http_client = reqwest::Client::new();
        let client = ILinkClient::with_http_client_and_options(
            http_client,
            ILinkClientOptions {
                bot_agent: Some("Amux/0.1".to_string()),
                ..ILinkClientOptions::default()
            },
        );

        let base_info = client.base_info();
        assert_eq!(base_info.bot_agent.as_deref(), Some("Amux/0.1"));
    }

    #[test]
    fn base_info_preserves_bot_agent_when_configured() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            bot_agent: Some("Amux/0.1".to_string()),
            ..ILinkClientOptions::default()
        });

        let base_info = client.base_info();
        assert_eq!(base_info.channel_version, CHANNEL_VERSION);
        assert_eq!(base_info.bot_agent.as_deref(), Some("Amux/0.1"));
    }

    #[test]
    fn base_info_defaults_bot_agent() {
        let client = ILinkClient::new();

        let base_info = client.base_info();
        assert_eq!(base_info.bot_agent.as_deref(), Some("OpenClaw"));
    }

    #[test]
    fn base_info_sanitizes_bot_agent() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            bot_agent: Some(" Amux/0.1\tBad Agent\r\n".to_string()),
            ..ILinkClientOptions::default()
        });

        let base_info = client.base_info();
        assert_eq!(base_info.bot_agent.as_deref(), Some("Amux/0.1"));
    }

    #[test]
    fn base_info_preserves_valid_bot_agent_comment() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            bot_agent: Some("Amux/0.1 (worker channel) Other/2.0".to_string()),
            ..ILinkClientOptions::default()
        });

        let base_info = client.base_info();
        assert_eq!(
            base_info.bot_agent.as_deref(),
            Some("Amux/0.1 (worker channel) Other/2.0")
        );
    }

    #[test]
    fn empty_updates_response_preserves_cursor() {
        let response = empty_updates_response("cursor-1");

        assert_eq!(response.ret, 0);
        assert!(response.msgs.is_empty());
        assert_eq!(response.get_updates_buf, "cursor-1");
    }

    #[tokio::test]
    async fn send_message_rejects_nonzero_ret_response() {
        let (base_url, server) =
            json_response_server(r#"{"ret":40003,"errmsg":"invalid context_token"}"#);
        let client = ILinkClient::new();

        let err = client
            .send_message(&base_url, "token", &json!({"text": "hello"}))
            .await
            .expect_err("nonzero ret must be treated as an API error");

        match err {
            WechatIlinkError::Api {
                message,
                http_status,
                errcode,
            } => {
                assert_eq!(message, "invalid context_token");
                assert_eq!(http_status, 200);
                assert_eq!(errcode, 40003);
            }
            other => panic!("expected API error, got {other:?}"),
        }
        server.join().expect("response server should exit");
    }

    #[tokio::test]
    async fn send_message_maps_ret_minus_two_to_rate_limited() {
        let (base_url, server) = json_response_server(r#"{"ret":-2}"#);
        let client = ILinkClient::new();

        let err = client
            .send_message(&base_url, "token", &json!({"text": "hello"}))
            .await
            .expect_err("ret=-2 must be treated as rate limiting");

        match err {
            WechatIlinkError::RateLimited {
                retry_after,
                message,
                http_status,
                errcode,
            } => {
                assert_eq!(retry_after, Duration::from_secs(90));
                assert_eq!(message, "ret=-2");
                assert_eq!(http_status, 200);
                assert_eq!(errcode, -2);
            }
            other => panic!("expected rate limit error, got {other:?}"),
        }
        server.join().expect("response server should exit");
    }

    #[test]
    fn qr_status_endpoint_includes_verify_code_when_present() {
        let endpoint = build_qr_status_endpoint("qr value", Some("12 34"));

        assert_eq!(
            endpoint,
            "/ilink/bot/get_qrcode_status?qrcode=qr%20value&verify_code=12%2034"
        );
    }

    #[test]
    fn wait_qr_status_response_is_wait() {
        let response = wait_qr_status_response();

        assert_eq!(response.status, "wait");
        assert!(response.bot_token.is_none());
    }

    #[test]
    fn common_headers_preserve_route_tag_when_configured() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            route_tag: Some("route-a".to_string()),
            ..ILinkClientOptions::default()
        });

        let request = client
            .apply_common_headers(client.http.get("https://example.test"))
            .build()
            .expect("request");
        assert_eq!(
            request
                .headers()
                .get("SKRouteTag")
                .and_then(|value| value.to_str().ok()),
            Some("route-a")
        );
    }

    #[test]
    fn common_headers_use_configured_ilink_app_id() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            ilink_app_id: Some("custom-app".to_string()),
            ..ILinkClientOptions::default()
        });

        let request = client
            .apply_common_headers(client.http.get("https://example.test"))
            .build()
            .expect("request");
        assert_eq!(
            request
                .headers()
                .get("iLink-App-Id")
                .and_then(|value| value.to_str().ok()),
            Some("custom-app")
        );
    }

    #[test]
    fn text_message_builder_can_disable_markdown_filter() {
        let client = ILinkClient::with_options(ILinkClientOptions {
            markdown_filter: false,
            ..ILinkClientOptions::default()
        });

        let msg = client.build_text_message_with_client_id("user-1", "ctx-1", "# title", "msg-1");
        assert_eq!(msg["item_list"][0]["text_item"]["text"], "# title");
    }

    fn json_response_server(body: &'static str) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept test request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write test response");
        });
        (format!("http://{addr}"), server)
    }
}
