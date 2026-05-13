use hex;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::time::SystemTime;

/// Message sender type.
/// Uses serde_repr for integer (de)serialization: JSON `1` ↔ `MessageType::User`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum MessageType {
    None = 0,
    User = 1,
    Bot = 2,
}

impl Default for MessageType {
    fn default() -> Self {
        Self::None
    }
}

/// Message delivery state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum MessageState {
    New = 0,
    Generating = 1,
    Finish = 2,
}

impl Default for MessageState {
    fn default() -> Self {
        Self::New
    }
}

/// Content type of a message item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum MessageItemType {
    None = 0,
    Text = 1,
    Image = 2,
    Voice = 3,
    File = 4,
    Video = 5,
}

impl Default for MessageItemType {
    fn default() -> Self {
        Self::None
    }
}

/// Media type for upload requests.
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum MediaType {
    Image = 1,
    Video = 2,
    File = 3,
    Voice = 4,
}

/// CDN media reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CDNMedia {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypt_query_param: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aes_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypt_type: Option<i32>,
    /// Complete download URL returned by server; when set, use directly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_url: Option<String>,
}

/// Text content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// Image content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aeskey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mid_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_height: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hd_size: Option<i64>,
}

/// Voice content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode_type: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bits_per_sample: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playtime: Option<i32>,
}

/// File content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<String>,
}

/// Video content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub play_length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_media: Option<CDNMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_height: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumb_width: Option<i32>,
}

/// Referenced/quoted message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_item: Option<Box<WireMessageItem>>,
}

/// A single content item in a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessageItem {
    #[serde(rename = "type")]
    #[serde(default)]
    pub item_type: MessageItemType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_time_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_completed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_item: Option<TextItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_item: Option<ImageItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice_item: Option<VoiceItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_item: Option<FileItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_item: Option<VideoItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_msg: Option<RefMessage>,
}

/// Raw wire message from the iLink API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    #[serde(default)]
    pub from_user_id: String,
    #[serde(default)]
    pub to_user_id: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub create_time_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_time_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_time_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default)]
    pub message_type: MessageType,
    #[serde(default)]
    pub message_state: MessageState,
    #[serde(default)]
    pub context_token: String,
    #[serde(default)]
    pub item_list: Vec<WireMessageItem>,
}

/// Context information observed from an incoming message.
///
/// Carries the context token, user identity, and the account key that
/// was used to observe it. The [`Debug`] impl redacts the context token
/// to avoid leaking it into logs. Serialization includes the raw token;
/// do not log serialized contexts.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WechatContext {
    /// Account key used when this context was observed.
    pub account_key: String,
    /// WeChat user ID from whom this context was observed.
    pub user_id: String,
    /// Opaque context token for reply/send operations.
    pub context_token: String,
    /// Unix timestamp in milliseconds when the context was observed.
    pub observed_at_unix_ms: i64,
    /// Message ID of the message that produced this context, if any.
    pub source_message_id: Option<String>,
}

impl std::fmt::Debug for WechatContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WechatContext")
            .field("account_key", &self.account_key)
            .field("user_id", &self.user_id)
            .field("context_token", &"<redacted>")
            .field("observed_at_unix_ms", &self.observed_at_unix_ms)
            .field("source_message_id", &self.source_message_id)
            .finish()
    }
}

impl WechatContext {
    /// Returns a stable fingerprint of the context token.
    ///
    /// Two contexts with the same token always produce the same fingerprint,
    /// regardless of other fields. Useful for detecting token rotation without
    /// comparing the raw token.
    pub fn token_fingerprint(&self) -> String {
        let digest = Md5::digest(self.context_token.as_bytes());
        format!("md5:{}", hex::encode(digest))
    }

    /// Returns the observation time as a [`SystemTime`].
    pub fn observed_at(&self) -> SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.observed_at_unix_ms as u64)
    }
}

/// Parsed incoming message — user-friendly.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub message_id: Option<String>,
    pub wire_message_id: Option<i64>,
    pub client_id: String,
    pub user_id: String,
    pub text: String,
    pub content_type: ContentType,
    pub timestamp: SystemTime,
    pub images: Vec<ImageContent>,
    pub voices: Vec<VoiceContent>,
    pub files: Vec<FileContent>,
    pub videos: Vec<VideoContent>,
    pub quoted: Option<QuotedMessage>,
    pub raw: WireMessage,
    pub(crate) context_token: String,
    /// Context observed from this message, including the account key.
    pub context: Option<WechatContext>,
}

impl IncomingMessage {
    /// Opaque reply token bound to this message.
    ///
    /// Prefer the full [`context`](Self::context) with
    /// [`WechatIlinkClient::send_text_with_context`](crate::WechatIlinkClient::send_text_with_context)
    /// or [`WechatIlinkClient::reply`](crate::WechatIlinkClient::reply), which uses it automatically.
    /// Use this getter only when constructing low-level protocol payloads with
    /// [`protocol::build_text_message`](crate::protocol::build_text_message) /
    /// [`protocol::build_media_message`](crate::protocol::build_media_message)
    /// for [`ILinkClient::send_message`](crate::protocol::ILinkClient::send_message).
    pub fn context_token(&self) -> &str {
        &self.context_token
    }

    /// Parse a raw [`WireMessage`] into a user-friendly [`IncomingMessage`],
    /// associating the context with the given account key.
    ///
    /// Prefer this over [`from_wire`](Self::from_wire) when the account key
    /// is known, so that [`context`](Self::context) carries correct
    /// [`WechatContext::account_key`] information.
    ///
    /// Returns `None` if the wire message is not a user-originated message
    /// (e.g. it was sent by the bot itself).
    pub fn from_wire_for_account(wire: &WireMessage, account_key: &str) -> Option<Self> {
        Self::from_wire_inner(wire, account_key)
    }

    /// Parse a raw [`WireMessage`] into a user-friendly [`IncomingMessage`].
    ///
    /// Uses a fallback account key derived from the wire message. When the
    /// account key is known, prefer [`from_wire_for_account`](Self::from_wire_for_account).
    ///
    /// Returns `None` if the wire message is not a user-originated message
    /// (e.g. it was sent by the bot itself).
    ///
    /// This is the stable entry point for consumers who drive
    /// [`ILinkClient::get_updates`](crate::protocol::ILinkClient::get_updates)
    /// themselves instead of using [`WechatIlinkClient`](crate::WechatIlinkClient)'s
    /// dispatcher.
    pub fn from_wire(wire: &WireMessage) -> Option<Self> {
        let fallback_account_key = if wire.to_user_id.is_empty() {
            wire.from_user_id.as_str()
        } else {
            wire.to_user_id.as_str()
        };
        Self::from_wire_inner(wire, fallback_account_key)
    }

    fn from_wire_inner(wire: &WireMessage, account_key: &str) -> Option<Self> {
        if wire.message_type != MessageType::User {
            return None;
        }

        let source_message_id = current_message_id(wire);

        let mut msg = IncomingMessage {
            message_id: source_message_id.clone(),
            wire_message_id: wire.message_id,
            client_id: wire.client_id.clone(),
            user_id: wire.from_user_id.clone(),
            text: extract_text(&wire.item_list),
            content_type: detect_type(&wire.item_list),
            timestamp: std::time::UNIX_EPOCH
                + std::time::Duration::from_millis(wire.create_time_ms as u64),
            images: Vec::new(),
            voices: Vec::new(),
            files: Vec::new(),
            videos: Vec::new(),
            quoted: None,
            raw: wire.clone(),
            context_token: wire.context_token.clone(),
            context: (!wire.context_token.is_empty()).then(|| WechatContext {
                account_key: account_key.to_string(),
                user_id: wire.from_user_id.clone(),
                context_token: wire.context_token.clone(),
                observed_at_unix_ms: wire.create_time_ms,
                source_message_id: source_message_id.clone(),
            }),
        };

        for item in &wire.item_list {
            if let Some(ref img) = item.image_item {
                msg.images.push(ImageContent {
                    media: img.media.clone(),
                    thumb_media: img.thumb_media.clone(),
                    aes_key: img.aeskey.clone(),
                    url: img.url.clone(),
                    width: img.thumb_width,
                    height: img.thumb_height,
                });
            }
            if let Some(ref voice) = item.voice_item {
                msg.voices.push(VoiceContent {
                    media: voice.media.clone(),
                    text: voice.text.clone(),
                    duration_ms: voice.playtime,
                    encode_type: voice.encode_type,
                });
            }
            if let Some(ref file) = item.file_item {
                msg.files.push(FileContent {
                    media: file.media.clone(),
                    file_name: file.file_name.clone(),
                    md5: file.md5.clone(),
                    size: file.len.as_ref().and_then(|s| s.parse().ok()),
                });
            }
            if let Some(ref video) = item.video_item {
                msg.videos.push(VideoContent {
                    media: video.media.clone(),
                    thumb_media: video.thumb_media.clone(),
                    duration_ms: video.play_length,
                });
            }
            if let Some(ref refm) = item.ref_msg {
                msg.quoted = Some(QuotedMessage {
                    message_id: quoted_message_id(refm),
                    title: refm.title.clone(),
                    text: refm
                        .message_item
                        .as_ref()
                        .and_then(|i| i.text_item.as_ref())
                        .and_then(|t| t.text.clone()),
                });
            }
        }

        Some(msg)
    }
}

fn quoted_message_id(refm: &RefMessage) -> Option<String> {
    refm.message_item
        .as_ref()
        .and_then(|item| item.msg_id.clone())
        .or_else(|| refm.msg_id.clone())
        .or_else(|| refm.message_id.map(|id| id.to_string()))
        .or_else(|| refm.client_id.clone())
}

fn current_message_id(wire: &WireMessage) -> Option<String> {
    wire.item_list
        .iter()
        .find_map(|item| item.msg_id.clone())
        .or_else(|| wire.message_id.map(|id| id.to_string()))
        .or_else(|| (!wire.client_id.is_empty()).then(|| wire.client_id.clone()))
}

fn detect_type(items: &[WireMessageItem]) -> ContentType {
    items
        .first()
        .map_or(ContentType::Text, |item| match item.item_type {
            MessageItemType::Image => ContentType::Image,
            MessageItemType::Voice => ContentType::Voice,
            MessageItemType::File => ContentType::File,
            MessageItemType::Video => ContentType::Video,
            _ => ContentType::Text,
        })
}

fn extract_text(items: &[WireMessageItem]) -> String {
    items
        .iter()
        .filter_map(|item| match item.item_type {
            MessageItemType::None => None,
            MessageItemType::Text => item.text_item.as_ref().and_then(|t| t.text.clone()),
            MessageItemType::Image => Some(
                item.image_item
                    .as_ref()
                    .and_then(|i| i.url.clone())
                    .unwrap_or_else(|| "[image]".to_string()),
            ),
            MessageItemType::Voice => Some(
                item.voice_item
                    .as_ref()
                    .and_then(|v| v.text.clone())
                    .unwrap_or_else(|| "[voice]".to_string()),
            ),
            MessageItemType::File => Some(
                item.file_item
                    .as_ref()
                    .and_then(|f| f.file_name.clone())
                    .unwrap_or_else(|| "[file]".to_string()),
            ),
            MessageItemType::Video => Some("[video]".to_string()),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Content type of an incoming message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Text,
    Image,
    Voice,
    File,
    Video,
}

#[derive(Debug, Clone)]
pub struct ImageContent {
    pub media: Option<CDNMedia>,
    pub thumb_media: Option<CDNMedia>,
    pub aes_key: Option<String>,
    pub url: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct VoiceContent {
    pub media: Option<CDNMedia>,
    pub text: Option<String>,
    pub duration_ms: Option<i32>,
    pub encode_type: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct FileContent {
    pub media: Option<CDNMedia>,
    pub file_name: Option<String>,
    pub md5: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct VideoContent {
    pub media: Option<CDNMedia>,
    pub thumb_media: Option<CDNMedia>,
    pub duration_ms: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct QuotedMessage {
    pub message_id: Option<String>,
    pub title: Option<String>,
    pub text: Option<String>,
}

/// Result of downloading media from a message.
#[derive(Debug, Clone)]
pub struct DownloadedMedia {
    pub data: Vec<u8>,
    /// "image", "file", "video", "voice"
    pub media_type: String,
    pub file_name: Option<String>,
    pub format: Option<String>,
}

/// Result of uploading media to CDN.
#[derive(Debug, Clone)]
pub struct UploadResult {
    pub media: CDNMedia,
    pub aes_key: [u8; 16],
    pub encrypted_file_size: usize,
}

/// Stored login credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub token: String,
    #[serde(rename = "baseUrl")]
    pub base_url: String,
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saved_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! wire_message {
        ($($field:tt)*) => {
            WireMessage {
                seq: None,
                message_id: None,
                update_time_ms: None,
                delete_time_ms: None,
                session_id: None,
                group_id: None,
                $($field)*
            }
        };
    }

    macro_rules! wire_item {
        ($($field:tt)*) => {
            WireMessageItem {
                create_time_ms: None,
                update_time_ms: None,
                is_completed: None,
                msg_id: None,
                $($field)*
            }
        };
    }

    #[test]
    fn message_type_values() {
        assert_eq!(MessageType::None as i32, 0);
        assert_eq!(MessageType::User as i32, 1);
        assert_eq!(MessageType::Bot as i32, 2);
    }

    #[test]
    fn message_state_values() {
        assert_eq!(MessageState::New as i32, 0);
        assert_eq!(MessageState::Generating as i32, 1);
        assert_eq!(MessageState::Finish as i32, 2);
    }

    #[test]
    fn message_item_type_values() {
        assert_eq!(MessageItemType::None as i32, 0);
        assert_eq!(MessageItemType::Text as i32, 1);
        assert_eq!(MessageItemType::Image as i32, 2);
        assert_eq!(MessageItemType::Voice as i32, 3);
        assert_eq!(MessageItemType::File as i32, 4);
        assert_eq!(MessageItemType::Video as i32, 5);
    }

    #[test]
    fn wire_message_json_round_trip() {
        let wire = wire_message! {
            from_user_id: "user1".to_string(),
            to_user_id: "bot1".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::User,
            message_state: MessageState::Finish,
            context_token: "ctx".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("hello".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            }],
        };
        let json = serde_json::to_string(&wire).unwrap();
        let decoded: WireMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.from_user_id, "user1");
        assert_eq!(decoded.message_type, MessageType::User);
        assert_eq!(decoded.item_list.len(), 1);
        assert_eq!(
            decoded.item_list[0]
                .text_item
                .as_ref()
                .unwrap()
                .text
                .as_deref(),
            Some("hello")
        );
    }

    #[test]
    fn wire_message_accepts_missing_optional_typescript_fields() {
        let wire: WireMessage = serde_json::from_value(serde_json::json!({}))
            .expect("wire message with optional fields");

        assert_eq!(wire.from_user_id, "");
        assert_eq!(wire.to_user_id, "");
        assert_eq!(wire.client_id, "");
        assert_eq!(wire.create_time_ms, 0);
        assert_eq!(wire.message_type, MessageType::None);
        assert_eq!(wire.message_state, MessageState::New);
        assert_eq!(wire.context_token, "");
        assert!(wire.item_list.is_empty());
    }

    #[test]
    fn wire_message_item_defaults_missing_type_to_none() {
        let item: WireMessageItem =
            serde_json::from_value(serde_json::json!({ "text_item": { "text": "hello" } }))
                .expect("message item with optional type");

        assert_eq!(item.item_type, MessageItemType::None);
        assert_eq!(
            item.text_item
                .as_ref()
                .and_then(|text| text.text.as_deref()),
            Some("hello")
        );
    }

    #[test]
    fn credentials_json_camel_case() {
        let creds = Credentials {
            token: "tok".to_string(),
            base_url: "https://api.example.com".to_string(),
            account_id: "acc1".to_string(),
            user_id: "uid1".to_string(),
            saved_at: Some("2024-01-01T00:00:00Z".to_string()),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert!(json.contains("\"baseUrl\""), "expected camelCase baseUrl");
        assert!(
            json.contains("\"accountId\""),
            "expected camelCase accountId"
        );
        assert!(json.contains("\"userId\""), "expected camelCase userId");

        let decoded: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.token, "tok");
        assert_eq!(decoded.base_url, "https://api.example.com");
    }

    #[test]
    fn credentials_omits_none_saved_at() {
        let creds = Credentials {
            token: "tok".to_string(),
            base_url: "https://api.example.com".to_string(),
            account_id: "acc1".to_string(),
            user_id: "uid1".to_string(),
            saved_at: None,
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains("saved_at"), "should omit None saved_at");
    }

    #[test]
    fn cdn_media_json() {
        let media = CDNMedia {
            encrypt_query_param: Some("param=abc".to_string()),
            aes_key: Some("key123".to_string()),
            encrypt_type: Some(1),
            full_url: None,
        };
        let json = serde_json::to_string(&media).unwrap();
        let decoded: CDNMedia = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.encrypt_query_param.as_deref(), Some("param=abc"));
        assert_eq!(decoded.aes_key.as_deref(), Some("key123"));
        assert_eq!(decoded.encrypt_type, Some(1));
    }

    #[test]
    fn wire_message_with_image() {
        let wire = wire_message! {
            from_user_id: "user1".to_string(),
            to_user_id: "bot1".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::User,
            message_state: MessageState::Finish,
            context_token: "ctx".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Image,
                text_item: None,
                image_item: Some(ImageItem {
                    media: None,
                    thumb_media: None,
                    aeskey: Some("key".to_string()),
                    url: Some("http://img.jpg".to_string()),
                    mid_size: Some(1024),
                    thumb_size: None,
                    thumb_width: Some(100),
                    thumb_height: Some(200),
                    hd_size: None,
                }),
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            }],
        };
        let json = serde_json::to_string(&wire).unwrap();
        let decoded: WireMessage = serde_json::from_str(&json).unwrap();
        let img = decoded.item_list[0].image_item.as_ref().unwrap();
        assert_eq!(img.url, Some("http://img.jpg".to_string()));
        assert_eq!(img.thumb_width, Some(100));
    }

    #[test]
    fn content_type_equality() {
        assert_eq!(ContentType::Text, ContentType::Text);
        assert_ne!(ContentType::Text, ContentType::Image);
    }

    #[test]
    fn detect_type_text() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Text,
            text_item: Some(TextItem {
                text: Some("hi".to_string()),
            }),
            image_item: None,
            voice_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(detect_type(&items), ContentType::Text);
    }

    #[test]
    fn detect_type_image() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Image,
            text_item: None,
            image_item: Some(ImageItem {
                media: None,
                thumb_media: None,
                aeskey: None,
                url: Some("http://img".to_string()),
                mid_size: None,
                thumb_size: None,
                thumb_width: None,
                thumb_height: None,
                hd_size: None,
            }),
            voice_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(detect_type(&items), ContentType::Image);
    }

    #[test]
    fn detect_type_empty() {
        assert_eq!(detect_type(&[]), ContentType::Text);
    }

    #[test]
    fn extract_text_single() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Text,
            text_item: Some(TextItem {
                text: Some("hello world".to_string()),
            }),
            image_item: None,
            voice_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "hello world");
    }

    #[test]
    fn extract_text_multi() {
        let items = vec![
            wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("line1".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            },
            wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("line2".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            },
        ];
        assert_eq!(extract_text(&items), "line1\nline2");
    }

    #[test]
    fn extract_text_image_url() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Image,
            text_item: None,
            image_item: Some(ImageItem {
                media: None,
                thumb_media: None,
                aeskey: None,
                url: Some("http://img.jpg".to_string()),
                mid_size: None,
                thumb_size: None,
                thumb_width: None,
                thumb_height: None,
                hd_size: None,
            }),
            voice_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "http://img.jpg");
    }

    #[test]
    fn extract_text_image_placeholder() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Image,
            text_item: None,
            image_item: Some(ImageItem {
                media: None,
                thumb_media: None,
                aeskey: None,
                url: None,
                mid_size: None,
                thumb_size: None,
                thumb_width: None,
                thumb_height: None,
                hd_size: None,
            }),
            voice_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "[image]");
    }

    #[test]
    fn extract_text_voice_with_text() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Voice,
            text_item: None,
            image_item: None,
            voice_item: Some(VoiceItem {
                media: None,
                encode_type: None,
                bits_per_sample: None,
                sample_rate: None,
                text: Some("hello".to_string()),
                playtime: None,
            }),
            file_item: None,
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "hello");
    }

    #[test]
    fn extract_text_file_name() {
        let items = vec![wire_item! {
            item_type: MessageItemType::File,
            text_item: None,
            image_item: None,
            voice_item: None,
            file_item: Some(FileItem {
                media: None,
                file_name: Some("doc.pdf".to_string()),
                md5: None,
                len: None,
            }),
            video_item: None,
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "doc.pdf");
    }

    #[test]
    fn extract_text_video() {
        let items = vec![wire_item! {
            item_type: MessageItemType::Video,
            text_item: None,
            image_item: None,
            voice_item: None,
            file_item: None,
            video_item: Some(VideoItem {
                media: None,
                video_size: None,
                play_length: None,
                video_md5: None,
                thumb_media: None,
                thumb_size: None,
                thumb_height: None,
                thumb_width: None,
            }),
            ref_msg: None,
        }];
        assert_eq!(extract_text(&items), "[video]");
    }

    #[test]
    fn from_wire_user_text() {
        let wire = wire_message! {
            from_user_id: "user123".to_string(),
            to_user_id: "bot456".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::User,
            message_state: MessageState::Finish,
            context_token: "ctx-abc".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("hello".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            }],
        };
        let msg = IncomingMessage::from_wire(&wire).unwrap();
        assert_eq!(msg.user_id, "user123");
        assert_eq!(msg.text, "hello");
        assert_eq!(msg.content_type, ContentType::Text);
        assert_eq!(msg.context_token(), "ctx-abc");
    }

    #[test]
    fn from_wire_skips_bot() {
        let wire = wire_message! {
            from_user_id: "bot456".to_string(),
            to_user_id: "user123".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::Bot,
            message_state: MessageState::Finish,
            context_token: "ctx".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("reply".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            }],
        };
        assert!(IncomingMessage::from_wire(&wire).is_none());
    }

    #[test]
    fn from_wire_with_image() {
        let wire = wire_message! {
            from_user_id: "user123".to_string(),
            to_user_id: "bot456".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::User,
            message_state: MessageState::Finish,
            context_token: "ctx".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Image,
                text_item: None,
                image_item: Some(ImageItem {
                    media: None,
                    thumb_media: None,
                    aeskey: Some("key".to_string()),
                    url: Some("http://img.jpg".to_string()),
                    mid_size: None,
                    thumb_size: None,
                    thumb_width: Some(100),
                    thumb_height: Some(200),
                    hd_size: None,
                }),
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: None,
            }],
        };
        let msg = IncomingMessage::from_wire(&wire).unwrap();
        assert_eq!(msg.images.len(), 1);
        assert_eq!(msg.images[0].url, Some("http://img.jpg".to_string()));
        assert_eq!(msg.images[0].width, Some(100));
        assert_eq!(msg.images[0].height, Some(200));
    }

    #[test]
    fn from_wire_with_quoted() {
        let wire = wire_message! {
            from_user_id: "user123".to_string(),
            to_user_id: "bot456".to_string(),
            client_id: "c1".to_string(),
            create_time_ms: 1700000000000,
            message_type: MessageType::User,
            message_state: MessageState::Finish,
            context_token: "ctx".to_string(),
            item_list: vec![wire_item! {
                item_type: MessageItemType::Text,
                text_item: Some(TextItem {
                    text: Some("replying".to_string()),
                }),
                image_item: None,
                voice_item: None,
                file_item: None,
                video_item: None,
                ref_msg: Some(RefMessage {
                    msg_id: None,
                    message_id: None,
                    client_id: None,
                    title: Some("Original".to_string()),
                    message_item: Some(Box::new(wire_item! {
                        item_type: MessageItemType::Text,
                        text_item: Some(TextItem {
                            text: Some("original text".to_string()),
                        }),
                        image_item: None,
                        voice_item: None,
                        file_item: None,
                        video_item: None,
                        ref_msg: None,
                    })),
                }),
            }],
        };
        let msg = IncomingMessage::from_wire(&wire).unwrap();
        let quoted = msg.quoted.as_ref().unwrap();
        assert_eq!(quoted.title, Some("Original".to_string()));
        assert_eq!(quoted.text.as_deref(), Some("original text"));
    }
}
