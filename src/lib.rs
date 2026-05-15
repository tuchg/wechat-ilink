//! # wechat-ilink
//!
//! WeChat iLink protocol client for Rust.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use wechat_ilink::WechatIlinkClient;
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = WechatIlinkClient::builder().build();
//!     let credentials = client.login_qr().await.unwrap();
//!     // Persist credentials in your application store.
//!     let _ = credentials;
//!
//!     client.on_event(Box::new(|event| {
//!         println!("{event:?}");
//!     })).await;
//!
//!     client.run_from_cursor(None).await.unwrap();
//! }
//! ```

pub mod bot;
pub mod cdn;
pub mod crypto;
pub mod error;
mod markdown_filter;
pub mod protocol;
pub mod types;

pub use bot::{
    EventHandler, MessageHandler, SendContent, SendReceipt, UserInteractionReason, WechatEvent,
    WechatIlinkClient, WechatIlinkClientBuilder, WechatRateLimitOptions,
};
pub use cdn::CdnClient;
pub use crypto::{
    decode_aes_key, decrypt_aes_ecb, decrypt_aes_ecb as download_decrypt, encode_aes_key_base64,
    encode_aes_key_hex, encrypt_aes_ecb, generate_aes_key,
};
pub use error::{Result, WechatIlinkError};
pub use markdown_filter::filter_markdown;
pub use protocol::{
    build_cdn_upload_url, BaseInfo, GetConfigResponse, GetUpdatesResponse, GetUploadUrlParams,
    GetUploadUrlResponse, ILinkClientOptions, NotifyStartResponse, NotifyStopResponse,
};
pub use types::*;
