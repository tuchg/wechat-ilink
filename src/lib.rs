//! # wechat-ilink
//!
//! WeChat iLink protocol client for Rust.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use wechat_ilink::{LoginQrEvent, WechatIlinkClient};
//!
//! #[tokio::main]
//! async fn main() -> wechat_ilink::Result<()> {
//!     let client = Arc::new(WechatIlinkClient::builder().build());
//!
//!     let mut login = client.login_qr_stream();
//!     while let Some(event) = login.next().await {
//!         match event? {
//!             LoginQrEvent::QrCode { content } => eprintln!("scan QR: {content}"),
//!             LoginQrEvent::Confirmed { credentials } => {
//!                 // Persist credentials in your application store.
//!                 let _ = credentials;
//!                 break;
//!             }
//!             LoginQrEvent::NeedVerifyCode { responder, .. } => {
//!                 let _ = responder.cancel();
//!             }
//!             LoginQrEvent::StatusChanged { .. } => {}
//!         }
//!     }
//!
//!     drop(login);
//!
//!     let mut events = client.stream_from_cursor(None);
//!     while let Some(event) = events.next().await {
//!         println!("{:?}", event?);
//!     }
//!     Ok(())
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
    LoginQrEvent, LoginQrStream, SendContent, SendReceipt, UserInteractionReason,
    VerifyCodeResponder, WechatEvent, WechatEventStream, WechatIlinkClient,
    WechatIlinkClientBuilder, WechatRateLimitOptions,
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
