# wechat-ilink

[![crates.io](https://img.shields.io/crates/v/wechat-ilink.svg)](https://crates.io/crates/wechat-ilink)
[![docs.rs](https://docs.rs/wechat-ilink/badge.svg)](https://docs.rs/wechat-ilink)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![rust: 2021](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org/)

Stream-first async WeChat iLink protocol client for Rust. Builder-based, event-driven, and fully stateless — your app owns credentials, context tokens, and cursors. Handles QR login, event-driven polling, automatic context refresh from incoming messages, typed rate limiting with automatic backoff, and context-expiry interaction events.

中文：[README.md](README.md)

## Status

`wechat-ilink` is an experimental `0.x` crate.

- It is an unofficial client for a WeChat iLink protocol that may change without notice.
- APIs may change while real-world usage hardens the boundaries.
- The SDK does not persist credentials, user `context_token` values, or application cursors.
- You own credentials, user contexts, cursors, reminder policy, and retry policy.

## Install

```toml
[dependencies]
wechat-ilink = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "fs", "time", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Rust module name:

```rust
use wechat_ilink::WechatIlinkClient;
```

## Quick start

```rust,no_run
use std::sync::Arc;

use wechat_ilink::{LoginQrEvent, WechatEvent, WechatIlinkClient};

#[tokio::main]
async fn main() -> wechat_ilink::Result<()> {
    let client = Arc::new(
        WechatIlinkClient::builder()
            .bot_agent("MyBot/0.1")
            .ilink_app_id("bot")
            // Enabled by default. Disable to send text without WeChat Markdown filtering.
            .markdown_filter(true)
            // ret=-2 defaults to 90s wait and 5 retries; event fires near 6 sends / 5 minutes.
            .rate_limit_retry_after(std::time::Duration::from_secs(90))
            .rate_limit_max_retries(5)
            .rate_limit_interaction_threshold(6)
            // Context defaults to 24h TTL; event fires 30 minutes before expiry.
            .context_ttl(std::time::Duration::from_secs(24 * 60 * 60))
            .context_expiry_remind_before(std::time::Duration::from_secs(30 * 60))
            .build(),
    );

    // Credentials are owned by your application; the SDK does not read or write
    // credential files.
    if let Some(credentials) = my_load_credentials().await {
        client.set_credentials(credentials).await;
    } else {
        let mut login = client.login_qr();
        while let Some(event) = login.next().await {
            match event? {
                LoginQrEvent::QrCode { content } => eprintln!("scan QR: {content}"),
                LoginQrEvent::StatusChanged { status } => eprintln!("login status: {status}"),
                LoginQrEvent::NeedVerifyCode { responder, .. } => {
                    // The app may read a verification code and call responder.send(code).
                    let _ = responder.cancel();
                }
                LoginQrEvent::Confirmed { credentials } => {
                    my_save_credentials(&credentials).await;
                    break;
                }
            }
        }
    }

    let cursor = my_load_cursor().await;
    let mut events = client.events_from_cursor(cursor);
    while let Some(event) = events.next().await {
        match event? {
            WechatEvent::ContextObserved(context) => {
                // Automatically observed/refreshed from incoming messages; persist it
                // for later send_*_with_context calls.
                let _ = context;
            }
            WechatEvent::CursorAdvanced { account_key, cursor } => {
                // Persist cursor and pass it to events_from_cursor on next startup.
                let _ = (account_key, cursor);
            }
            WechatEvent::Message(message) => {
                println!("{}: {}", message.user_id, message.text);
            }
            WechatEvent::AuthSessionExpired { account_key } => {
                eprintln!("auth expired: {account_key}");
                break;
            }
            WechatEvent::UserInteractionRequested { account_key, user_id, reason } => {
                // SDK only notifies; the app decides whether to ask the user to reply in WeChat.
                eprintln!("user interaction suggested: {account_key} {user_id:?} {reason:?}");
            }
        }
    }
    Ok(())
}

async fn my_load_credentials() -> Option<wechat_ilink::Credentials> { None }
async fn my_save_credentials(_: &wechat_ilink::Credentials) {}
async fn my_load_cursor() -> Option<String> { None }
```

See [`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs) for a complete external store and keep-alive reminder example.

See [`examples/multi_account_context_store.rs`](examples/multi_account_context_store.rs) for multiple login accounts with separate credentials, cursors, and context stores.

## What this crate does

`wechat-ilink` is a low-level async protocol client. It handles:

- stream-based QR login, polling events, and errors;
- event-driven WeChat iLink polling;
- wire-message parsing into `IncomingMessage`;
- automatic `WechatContext` observation/refresh from incoming messages;
- explicit-context text, media, and typing sends;
- incoming media download and CDN upload helpers;
- protocol error mapping.

It is not a bot framework and does not own your application state.

## Core model

Incoming WeChat messages may include a `context_token`. The SDK exposes it as:

```rust
IncomingMessage.context: Option<WechatContext>
```

and emits it through:

```rust
WechatEvent::ContextObserved(WechatContext)
```

Your application should persist:

- `WechatContext` per user/account, for future proactive sends;
- cursor from `WechatEvent::CursorAdvanced`, for restart without replay;
- your own reminder markers, such as reminders asking users to send a test message every few hours.

## Sending messages

Reply to an incoming message:

```rust,no_run
# async fn example(bot: &wechat_ilink::WechatIlinkClient, msg: &wechat_ilink::IncomingMessage) -> wechat_ilink::Result<()> {
bot.reply(msg, "ack").await?;
# Ok(())
# }
```

Proactive sends require an application-stored `WechatContext`:

```rust,no_run
# async fn example(bot: &wechat_ilink::WechatIlinkClient, context: &wechat_ilink::WechatContext) -> wechat_ilink::Result<()> {
bot.send_text_with_context(context, "hello").await?;
bot.send_typing_with_context(context).await?;
# Ok(())
# }
```

Media sends also require explicit context:

```rust,no_run
# async fn example(bot: &wechat_ilink::WechatIlinkClient, context: &wechat_ilink::WechatContext) -> wechat_ilink::Result<()> {
bot.send_media_with_context(
    context,
    wechat_ilink::SendContent::File {
        data: b"hello".to_vec(),
        file_name: "hello.txt".into(),
    },
)
.await?;
# Ok(())
# }
```

## Events

`events_from_cursor` is the recommended integration point:

- `ContextObserved(WechatContext)`: a new context token was observed from an incoming message.
- `Message(IncomingMessage)`: parsed incoming message.
- `CursorAdvanced { account_key, cursor }`: polling cursor advanced; persist it.
- `AuthSessionExpired { account_key }`: auth expired; re-login is required.
- `UserInteractionRequested { account_key, user_id, reason }`: the SDK suggests asking the user to send a WeChat message. `reason` may be context expiry or proactive sends reaching the configured account threshold (default: 6 messages in 5 minutes). The SDK only notifies; callers decide whether and what to send, and via which channel.

For one update batch, the SDK emits `ContextObserved` / `Message` before `CursorAdvanced`, so callers can persist message-derived state before saving the cursor. When the user sends a WeChat message, the SDK resets that account's proactive-send counter and rate-limit backoff.

## External context store and keep-alive reminder example

Full examples are intentionally outside the README:

- [`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs): external storage plus a 4-hour keep-alive reminder.
- [`examples/multi_account_context_store.rs`](examples/multi_account_context_store.rs): multiple login accounts, each with separate credentials, cursors, and context stores.

It shows how to:

1. store `WechatContext` outside the SDK in a JSON file;
2. store the polling cursor outside the SDK;
3. send a proactive reminder every 4 hours asking users to reply with “测试”;
4. refresh the context token from `ContextObserved` when the user replies, which also resets the SDK proactive-send window.

In production, replace the JSON file with SQLite, Postgres, Redis, or your own store.

## What this crate does not do

`wechat-ilink` intentionally does not provide:

- user database;
- recipient management;
- credentials persistence;
- `context_token` persistence;
- cursor persistence;
- keep-alive reminder policy;
- durable queues or message coalescing for large application backlogs;
- application-level retry/suppression policy;
- bot workflow or conversation orchestration.

Those belong in your application.

## Security and privacy

- Do not log `context_token` values.
- Do not commit credential files.
- `WechatContext` redacts tokens in `Debug`, but serialization includes the raw token.
- If you store contexts in a database, protect them like credentials.

## Error handling

Important errors:

- `WechatIlinkError::NoContext(user_id)`: message has no context and cannot be used with `reply`.
- `WechatIlinkError::Api { .. }`: iLink API returned an error.
- `WechatIlinkError::Auth(..)`: not logged in or auth failed.
- `WechatIlinkError::Transport(..)`: network or HTTP client failure.

## Versioning

While the crate is `0.x`, minor versions may include API changes. Pin exact versions for production deployments.

## License

MIT
