# wechat-ilink

Unofficial WeChat iLink protocol client for Rust with explicit context and cursor APIs.

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
wechat-ilink = "0.3"
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
use wechat_ilink::{WechatEvent, WechatIlinkClient};

#[tokio::main]
async fn main() -> wechat_ilink::Result<()> {
    let client = WechatIlinkClient::builder()
        .bot_agent("MyBot/0.1")
        .ilink_app_id("bot")
        // Enabled by default. Disable to send text without WeChat Markdown filtering.
        .markdown_filter(true)
        .on_qr_url(|url| eprintln!("scan QR: {url}"))
        .build();

    // Credentials are owned by your application; the SDK does not read or write
    // credential files.
    if let Some(credentials) = my_load_credentials().await {
        client.set_credentials(credentials).await;
    } else {
        let credentials = client.login_qr().await?;
        my_save_credentials(&credentials).await;
    }

    client
        .on_event(Box::new(|event| match event {
            WechatEvent::ContextObserved(context) => {
                // Persist context for later send_text_with_context / send_media_with_context.
                let _ = context;
            }
            WechatEvent::CursorAdvanced { account_key, cursor } => {
                // Persist cursor and pass it to run_from_cursor on next startup.
                let _ = (account_key, cursor);
            }
            WechatEvent::Message(message) => {
                println!("{}: {}", message.user_id, message.text);
            }
            WechatEvent::AuthSessionExpired { account_key } => {
                eprintln!("auth expired: {account_key}");
            }
        }))
        .await;

    let cursor = my_load_cursor().await;
    client.run_from_cursor(cursor).await
}

async fn my_load_credentials() -> Option<wechat_ilink::Credentials> { None }
async fn my_save_credentials(_: &wechat_ilink::Credentials) {}
async fn my_load_cursor() -> Option<String> { None }
```

See [`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs) for a complete external store and keep-alive reminder example.

## What this crate does

`wechat-ilink` is a low-level async protocol client. It handles:

- QR login that returns credentials;
- WeChat iLink long polling;
- wire-message parsing into `IncomingMessage`;
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

`WechatEvent` is the recommended integration point:

- `ContextObserved(WechatContext)`: a new context token was observed from an incoming message.
- `Message(IncomingMessage)`: parsed incoming message.
- `CursorAdvanced { account_key, cursor }`: polling cursor advanced; persist it.
- `AuthSessionExpired { account_key }`: auth expired; re-login is required.

For one update batch, the SDK emits `ContextObserved` / `Message` before `CursorAdvanced`, so callers can persist message-derived state before saving the cursor.

## External context store and keep-alive reminder example

The full example is intentionally outside the README:

[`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs)

It shows how to:

1. store `WechatContext` outside the SDK in a JSON file;
2. store the polling cursor outside the SDK;
3. send a proactive reminder every 4 hours asking users to reply with “测试”;
4. refresh the context token from `ContextObserved` when the user replies.

In production, replace the JSON file with SQLite, Postgres, Redis, or your own store.

## What this crate does not do

`wechat-ilink` intentionally does not provide:

- user database;
- recipient management;
- credentials persistence;
- `context_token` persistence;
- cursor persistence;
- keep-alive reminder policy;
- retry/suppression policy;
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
