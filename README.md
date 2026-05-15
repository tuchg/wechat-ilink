# wechat-ilink

非官方 WeChat iLink Rust 协议客户端，使用显式 context 和 cursor API。

English: [README.en.md](README.en.md)

## 状态

`wechat-ilink` 仍是实验性 `0.x` crate。

- 非官方客户端，WeChat iLink 协议可能随时变化。
- API 还会根据真实使用继续收敛。
- SDK 不持久化 credentials、用户 `context_token`，也不持久化业务 cursor。
- 凭证、用户 context、cursor、提醒策略、重试策略都由调用方负责。

## 安装

```toml
[dependencies]
wechat-ilink = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "fs", "time", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Rust 模块名：

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
        // 默认启用。关闭后，发送文本不会做 WeChat Markdown 兼容过滤。
        .markdown_filter(true)
        // ret=-2 默认等待 90s，最多重试 5 次；接近 5 分钟 6 条时发事件。
        .rate_limit_retry_after(std::time::Duration::from_secs(90))
        .rate_limit_max_retries(5)
        .rate_limit_interaction_threshold(6)
        // context 默认 24h TTL，到期前 30 分钟发 UserInteractionRequested 事件。
        .context_ttl(std::time::Duration::from_secs(24 * 60 * 60))
        .context_expiry_remind_before(std::time::Duration::from_secs(30 * 60))
        .on_qr_url(|url| eprintln!("scan QR: {url}"))
        .build();

    // 凭证由应用层加载；SDK 不读写 credential 文件。
    if let Some(credentials) = my_load_credentials().await {
        client.set_credentials(credentials).await;
    } else {
        let credentials = client.login_qr().await?;
        my_save_credentials(&credentials).await;
    }

    client
        .on_event(Box::new(|event| match event {
            WechatEvent::ContextObserved(context) => {
                // 保存 context，用于之后 send_text_with_context / send_media_with_context。
                let _ = context;
            }
            WechatEvent::CursorAdvanced { account_key, cursor } => {
                // 保存 cursor，下次启动传给 run_from_cursor。
                let _ = (account_key, cursor);
            }
            WechatEvent::Message(message) => {
                println!("{}: {}", message.user_id, message.text);
            }
            WechatEvent::AuthSessionExpired { account_key } => {
                eprintln!("auth expired: {account_key}");
            }
            WechatEvent::UserInteractionRequested { account_key, user_id, reason } => {
                // SDK 只通知；应用决定是否提醒用户在微信里发一条消息来刷新窗口。
                eprintln!("user interaction suggested: {account_key} {user_id:?} {reason:?}");
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

完整的外部存储与保活提醒示例见 [`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs)。

多登录账号示例见 [`examples/multi_account_context_store.rs`](examples/multi_account_context_store.rs)。

## 这个 crate 做什么

`wechat-ilink` 是底层异步协议客户端，负责：

- QR 登录并返回 credentials；
- WeChat iLink 长轮询；
- wire message 到 `IncomingMessage` 的解析；
- 显式 context 发送文本、媒体、typing；
- 入站媒体下载、CDN 上传辅助；
- 协议错误映射。

它不是 bot framework，不负责你的应用状态。

## 核心模型

WeChat 入站消息可能带 `context_token`。SDK 会把它暴露为：

```rust
IncomingMessage.context: Option<WechatContext>
```

并通过事件发出：

```rust
WechatEvent::ContextObserved(WechatContext)
```

调用方应该自己保存：

- 每个用户/账号的 `WechatContext`，用于之后主动发送；
- `WechatEvent::CursorAdvanced` 中的 cursor，用于重启后继续轮询；
- `WechatEvent::UserInteractionRequested` 用于通知应用“需要用户在微信里发条消息”：包括 context 接近过期、每账号 5 分钟内主动发送达到 6 条；用户在微信端发来消息会重置这些窗口；
- 自己的提醒记录，例如每几个小时提醒用户发测试消息刷新 context。

## 发送消息

回复入站消息：

```rust,no_run
# async fn example(bot: &wechat_ilink::WechatIlinkClient, msg: &wechat_ilink::IncomingMessage) -> wechat_ilink::Result<()> {
bot.reply(msg, "收到").await?;
# Ok(())
# }
```

主动发送必须提供调用方保存的 `WechatContext`：

```rust,no_run
# async fn example(bot: &wechat_ilink::WechatIlinkClient, context: &wechat_ilink::WechatContext) -> wechat_ilink::Result<()> {
bot.send_text_with_context(context, "你好").await?;
bot.send_typing_with_context(context).await?;
# Ok(())
# }
```

媒体发送同样需要显式 context：

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

## 事件

推荐通过 `WechatEvent` 集成：

- `ContextObserved(WechatContext)`：从入站消息观察到新的 context token。
- `Message(IncomingMessage)`：解析后的入站消息。
- `CursorAdvanced { account_key, cursor }`：轮询 cursor 前进，调用方应保存。
- `AuthSessionExpired { account_key }`：登录态过期，需要重新登录。
- `UserInteractionRequested { account_key, user_id, reason }`：SDK 建议应用请求用户在微信里发条消息。`reason` 可能是 context 接近过期，或该账号主动发送在窗口内达到阈值（默认 5 分钟内 6 条）。SDK 只通知，调用方决定是否提示用户、发什么、走哪个通道。

同一个 update batch 中，SDK 会先发 `ContextObserved` / `Message`，再发 `CursorAdvanced`。因此调用方可以先保存消息派生状态，再保存 cursor。用户在微信终端发来消息时，SDK 会重置该账号的主动发送计数和 rate-limit 退避。

## 外部存储 context 与保活提醒示例

完整示例不放在 README 里：

- [`examples/external_store_keepalive.rs`](examples/external_store_keepalive.rs)：外部存储 + 每 4 小时保活提醒。
- [`examples/multi_account_context_store.rs`](examples/multi_account_context_store.rs)：多个登录账号，每个账号独立 credentials/cursor/context store。

这个示例演示：

1. 在 SDK 外部用 JSON 文件保存 `WechatContext`；
2. 在 SDK 外部保存 polling cursor；
3. 每 4 小时主动提醒用户回复“测试”；
4. 用户回复后通过 `ContextObserved` 刷新 context token，并重置 SDK 的主动发送窗口。

生产环境建议把示例里的 JSON 文件替换成 SQLite、Postgres、Redis 或你自己的存储。

## 本 crate 不负责什么

`wechat-ilink` 有意不提供：

- 用户数据库；
- 收件人管理；
- credentials 持久化；
- `context_token` 持久化；
- cursor 持久化；
- 保活提醒策略；
- 大量业务消息的持久队列或合并策略；
- 应用层重试/抑制策略；
- bot 工作流或会话编排。

这些属于应用层。

## 安全与隐私

- 不要记录 `context_token` 到日志。
- 不要把凭证文件提交到仓库。
- `WechatContext` 的 `Debug` 会隐藏 token，但序列化会包含原始 token。
- 如果把 context 存到数据库，请按凭证级别保护。

## 错误处理

常见错误：

- `WechatIlinkError::NoContext(user_id)`：消息没有 context，无法用 `reply` 回复。
- `WechatIlinkError::Api { .. }`：iLink API 返回错误。
- `WechatIlinkError::Auth(..)`：未登录或鉴权失败。
- `WechatIlinkError::Transport(..)`：网络或 HTTP 客户端错误。

## 版本策略

在 `0.x` 阶段，minor 版本可能包含 API 变化。生产部署建议锁定精确版本。

## 许可证

MIT
