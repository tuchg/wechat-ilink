use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use wechat_ilink::{Credentials, LoginQrEvent, WechatContext, WechatEvent, WechatIlinkClient};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoreFile {
    credentials: Option<Credentials>,
    contexts: BTreeMap<String, StoredContext>,
    cursors: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredContext {
    context: WechatContext,
    last_keepalive_reminder_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct Store {
    path: PathBuf,
    state: Arc<RwLock<StoreFile>>,
}

impl Store {
    async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let state = match tokio::fs::read_to_string(&path).await {
            Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => StoreFile::default(),
            Err(err) => return Err(err),
        };
        Ok(Self {
            path,
            state: Arc::new(RwLock::new(state)),
        })
    }

    async fn save(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let data = serde_json::to_string_pretty(&*self.state.read().await).unwrap();
        tokio::fs::write(&self.path, format!("{data}\n")).await
    }

    async fn upsert_context(&self, context: WechatContext) -> std::io::Result<()> {
        let key = context_key(&context.account_key, &context.user_id);
        self.state.write().await.contexts.insert(
            key,
            StoredContext {
                context,
                // Fresh token version. Allow next scheduled keep-alive reminder.
                last_keepalive_reminder_at_unix_ms: None,
            },
        );
        self.save().await
    }

    async fn save_cursor(&self, account_key: String, cursor: String) -> std::io::Result<()> {
        self.state.write().await.cursors.insert(account_key, cursor);
        self.save().await
    }

    async fn cursor(&self, account_key: &str) -> Option<String> {
        self.state.read().await.cursors.get(account_key).cloned()
    }

    async fn credentials(&self) -> Option<Credentials> {
        self.state.read().await.credentials.clone()
    }

    async fn save_credentials(&self, credentials: Credentials) -> std::io::Result<()> {
        self.state.write().await.credentials = Some(credentials);
        self.save().await
    }

    async fn contexts_due_for_keepalive(
        &self,
        now_ms: i64,
        interval: Duration,
    ) -> Vec<WechatContext> {
        let interval_ms = interval.as_millis() as i64;
        self.state
            .read()
            .await
            .contexts
            .values()
            .filter(|stored| {
                stored
                    .last_keepalive_reminder_at_unix_ms
                    .is_none_or(|last| now_ms.saturating_sub(last) >= interval_ms)
            })
            .map(|stored| stored.context.clone())
            .collect()
    }

    async fn mark_keepalive_reminded(
        &self,
        account_key: &str,
        user_id: &str,
        now_ms: i64,
    ) -> std::io::Result<()> {
        let key = context_key(account_key, user_id);
        if let Some(stored) = self.state.write().await.contexts.get_mut(&key) {
            stored.last_keepalive_reminder_at_unix_ms = Some(now_ms);
        }
        self.save().await
    }
}

fn context_key(account_key: &str, user_id: &str) -> String {
    format!("{account_key}:{user_id}")
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open("./data/wechat-ilink-state.json").await?;
    let client = Arc::new(
        WechatIlinkClient::builder()
            .bot_agent("MyBot/0.1")
            .ilink_app_id("bot")
            .markdown_filter(true)
            .build(),
    );

    let creds = match store.credentials().await {
        Some(creds) => {
            client.set_credentials(creds.clone()).await;
            creds
        }
        None => login_and_save(&client, &store).await?,
    };
    let account_key = if creds.account_id.is_empty() {
        creds.user_id.clone()
    } else {
        creds.account_id.clone()
    };

    let reminder_client = Arc::clone(&client);
    let reminder_store = store.clone();
    tokio::spawn(async move {
        let interval = Duration::from_secs(4 * 60 * 60);
        let mut tick = tokio::time::interval(interval);
        tick.tick().await; // consume immediate first tick; first reminder happens after interval

        loop {
            tick.tick().await;
            let now_ms = now_unix_ms();
            for context in reminder_store
                .contexts_due_for_keepalive(now_ms, interval)
                .await
            {
                let text = "为了保持微信会话可用，请回复一条“测试”。";
                match reminder_client.send_text_with_context(&context, text).await {
                    Ok(_) => {
                        let _ = reminder_store
                            .mark_keepalive_reminded(&context.account_key, &context.user_id, now_ms)
                            .await;
                    }
                    Err(err) => {
                        eprintln!("keep-alive reminder failed for {}: {err}", context.user_id);
                    }
                }
            }
        }
    });

    let cursor = store.cursor(&account_key).await;
    let mut events = Arc::clone(&client).events_from_cursor(cursor);
    while let Some(event) = events.next().await {
        match event? {
            WechatEvent::ContextObserved(context) => {
                store.upsert_context(context).await?;
            }
            WechatEvent::CursorAdvanced {
                account_key,
                cursor,
            } => {
                store.save_cursor(account_key, cursor).await?;
            }
            WechatEvent::AuthSessionExpired { account_key } => {
                eprintln!("WeChat auth expired for account {account_key}; re-login needed");
                break;
            }
            WechatEvent::Message(message) => {
                println!("{}: {}", message.user_id, message.text);
            }
            WechatEvent::UserInteractionRequested {
                account_key,
                user_id,
                reason,
            } => {
                eprintln!(
                    "WeChat user interaction suggested for account {account_key}, user {user_id:?}: {reason:?}"
                );
            }
        }
    }
    Ok(())
}

async fn login_and_save(
    client: &WechatIlinkClient,
    store: &Store,
) -> wechat_ilink::Result<Credentials> {
    let mut login = client.login_qr();
    while let Some(event) = login.next().await {
        match event? {
            LoginQrEvent::QrCode { content } => eprintln!("scan QR: {content}"),
            LoginQrEvent::StatusChanged { status } => eprintln!("QR login status: {status}"),
            LoginQrEvent::NeedVerifyCode { prompt, responder } => {
                eprintln!("{prompt}; verification-code entry not implemented in this example");
                let _ = responder.cancel();
            }
            LoginQrEvent::Confirmed { credentials } => {
                store
                    .save_credentials(credentials.clone())
                    .await
                    .map_err(wechat_ilink::WechatIlinkError::Io)?;
                return Ok(credentials);
            }
        }
    }
    Err(wechat_ilink::WechatIlinkError::Auth(
        "QR login stream ended before confirmation".into(),
    ))
}
