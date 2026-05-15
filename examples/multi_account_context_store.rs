use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use wechat_ilink::{Credentials, WechatContext, WechatEvent, WechatIlinkClient};

#[derive(Debug, Clone)]
struct AccountConfig {
    name: String,
    state_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoreFile {
    credentials: Option<Credentials>,
    cursors: BTreeMap<String, String>,
    contexts: BTreeMap<String, WechatContext>,
}

#[derive(Debug, Clone)]
struct AccountStore {
    path: PathBuf,
    state: Arc<RwLock<StoreFile>>,
}

impl AccountStore {
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

    async fn credentials(&self) -> Option<Credentials> {
        self.state.read().await.credentials.clone()
    }

    async fn save_credentials(&self, credentials: Credentials) -> std::io::Result<()> {
        self.state.write().await.credentials = Some(credentials);
        self.save().await
    }

    async fn cursor(&self, account_key: &str) -> Option<String> {
        self.state.read().await.cursors.get(account_key).cloned()
    }

    async fn save_cursor(&self, account_key: String, cursor: String) -> std::io::Result<()> {
        self.state.write().await.cursors.insert(account_key, cursor);
        self.save().await
    }

    async fn upsert_context(&self, context: WechatContext) -> std::io::Result<()> {
        let key = context_key(&context.account_key, &context.user_id);
        self.state.write().await.contexts.insert(key, context);
        self.save().await
    }

    async fn context(&self, account_key: &str, user_id: &str) -> Option<WechatContext> {
        self.state
            .read()
            .await
            .contexts
            .get(&context_key(account_key, user_id))
            .cloned()
    }
}

fn context_key(account_key: &str, user_id: &str) -> String {
    format!("{account_key}:{user_id}")
}

fn account_key(credentials: &Credentials) -> String {
    if credentials.account_id.is_empty() {
        credentials.user_id.clone()
    } else {
        credentials.account_id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let accounts = accounts_from_env();
    let mut tasks = Vec::new();

    for account in accounts {
        tasks.push(tokio::spawn(run_account(account)));
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}

fn accounts_from_env() -> Vec<AccountConfig> {
    let names = std::env::var("WECHAT_ILINK_ACCOUNTS").unwrap_or_else(|_| "work,personal".into());
    names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| AccountConfig {
            name: name.to_string(),
            state_path: PathBuf::from(format!("./data/wechat-ilink-{name}.json")),
        })
        .collect()
}

async fn run_account(
    config: AccountConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = AccountStore::open(&config.state_path).await?;
    let client = Arc::new(
        WechatIlinkClient::builder()
            .bot_agent(format!("MultiAccountExample/0.1 ({})", config.name))
            .ilink_app_id("bot")
            .markdown_filter(true)
            .on_qr_url({
                let account_name = config.name.clone();
                move |url| eprintln!("[{account_name}] scan QR: {url}")
            })
            .build(),
    );

    let credentials = match store.credentials().await {
        Some(credentials) => {
            client.set_credentials(credentials.clone()).await;
            credentials
        }
        None => {
            // First run for each account opens its own QR login flow.
            let credentials = client.login_qr().await?;
            store.save_credentials(credentials.clone()).await?;
            credentials
        }
    };
    let account_key = account_key(&credentials);

    let event_store = store.clone();
    let account_name = config.name.clone();
    client
        .on_event(Box::new(move |event| {
            let event_store = event_store.clone();
            let account_name = account_name.clone();
            let event = event.clone();
            tokio::spawn(async move {
                match event {
                    WechatEvent::ContextObserved(context) => {
                        // Contexts include protocol account_key. This prevents collisions when
                        // two login accounts see the same WeChat user_id.
                        let _ = event_store.upsert_context(context).await;
                    }
                    WechatEvent::CursorAdvanced {
                        account_key,
                        cursor,
                    } => {
                        let _ = event_store.save_cursor(account_key, cursor).await;
                    }
                    WechatEvent::Message(message) => {
                        println!("[{account_name}] {}: {}", message.user_id, message.text);
                    }
                    WechatEvent::AuthSessionExpired { account_key } => {
                        eprintln!(
                            "[{account_name}] auth expired for {account_key}; re-login required"
                        );
                    }
                    WechatEvent::UserInteractionRequested {
                        account_key,
                        user_id,
                        reason,
                    } => {
                        eprintln!(
                            "[{account_name}] user interaction suggested for {account_key}, user {user_id:?}: {reason:?}"
                        );
                    }
                }
            });
        }))
        .await;

    // Optional demo send for this specific login account:
    // WECHAT_ILINK_SEND_TO_USER=<user_id> cargo run --example multi_account_context_store
    if let Ok(user_id) = std::env::var("WECHAT_ILINK_SEND_TO_USER") {
        match store.context(&account_key, &user_id).await {
            Some(context) => {
                client
                    .send_text_with_context(&context, "hello from multi-account example")
                    .await?;
            }
            None => {
                eprintln!(
                    "[{}] no stored context for user {}; wait for that user to send a message",
                    config.name, user_id
                );
            }
        }
    }

    let cursor = store.cursor(&account_key).await;
    client.run_from_cursor(cursor).await?;
    Ok(())
}
