use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use wechat_ilink::{Credentials, LoginQrEvent, WechatContext, WechatEvent, WechatIlinkClient};

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
            .build(),
    );

    let credentials = match store.credentials().await {
        Some(credentials) => {
            client.set_credentials(credentials.clone()).await;
            credentials
        }
        None => {
            // First run for each account opens its own QR login flow.
            login_and_save(&client, &store, &config.name).await?
        }
    };
    let account_key = account_key(&credentials);

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
    let mut events = Arc::clone(&client).stream_from_cursor(cursor);
    while let Some(event) = events.next().await {
        match event? {
            WechatEvent::ContextObserved(context) => {
                // Contexts include protocol account_key. This prevents collisions when
                // two login accounts see the same WeChat user_id.
                store.upsert_context(context).await?;
            }
            WechatEvent::CursorAdvanced {
                account_key,
                cursor,
            } => {
                store.save_cursor(account_key, cursor).await?;
            }
            WechatEvent::Message(message) => {
                println!("[{}] {}: {}", config.name, message.user_id, message.text);
            }
            WechatEvent::AuthSessionExpired { account_key } => {
                eprintln!(
                    "[{}] auth expired for {account_key}; re-login required",
                    config.name
                );
                break;
            }
            WechatEvent::UserInteractionRequested {
                account_key,
                user_id,
                reason,
            } => {
                eprintln!(
                    "[{}] user interaction suggested for {account_key}, user {user_id:?}: {reason:?}",
                    config.name
                );
            }
        }
    }
    Ok(())
}

async fn login_and_save(
    client: &WechatIlinkClient,
    store: &AccountStore,
    account_name: &str,
) -> wechat_ilink::Result<Credentials> {
    let mut login = client.login_qr_stream();
    while let Some(event) = login.next().await {
        match event? {
            LoginQrEvent::QrCode { content } => eprintln!("[{account_name}] scan QR: {content}"),
            LoginQrEvent::StatusChanged { status } => {
                eprintln!("[{account_name}] QR login status: {status}")
            }
            LoginQrEvent::NeedVerifyCode { prompt, responder } => {
                eprintln!("[{account_name}] {prompt}; verification-code entry not implemented in this example");
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
