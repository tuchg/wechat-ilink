use std::sync::Arc;

use wechat_ilink::{LoginQrEvent, Result, WechatEvent, WechatIlinkClient, WechatIlinkError};

#[tokio::test]
async fn renamed_public_api_is_available_from_wechat_ilink_crate() {
    let client = Arc::new(
        WechatIlinkClient::builder()
            .bot_agent("Amux/0.1")
            .ilink_app_id("bot")
            .markdown_filter(true)
            .build(),
    );
    let _events = client.clone().events_from_cursor(None);
    let _login = client.login_qr();
    let _: Option<WechatEvent> = None;
    let _: Option<LoginQrEvent> = None;

    let err = WechatIlinkError::NoContext("user-1".to_string());
    let result: Result<()> = Err(err);
    assert!(matches!(result, Err(WechatIlinkError::NoContext(user)) if user == "user-1"));
}
