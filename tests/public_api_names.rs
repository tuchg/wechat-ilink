use wechat_ilink::{Result, WechatIlinkClient, WechatIlinkError};

#[test]
fn renamed_public_api_is_available_from_wechat_ilink_crate() {
    let _client = WechatIlinkClient::builder()
        .bot_agent("Amux/0.1")
        .ilink_app_id("bot")
        .markdown_filter(true)
        .build();
    let err = WechatIlinkError::NoContext("user-1".to_string());
    let result: Result<()> = Err(err);
    assert!(matches!(result, Err(WechatIlinkError::NoContext(user)) if user == "user-1"));
}
