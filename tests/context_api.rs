use wechat_ilink::{
    IncomingMessage, SendContent, WechatContext, WechatIlinkClient, WechatIlinkError, WireMessage,
};

#[test]
fn incoming_message_exposes_redacted_context() {
    let wire: WireMessage = serde_json::from_value(serde_json::json!({
        "from_user_id": "user-1",
        "to_user_id": "bot-user",
        "client_id": "client-1",
        "create_time_ms": 1_700_000_000_000i64,
        "message_type": 1,
        "message_state": 2,
        "context_token": "secret-context-token",
        "item_list": [{ "type": 1, "msg_id": "msg-1", "text_item": { "text": "hi" } }]
    }))
    .expect("wire");

    let incoming = IncomingMessage::from_wire_for_account(&wire, "account-1").expect("incoming");
    let context = incoming.context.as_ref().expect("context");

    assert_eq!(context.account_key, "account-1");
    assert_eq!(context.user_id, "user-1");
    assert_eq!(context.context_token, "secret-context-token");
    assert_eq!(context.source_message_id.as_deref(), Some("msg-1"));
    assert_eq!(context.observed_at_unix_ms, 1_700_000_000_000);
    assert!(!format!("{context:?}").contains("secret-context-token"));
    assert_eq!(
        context.token_fingerprint(),
        WechatContext {
            account_key: "account-1".to_string(),
            user_id: "user-1".to_string(),
            context_token: "secret-context-token".to_string(),
            observed_at_unix_ms: 1_700_000_000_001,
            source_message_id: None,
        }
        .token_fingerprint()
    );
}

#[tokio::test]
async fn explicit_context_send_apis_reach_auth() {
    let client = WechatIlinkClient::new();
    let context = WechatContext {
        account_key: "account-1".to_string(),
        user_id: "user-1".to_string(),
        context_token: "ctx-1".to_string(),
        observed_at_unix_ms: 1,
        source_message_id: Some("msg-1".to_string()),
    };

    let send_result = client.send_text_with_context(&context, "hello").await;
    assert!(matches!(send_result, Err(WechatIlinkError::Auth(_))));

    let media_result = client
        .send_media_with_context(&context, SendContent::Text("hello".to_string()))
        .await;
    assert!(matches!(media_result, Err(WechatIlinkError::Auth(_))));

    let typing_result = client.send_typing_with_context(&context).await;
    assert!(matches!(typing_result, Err(WechatIlinkError::Auth(_))));
}

#[tokio::test]
async fn reply_uses_incoming_context_and_fails_when_missing() {
    let bot = WechatIlinkClient::new();

    let present_wire: WireMessage = serde_json::from_value(serde_json::json!({
        "from_user_id": "user-1",
        "to_user_id": "bot-1",
        "client_id": "client-1",
        "create_time_ms": 1700000000000i64,
        "message_type": 1,
        "message_state": 2,
        "context_token": "ctx-1",
        "item_list": [{ "type": 1, "msg_id": "item-1", "text_item": { "text": "hi" } }]
    }))
    .expect("wire");
    let present = IncomingMessage::from_wire(&present_wire).expect("incoming");
    assert!(present.context.is_some());

    let reply_result = bot.reply(&present, "hello").await;
    assert!(matches!(reply_result, Err(WechatIlinkError::Auth(_))));

    let reply_media_result = bot
        .reply_media(&present, SendContent::Text("hello".to_string()))
        .await;
    assert!(matches!(reply_media_result, Err(WechatIlinkError::Auth(_))));

    let missing_wire: WireMessage = serde_json::from_value(serde_json::json!({
        "from_user_id": "user-1",
        "to_user_id": "bot-1",
        "client_id": "client-1",
        "create_time_ms": 1700000000000i64,
        "message_type": 1,
        "message_state": 2,
        "context_token": "",
        "item_list": [{ "type": 1, "msg_id": "item-2", "text_item": { "text": "hi" } }]
    }))
    .expect("wire");
    let missing = IncomingMessage::from_wire(&missing_wire).expect("incoming");
    assert!(missing.context.is_none());

    let reply_result = bot.reply(&missing, "hello").await;
    assert!(matches!(reply_result, Err(WechatIlinkError::NoContext(_))));

    let reply_media_result = bot
        .reply_media(&missing, SendContent::Text("hello".to_string()))
        .await;
    assert!(matches!(
        reply_media_result,
        Err(WechatIlinkError::NoContext(_))
    ));
}
