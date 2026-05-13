use wechat_ilink::{
    protocol::{build_media_message_with_client_id, build_text_message_with_client_id},
    IncomingMessage, SendContent, SendReceipt, WechatIlinkClient, WechatIlinkError, WireMessage,
};

#[test]
fn incoming_message_preserves_wire_and_quoted_message_ids() {
    let raw = r#"{
        "seq": 42,
        "message_id": 9001,
        "from_user_id": "user-1",
        "to_user_id": "bot-1",
        "client_id": "client-inbound",
        "create_time_ms": 1700000000000,
        "message_type": 1,
        "message_state": 2,
        "context_token": "ctx-1",
        "item_list": [{
            "type": 1,
            "msg_id": "current-item",
            "text_item": { "text": "continue this" },
            "ref_msg": {
                "title": "agent notification",
                "message_item": {
                    "type": 1,
                    "msg_id": "quoted-notification",
                    "text_item": { "text": "agent output" }
                }
            }
        }]
    }"#;

    let wire: WireMessage = serde_json::from_str(raw).expect("wire message");
    assert_eq!(wire.seq, Some(42));
    assert_eq!(wire.message_id, Some(9001));
    assert_eq!(wire.item_list[0].msg_id.as_deref(), Some("current-item"));

    let incoming = IncomingMessage::from_wire(&wire).expect("incoming user message");
    assert_eq!(incoming.message_id.as_deref(), Some("current-item"));
    assert_eq!(incoming.wire_message_id, Some(9001));
    assert_eq!(incoming.client_id, "client-inbound");

    let quoted = incoming.quoted.as_ref().expect("quoted message");
    assert_eq!(quoted.message_id.as_deref(), Some("quoted-notification"));
    assert_eq!(quoted.title.as_deref(), Some("agent notification"));
    assert_eq!(quoted.text.as_deref(), Some("agent output"));
}

#[test]
fn incoming_message_uses_quoted_client_id_when_item_msg_id_is_missing() {
    let raw = r#"{
        "seq": 42,
        "message_id": 9001,
        "from_user_id": "user-1",
        "to_user_id": "bot-1",
        "client_id": "client-inbound",
        "create_time_ms": 1700000000000,
        "message_type": 1,
        "message_state": 2,
        "context_token": "ctx-1",
        "item_list": [{
            "type": 1,
            "msg_id": "current-item",
            "text_item": { "text": "continue this" },
            "ref_msg": {
                "title": "agent notification",
                "client_id": "quoted-client-id",
                "message_item": {
                    "type": 1,
                    "text_item": { "text": "agent output" }
                }
            }
        }]
    }"#;

    let wire: WireMessage = serde_json::from_str(raw).expect("wire message");
    let incoming = IncomingMessage::from_wire(&wire).expect("incoming user message");

    let quoted = incoming.quoted.as_ref().expect("quoted message");
    assert_eq!(quoted.message_id.as_deref(), Some("quoted-client-id"));
    assert_eq!(quoted.text.as_deref(), Some("agent output"));
}

#[test]
fn outbound_message_builders_accept_caller_supplied_client_id() {
    let text =
        build_text_message_with_client_id("user-1", "ctx-1", "agent output", "outbound-text-1");
    assert_eq!(text["client_id"], "outbound-text-1");
    assert_eq!(text["item_list"][0]["msg_id"], "outbound-text-1");
    assert_eq!(text["item_list"][0]["text_item"]["text"], "agent output");

    let media = build_media_message_with_client_id(
        "user-1",
        "ctx-1",
        vec![serde_json::json!({"type": 1, "text_item": {"text": "caption"}})],
        "outbound-media-1",
    );
    assert_eq!(media["client_id"], "outbound-media-1");
    assert_eq!(media["item_list"][0]["msg_id"], "outbound-media-1");
    assert_eq!(media["item_list"][0]["text_item"]["text"], "caption");
}

#[tokio::test]
async fn reply_and_explicit_send_return_send_receipts_directly() {
    let bot = WechatIlinkClient::new();

    let incoming = IncomingMessage::from_wire(
        &serde_json::from_value(serde_json::json!({
            "from_user_id": "user-1",
            "to_user_id": "bot-1",
            "client_id": "client-1",
            "create_time_ms": 1700000000000i64,
            "message_type": 1,
            "message_state": 2,
            "context_token": "ctx-1",
            "item_list": [{ "type": 1, "msg_id": "item-1", "text_item": { "text": "hi" } }]
        }))
        .expect("wire message"),
    )
    .expect("incoming");

    let send_result = bot
        .send_text_with_context(incoming.context.as_ref().unwrap(), "hello")
        .await;
    assert_send_receipt_result(&send_result);
    assert!(matches!(send_result, Err(WechatIlinkError::Auth(_))));

    let reply_result = bot.reply(&incoming, "hello").await;
    assert_send_receipt_result(&reply_result);
    assert!(matches!(reply_result, Err(WechatIlinkError::Auth(_))));
}

#[tokio::test]
async fn reply_media_and_explicit_send_media_return_send_receipts_directly() {
    let bot = WechatIlinkClient::new();

    let incoming = IncomingMessage::from_wire(
        &serde_json::from_value(serde_json::json!({
            "from_user_id": "user-1",
            "to_user_id": "bot-1",
            "client_id": "client-1",
            "create_time_ms": 1700000000000i64,
            "message_type": 1,
            "message_state": 2,
            "context_token": "ctx-1",
            "item_list": [{ "type": 1, "msg_id": "item-1", "text_item": { "text": "hi" } }]
        }))
        .expect("wire message"),
    )
    .expect("incoming");

    let send_result = bot
        .send_media_with_context(
            incoming.context.as_ref().unwrap(),
            SendContent::Text("hello".to_string()),
        )
        .await;
    assert_send_receipt_result(&send_result);
    assert!(matches!(send_result, Err(WechatIlinkError::Auth(_))));

    let reply_result = bot
        .reply_media(&incoming, SendContent::Text("hello".to_string()))
        .await;
    assert_send_receipt_result(&reply_result);
    assert!(matches!(reply_result, Err(WechatIlinkError::Auth(_))));
}

fn assert_send_receipt_result(_: &wechat_ilink::Result<SendReceipt>) {}
