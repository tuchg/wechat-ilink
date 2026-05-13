use wechat_ilink::{
    protocol::build_text_message_with_client_id, CDNMedia, GetConfigResponse, GetUpdatesResponse,
    GetUploadUrlParams, MessageItemType, MessageType, WireMessage,
};

#[test]
fn wire_types_accept_current_typescript_sdk_fields() {
    let raw = r#"{
        "seq": 7,
        "message_id": 8,
        "from_user_id": "user-1",
        "to_user_id": "bot-1",
        "client_id": "client-1",
        "create_time_ms": 1700000000000,
        "update_time_ms": 1700000001000,
        "delete_time_ms": 1700000002000,
        "session_id": "session-1",
        "group_id": "group-1",
        "message_type": 1,
        "message_state": 2,
        "context_token": "ctx-1",
        "item_list": [
            {
                "type": 2,
                "msg_id": "image-1",
                "image_item": {
                    "media": { "full_url": "https://cdn.example/image" },
                    "thumb_media": { "encrypt_query_param": "thumb-param", "aes_key": "thumb-key" },
                    "aeskey": "00112233445566778899aabbccddeeff",
                    "url": "https://example/image.jpg",
                    "mid_size": 11,
                    "thumb_size": 12,
                    "thumb_height": 13,
                    "thumb_width": 14,
                    "hd_size": 15
                }
            },
            {
                "type": 3,
                "msg_id": "voice-1",
                "voice_item": {
                    "media": { "encrypt_query_param": "voice-param", "aes_key": "voice-key" },
                    "encode_type": 6,
                    "bits_per_sample": 16,
                    "sample_rate": 24000,
                    "playtime": 1000,
                    "text": "voice text"
                }
            },
            {
                "type": 5,
                "msg_id": "video-1",
                "video_item": {
                    "media": { "encrypt_query_param": "video-param", "aes_key": "video-key" },
                    "video_size": 21,
                    "play_length": 22,
                    "video_md5": "video-md5",
                    "thumb_media": { "full_url": "https://cdn.example/video-thumb" },
                    "thumb_size": 23,
                    "thumb_height": 24,
                    "thumb_width": 25
                }
            }
        ]
    }"#;

    let wire: WireMessage = serde_json::from_str(raw).expect("wire message");
    let image = wire.item_list[0].image_item.as_ref().expect("image");
    assert_eq!(image.thumb_size, Some(12));
    assert_eq!(image.hd_size, Some(15));
    assert_eq!(
        image
            .media
            .as_ref()
            .and_then(|media| media.full_url.as_deref()),
        Some("https://cdn.example/image")
    );

    let voice = wire.item_list[1].voice_item.as_ref().expect("voice");
    assert_eq!(voice.bits_per_sample, Some(16));
    assert_eq!(voice.sample_rate, Some(24000));

    let video = wire.item_list[2].video_item.as_ref().expect("video");
    assert_eq!(video.video_md5.as_deref(), Some("video-md5"));
    assert_eq!(video.thumb_size, Some(23));
    assert_eq!(video.thumb_height, Some(24));
    assert_eq!(video.thumb_width, Some(25));
}

#[test]
fn wire_enums_accept_none_values_from_typescript_sdk() {
    assert_eq!(
        serde_json::from_value::<MessageType>(0.into()).unwrap(),
        MessageType::None
    );
    assert_eq!(
        serde_json::from_value::<MessageItemType>(0.into()).unwrap(),
        MessageItemType::None
    );
}

#[test]
fn response_types_preserve_current_typescript_sdk_fields() {
    let updates: GetUpdatesResponse = serde_json::from_value(serde_json::json!({
        "ret": 0,
        "msgs": [],
        "sync_buf": "legacy-sync",
        "get_updates_buf": "next-cursor",
        "longpolling_timeout_ms": 35000
    }))
    .expect("updates response");
    assert_eq!(updates.sync_buf.as_deref(), Some("legacy-sync"));
    assert_eq!(updates.longpolling_timeout_ms, Some(35000));

    let config: GetConfigResponse = serde_json::from_value(serde_json::json!({
        "ret": 0,
        "errmsg": "ok",
        "typing_ticket": "ticket-1"
    }))
    .expect("config response");
    assert_eq!(config.ret, Some(0));
    assert_eq!(config.errmsg.as_deref(), Some("ok"));
}

#[test]
fn upload_params_preserve_current_typescript_sdk_thumbnail_fields() {
    let params = GetUploadUrlParams {
        filekey: "file-key".to_string(),
        media_type: 1,
        to_user_id: "user-1".to_string(),
        rawsize: 100,
        rawfilemd5: "raw-md5".to_string(),
        filesize: 128,
        thumb_rawsize: Some(50),
        thumb_rawfilemd5: Some("thumb-md5".to_string()),
        thumb_filesize: Some(64),
        no_need_thumb: false,
        aeskey: "aes-key".to_string(),
    };

    assert_eq!(params.thumb_rawsize, Some(50));
    assert_eq!(params.thumb_rawfilemd5.as_deref(), Some("thumb-md5"));
    assert_eq!(params.thumb_filesize, Some(64));
}

#[test]
fn cdn_media_accepts_full_url_without_encrypted_params() {
    let media: CDNMedia =
        serde_json::from_value(serde_json::json!({ "full_url": "https://cdn.example/file" }))
            .expect("cdn media");
    assert_eq!(media.full_url.as_deref(), Some("https://cdn.example/file"));
    assert!(media.encrypt_query_param.is_none());
    assert!(media.aes_key.is_none());
}

#[test]
fn outbound_text_filters_unsupported_markdown_like_typescript_sdk() {
    let message = build_text_message_with_client_id(
        "user-1",
        "ctx-1",
        concat!(
            "##### Heading\n",
            "> quote\n",
            "plain `code` ~~strike~~ ***strong*** _em_\n",
            "![alt](https://example.test/image.png)\n",
            "| A | B |\n",
            "|---|---|\n",
            "| x | y |\n",
            "```rust\n",
            "fn main() {}\n",
            "```\n",
        ),
        "client-1",
    );

    assert_eq!(
        message["item_list"][0]["text_item"]["text"],
        "Heading\nquote\nplain code strike strong em\n\nA\tB\nx\ty\nfn main() {}\n"
    );
}
