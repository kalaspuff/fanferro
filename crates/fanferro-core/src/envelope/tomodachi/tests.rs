use super::*;
use std::sync::Arc;

use crate::envelope::{EnvelopeSet, RawEnvelope};
use crate::types::{AttributeValue, HeaderValue};

/// Built by tomodachi 0.29's `JsonBase.build_message()` for topic `order.created`.
const TOMODACHI_JSON: &str = r#"{"service": {"name": "legacy-orders", "uuid": "1b2c3d4e-0000-4000-8000-000000000001"}, "metadata": {"message_uuid": "1b2c3d4e-0000-4000-8000-000000000001.3090053a-24be-4b65-847c-91b951ff0074", "protocol_version": "tomodachi-json-base--1.0.0", "compatible_protocol_versions": ["json_base-wip"], "timestamp": 1793547225.2425017, "topic": "order.created", "data_encoding": "raw"}, "data": {"id": 7, "price": 1.1}}"#;

/// Built by tomodachi 0.29's `ProtobufBase.build_message()` for topic `order.created`, with a
/// `google.protobuf.StringValue` of `"hej"` as data.
const TOMODACHI_PROTOBUF: &str = "CjUKDWxlZ2FjeS1vcmRlcnMSJDFiMmMzZDRlLTAwMDAtNDAwMC04MDAwLTAwMDAwMDAwMDAwMRKKAQpJMWIyYzNkNGUtMDAwMC00MDAwLTgwMDAtMDAwMDAwMDAwMDAxLmYzZDQ5NWM3LTJhZWItNGZmYy1iYThmLTdhZDY2ZWZkYzk0NxIedG9tb2RhY2hpLXByb3RvYnVmLWJhc2UtLTEuMC4wGY6FT/bWudpBIg1vcmRlci5jcmVhdGVkKgVwcm90bxoFCgNoZWo=";

/// The serialized `google.protobuf.StringValue` of `"hej"`.
const STRING_VALUE_HEJ: &[u8] = b"\n\x03hej";

/// Sent by tomodachi 0.29's `send_message()` (its default `MessageBodyFormatter`) with `JsonBase`.
const TOMODACHI_QUEUE_MESSAGE: &str = r#"{"Type": "Message", "QueueUrl": "http://localhost:4566/000000000000/q", "Version": "0", "Message": "{\"service\": {\"name\": \"legacy-orders\", \"uuid\": \"1b2c3d4e-0000-4000-8000-000000000001\"}, \"metadata\": {\"message_uuid\": \"1b2c3d4e-0000-4000-8000-000000000001.4f2f1503-8fae-4be1-900c-f968b51e4c90\", \"protocol_version\": \"tomodachi-json-base--1.0.0\", \"compatible_protocol_versions\": [\"json_base-wip\"], \"timestamp\": 1793547225.2425613, \"topic\": \"\", \"data_encoding\": \"raw\"}, \"data\": {\"id\": 8}}", "Timestamp": "2026-11-01T15:33:45.242Z", "MessageAttributes": {"tenant": {"Type": "String", "Value": "acme"}}}"#;

fn wire(body: impl Into<Bytes>) -> WireMessage {
    WireMessage::new(body, MessageAttributes::new())
}

fn json_payload(data: &'static [u8]) -> Payload {
    Payload {
        data: Bytes::from_static(data),
        content_type: Some("application/json".into()),
        message_type: Some("orders.created.v1".into()),
        id: Some("0193-id".into()),
        source: Some("billing".into()),
        ..Payload::default()
    }
}

fn proto_payload(data: &'static [u8]) -> Payload {
    Payload {
        data: Bytes::from_static(data),
        content_type: Some("application/x-protobuf".into()),
        id: Some("0193-id".into()),
        ..Payload::default()
    }
}

fn json_body(wire: &WireMessage) -> serde_json::Value {
    serde_json::from_slice(&wire.body).unwrap()
}

#[test]
fn reads_tomodachi_json_messages() {
    let wire = wire(TOMODACHI_JSON);
    let payload = TomodachiJson::default().unwrap(&wire).unwrap().unwrap();
    assert_eq!(&payload.data[..], br#"{"id": 7, "price": 1.1}"#);
    assert_eq!(payload.content_type.as_deref(), Some("application/json"));
    assert_eq!(payload.message_type.as_deref(), Some("order.created"));
    // The message uuid without the service uuid: tomodachi's uuid4.
    assert_eq!(payload.id.as_deref(), Some("3090053a-24be-4b65-847c-91b951ff0074"));
    assert_eq!(payload.source.as_deref(), Some("legacy-orders"));
    let time = payload.time.unwrap();
    assert_eq!(time.unix_timestamp(), 1_793_547_225);
    assert_eq!(time.millisecond(), 242);
    // The data is shared with the body, not copied.
    let start = payload.data.as_ptr() as usize - wire.body.as_ptr() as usize;
    assert!(start < wire.body.len());
}

#[test]
fn reads_tomodachi_protobuf_messages() {
    let payload = TomodachiProtobuf::default().unwrap(&wire(TOMODACHI_PROTOBUF)).unwrap().unwrap();
    assert_eq!(&payload.data[..], STRING_VALUE_HEJ);
    assert_eq!(payload.content_type.as_deref(), Some("application/x-protobuf"));
    assert_eq!(payload.message_type.as_deref(), Some("order.created"));
    assert_eq!(payload.id.as_deref(), Some("f3d495c7-2aeb-4ffc-ba8f-7ad66efdc947"));
    assert_eq!(payload.source.as_deref(), Some("legacy-orders"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 1_793_547_225);
}

#[test]
fn reads_messages_tomodachi_sent_to_a_queue() {
    let payload = TomodachiJson::default().unwrap(&wire(TOMODACHI_QUEUE_MESSAGE)).unwrap().unwrap();
    assert_eq!(&payload.data[..], br#"{"id": 8}"#);
    assert_eq!(payload.id.as_deref(), Some("4f2f1503-8fae-4be1-900c-f968b51e4c90"));
    // Sent to a queue: no topic, so no type.
    assert_eq!(payload.message_type, None);

    let envelope = TomodachiProtobuf::default();
    let document = serde_json::json!({"Type": "Message", "Version": "0", "Message": TOMODACHI_PROTOBUF});
    let payload = envelope.unwrap(&wire(document.to_string())).unwrap().unwrap();
    assert_eq!(&payload.data[..], STRING_VALUE_HEJ);
}

#[test]
fn the_type_attribute_takes_precedence_over_the_topic() {
    let mut wire = wire(TOMODACHI_JSON);
    wire.attributes.insert("fanferro.type".into(), AttributeValue::string("orders.created.v1"));
    let payload = TomodachiJson::default().unwrap(&wire).unwrap().unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("orders.created.v1"));
    let envelope = TomodachiJson { type_attribute: None, ..TomodachiJson::default() };
    assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().message_type.as_deref(), Some("order.created"));
}

#[test]
fn other_formats_are_left_to_the_next_envelope() {
    let json = TomodachiJson::default();
    let protobuf = TomodachiProtobuf::default();
    for body in [
        &b""[..],
        b"hello",
        b"[1, 2]",
        br#"{"id": 1}"#,
        br#"{"metadata": {"protocol_version": "other"}, "data": 1}"#,
        br#"{"metadata": "x", "Type": "Message"}"#,
        br#"{"Type": "Notification", "Message": "x"}"#,
        br#"{"Type": "Message", "Message": "{\"id\": 1}"}"#,
        br#"{"specversion": "1.0", "id": "1", "source": "s", "type": "t"}"#,
        b"CgNoZWo=",
        b"Cg==",
    ] {
        assert_eq!(json.unwrap(&wire(body)).unwrap(), None, "{}", String::from_utf8_lossy(body));
        assert_eq!(protobuf.unwrap(&wire(body)).unwrap(), None, "{}", String::from_utf8_lossy(body));
    }
    assert_eq!(json.unwrap(&wire(TOMODACHI_PROTOBUF)).unwrap(), None);
    assert_eq!(protobuf.unwrap(&wire(TOMODACHI_JSON)).unwrap(), None);
    assert_eq!(protobuf.unwrap(&wire(TOMODACHI_QUEUE_MESSAGE)).unwrap(), None);
}

#[test]
fn malformed_tomodachi_messages_are_errors() {
    let json = TomodachiJson::default();
    for body in [
        r#"{"metadata": {"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "zstd"}, "data": 1}"#,
        r#"{"metadata": {"protocol_version": "tomodachi-json-base--1.0.0"}, "data": 1}"#,
        r#"{"metadata": {"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "base64_gzip_json"}, "data": 1}"#,
        r#"{"metadata": {"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "base64_gzip_json"}, "data": "!!"}"#,
        r#"{"metadata": {"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "base64_gzip_json"}, "data": "AAAA"}"#,
    ] {
        assert!(json.unwrap(&wire(body)).is_err(), "{body}");
    }
    let message = SnsSqsMessage {
        service: None,
        metadata: Some(MetadataProto {
            protocol_version: TOMODACHI_PROTOBUF_PROTOCOL_VERSION.into(),
            data_encoding: "lz4".into(),
            ..MetadataProto::default()
        }),
        data: Bytes::from_static(b"x"),
    };
    let body = BASE64.encode(message.encode_to_vec());
    assert!(TomodachiProtobuf::default().unwrap(&wire(body)).is_err());
}

#[test]
fn reads_every_tomodachi_data_encoding() {
    let json = TomodachiJson::default();
    let compressed = BASE64.encode(zlib_compress(br#"{"big": true}"#));
    let body = format!(
        r#"{{"metadata": {{"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "base64_gzip_json"}}, "data": "{compressed}"}}"#
    );
    assert_eq!(&json.unwrap(&wire(body)).unwrap().unwrap().data[..], br#"{"big": true}"#);
    // Old tomodachi versions; a missing data field is null.
    let body = r#"{"metadata": {"protocol_version": "json_base-wip", "data_encoding": "raw"}}"#;
    let payload = json.unwrap(&wire(body)).unwrap().unwrap();
    assert_eq!(&payload.data[..], b"null");
    assert_eq!((payload.id, payload.time, payload.source), (None, None, None));

    let protobuf = TomodachiProtobuf::default();
    let cases: [(&str, Bytes, &[u8], &str); 5] = [
        ("proto", Bytes::from_static(STRING_VALUE_HEJ), STRING_VALUE_HEJ, "application/x-protobuf"),
        ("gzip_proto", zlib_compress(STRING_VALUE_HEJ).into(), STRING_VALUE_HEJ, "application/x-protobuf"),
        ("base64", BASE64.encode(STRING_VALUE_HEJ).into(), STRING_VALUE_HEJ, "application/x-protobuf"),
        (
            "base64_gzip_proto",
            BASE64.encode(zlib_compress(STRING_VALUE_HEJ)).into(),
            STRING_VALUE_HEJ,
            "application/x-protobuf",
        ),
        ("raw", Bytes::from_static(b"\x00\x01"), b"\x00\x01", "application/octet-stream"),
    ];
    for (encoding, data, expected, content_type) in cases {
        let message = SnsSqsMessage {
            service: Some(ServiceProto { name: "s".into(), uuid: "u".into() }),
            metadata: Some(MetadataProto {
                message_uuid: "u.1".into(),
                protocol_version: TOMODACHI_PROTOBUF_PROTOCOL_VERSION.into(),
                data_encoding: encoding.into(),
                ..MetadataProto::default()
            }),
            data,
        };
        let payload = protobuf.unwrap(&wire(BASE64.encode(message.encode_to_vec()))).unwrap().unwrap();
        assert_eq!(&payload.data[..], expected, "{encoding}");
        assert_eq!(payload.content_type.as_deref(), Some(content_type), "{encoding}");
        assert_eq!(payload.id.as_deref(), Some("1"));
    }
}

#[test]
fn decompression_is_bounded() {
    let bomb = BASE64.encode(zlib_compress(&vec![b' '; MAX_DECOMPRESSED_SIZE + 1]));
    let body = format!(
        r#"{{"metadata": {{"protocol_version": "tomodachi-json-base--1.0.0", "data_encoding": "base64_gzip_json"}}, "data": "{bomb}"}}"#
    );
    let error = TomodachiJson::default().unwrap(&wire(body)).unwrap_err();
    assert!(error.to_string().contains("expands to more than"), "{error}");
}

#[test]
fn writes_json_messages_tomodachi_reads() {
    let envelope = TomodachiJson { service_uuid: Some("svc-uuid".into()), ..TomodachiJson::default() };
    let wire = envelope
        .wrap_for(
            &json_payload(br#"{"id": 7, "n": 12345678901234567890}"#),
            TomodachiDestination::Topic("order___2e_created"),
        )
        .unwrap();
    let body = std::str::from_utf8(&wire.body).unwrap();
    // The data is embedded verbatim (big numbers and spacing kept).
    assert!(body.ends_with(r#""data":{"id": 7, "n": 12345678901234567890}}"#), "{body}");
    let document = json_body(&wire);
    assert_eq!(document["service"], serde_json::json!({"name": "billing", "uuid": "svc-uuid"}));
    let metadata = &document["metadata"];
    assert_eq!(metadata["message_uuid"], "svc-uuid.0193-id");
    assert_eq!(metadata["protocol_version"], TOMODACHI_JSON_PROTOCOL_VERSION);
    assert_eq!(metadata["compatible_protocol_versions"], serde_json::json!(["json_base-wip"]));
    assert_eq!(metadata["topic"], "order.created");
    assert_eq!(metadata["data_encoding"], "raw");
    assert!(metadata["timestamp"].as_f64().unwrap() > 1.7e9);
    assert_eq!(wire.attributes["fanferro.type"].as_text(), Some("orders.created.v1"));

    // fanferro reads back what it wrote, with the id it was published with.
    let payload = envelope.unwrap(&wire).unwrap().unwrap();
    assert_eq!(payload.id.as_deref(), Some("0193-id"));
    assert_eq!(payload.message_type.as_deref(), Some("orders.created.v1"));
    assert_eq!(payload.source.as_deref(), Some("billing"));
}

#[test]
fn writes_queue_messages_in_tomodachis_queue_document() {
    let envelope = TomodachiJson::default();
    let wire = envelope.wrap_for(&json_payload(b"[1]"), TomodachiDestination::Queue).unwrap();
    let document = json_body(&wire);
    assert_eq!(document["Type"], "Message");
    assert_eq!(document["Version"], "0");
    assert!(document["Timestamp"].as_str().unwrap().ends_with('Z'));
    assert_eq!(document["MessageAttributes"], serde_json::json!({}));
    let inner: serde_json::Value = serde_json::from_str(document["Message"].as_str().unwrap()).unwrap();
    assert_eq!(inner["metadata"]["topic"], "");
    assert_eq!(inner["data"], serde_json::json!([1]));
    assert_eq!(&envelope.unwrap(&wire).unwrap().unwrap().data[..], b"[1]");

    let envelope = TomodachiProtobuf::default();
    let wire = envelope.wrap_for(&proto_payload(STRING_VALUE_HEJ), TomodachiDestination::Queue).unwrap();
    let document = json_body(&wire);
    assert_eq!(document["Type"], "Message");
    let inner = BASE64.decode(document["Message"].as_str().unwrap()).unwrap();
    let message = SnsSqsMessage::decode(&inner[..]).unwrap();
    assert_eq!(&message.data[..], STRING_VALUE_HEJ);
    assert_eq!(&envelope.unwrap(&wire).unwrap().unwrap().data[..], STRING_VALUE_HEJ);
}

#[test]
fn writes_protobuf_messages_tomodachi_reads() {
    let envelope = TomodachiProtobuf { service_name: Some("billing".into()), ..TomodachiProtobuf::default() };
    let wire = envelope.wrap_for(&proto_payload(STRING_VALUE_HEJ), TomodachiDestination::Topic("orders.fifo")).unwrap();
    assert!(wire.body.is_ascii());
    let message = SnsSqsMessage::decode(Bytes::from(BASE64.decode(&wire.body).unwrap())).unwrap();
    let service = message.service.unwrap();
    assert_eq!(service.name, "billing");
    assert_eq!(service.uuid, process_service_uuid());
    let metadata = message.metadata.unwrap();
    assert_eq!(metadata.message_uuid, format!("{}.0193-id", process_service_uuid()));
    assert_eq!(metadata.protocol_version, TOMODACHI_PROTOBUF_PROTOCOL_VERSION);
    // tomodachi adds the suffix of FIFO topics itself.
    assert_eq!(metadata.topic, "orders");
    assert_eq!(metadata.data_encoding, "proto");
    assert_eq!(&message.data[..], STRING_VALUE_HEJ);
    assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().id.as_deref(), Some("0193-id"));
}

#[test]
fn compresses_large_data_like_tomodachi() {
    let large: &'static [u8] =
        Box::leak(format!("\"{}\"", "x".repeat(TOMODACHI_COMPRESS_THRESHOLD)).into_bytes().into_boxed_slice());
    let envelope = TomodachiJson::default();
    let wire = envelope.wrap(&json_payload(large)).unwrap();
    assert!(wire.body.len() < 2000);
    assert_eq!(json_body(&wire)["metadata"]["data_encoding"], "base64_gzip_json");
    assert_eq!(&envelope.unwrap(&wire).unwrap().unwrap().data[..], large);
    let uncompressed = TomodachiJson { compress_threshold: None, ..TomodachiJson::default() };
    assert_eq!(json_body(&uncompressed.wrap(&json_payload(large)).unwrap())["metadata"]["data_encoding"], "raw");

    let envelope = TomodachiProtobuf::default();
    let wire = envelope.wrap(&proto_payload(&large[..TOMODACHI_COMPRESS_THRESHOLD + 1])).unwrap();
    let message = SnsSqsMessage::decode(Bytes::from(BASE64.decode(&wire.body).unwrap())).unwrap();
    assert_eq!(message.metadata.unwrap().data_encoding, "gzip_proto");
    assert_eq!(&envelope.unwrap(&wire).unwrap().unwrap().data[..], &large[..TOMODACHI_COMPRESS_THRESHOLD + 1]);
    // At the threshold, protobuf data is not compressed yet (tomodachi compresses above it).
    let wire = envelope.wrap(&proto_payload(&large[..TOMODACHI_COMPRESS_THRESHOLD])).unwrap();
    let message = SnsSqsMessage::decode(Bytes::from(BASE64.decode(&wire.body).unwrap())).unwrap();
    assert_eq!(message.metadata.unwrap().data_encoding, "proto");
}

#[test]
fn text_payloads_are_json_strings_and_binary_ones_are_refused() {
    let envelope = TomodachiJson::default();
    let text = Payload {
        data: Bytes::from_static(b"hi \"there\""),
        content_type: Some("text/plain".into()),
        ..Payload::default()
    };
    let wire = envelope.wrap(&text).unwrap();
    assert_eq!(json_body(&wire)["data"], "hi \"there\"");
    assert_eq!(&envelope.unwrap(&wire).unwrap().unwrap().data[..], br#""hi \"there\"""#);

    let binary = proto_payload(STRING_VALUE_HEJ);
    let error = envelope.wrap(&binary).unwrap_err();
    assert!(error.to_string().contains("tomodachi protobuf envelope"), "{error}");
    let invalid = json_payload(b"{");
    assert!(envelope.wrap(&invalid).is_err());

    let error = TomodachiProtobuf::default().wrap(&json_payload(b"{}")).unwrap_err();
    assert!(error.to_string().contains("tomodachi JSON envelope"), "{error}");
}

#[test]
fn headers_and_the_type_travel_as_attributes() {
    let mut payload = json_payload(b"1");
    payload.headers.insert("traceparent".into(), HeaderValue::String("00-abc-def-01".into()));
    payload.headers.insert("version".into(), HeaderValue::Integer(2));
    let wire = TomodachiJson::default().wrap(&payload).unwrap();
    assert_eq!(wire.attributes["traceparent"].as_text(), Some("00-abc-def-01"));
    assert_eq!(wire.attributes["version"].data_type(), "Number");
    assert_eq!(wire.attributes["fanferro.type"].as_text(), Some("orders.created.v1"));
    let without = TomodachiProtobuf { type_attribute: None, ..TomodachiProtobuf::default() };
    assert!(without.wrap(&proto_payload(b"")).unwrap().attributes.is_empty());
}

#[test]
fn messages_without_an_id_get_a_new_one() {
    let payload = Payload { id: None, ..json_payload(b"1") };
    let envelope = TomodachiJson::default();
    let first = envelope.unwrap(&envelope.wrap(&payload).unwrap()).unwrap().unwrap().id.unwrap();
    let second = envelope.unwrap(&envelope.wrap(&payload).unwrap()).unwrap().unwrap().id.unwrap();
    assert_ne!(first, second);
    assert_eq!(first.len(), 36);
}

#[test]
fn decodes_tomodachi_topic_names() {
    for (encoded, decoded) in [
        ("orders", "orders"),
        ("order___2e_created", "order.created"),
        ("order___2e_created___2f_v1", "order.created/v1"),
        ("___c3a5____c3a4____c3b6____2e_x", "åäö.x"),
        ("a____2e_b", "a_.b"),
        ("a___zz_b", "a___zz_b"),
        ("a___", "a___"),
        ("___2e", "___2e"),
    ] {
        assert_eq!(decode_tomodachi_topic(encoded), decoded, "{encoded}");
    }
    assert!(matches!(decode_tomodachi_topic("plain"), Cow::Borrowed(_)));
}

#[test]
fn consumers_detect_tomodachi_messages_among_other_formats() {
    let set = EnvelopeSet::new(vec![
        Arc::new(TomodachiJson::default()),
        Arc::new(TomodachiProtobuf::default()),
        Arc::new(RawEnvelope::default()),
    ]);
    assert_eq!(set.unwrap(&wire(TOMODACHI_JSON)).unwrap().1, "tomodachi-json");
    assert_eq!(set.unwrap(&wire(TOMODACHI_PROTOBUF)).unwrap().1, "tomodachi-protobuf");
    assert_eq!(set.unwrap(&wire(TOMODACHI_QUEUE_MESSAGE)).unwrap().1, "tomodachi-json");
    assert_eq!(set.unwrap(&wire(r#"{"id": 1}"#)).unwrap().1, "raw");
}
