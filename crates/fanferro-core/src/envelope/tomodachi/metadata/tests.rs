use super::*;
use std::sync::Arc;

use crate::envelope::{EnvelopeSet, RawEnvelope, TomodachiJson, TomodachiProtobuf};
use crate::types::AttributeValue;

/// Built with Python's protobuf for topic `order-created` like the envelope's reference
/// implementation does, with a `google.protobuf.StringValue` of `"hej"` as data.
const PROTO_MESSAGE: &str = "CgUKA2hlahLUAQokMzA5MDA1M2EtMjRiZS00YjY1LTg0N2MtOTFiOTUxZmYwMDc0EhdleGFtcGxlLWVudmVsb3BlLS0xLjAuMBoNb3JkZXItY3JlYXRlZCINbGVnYWN5LW9yZGVycyobMjAyNi0xMS0wMVQxNTozMzo0NS4yNDI1MDJaMgVwcm90b0IkYzBmZmVlMDAtMDAwMC00MDAwLTgwMDAtMDAwMDAwMDAwMDAxSh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG9SC1N0cmluZ1ZhbHVl";

/// The same with JSON data, `{"id": 7, "price": 1.1}`.
const JSON_MESSAGE: &str = "Chd7ImlkIjogNywgInByaWNlIjogMS4xfRKmAQokNGYyZjE1MDMtOGZhZS00YmUxLTkwMGMtZjk2OGI1MWU0YzkwEhdleGFtcGxlLWVudmVsb3BlLS0xLjAuMBoNb3JkZXItY3JlYXRlZCINbGVnYWN5LW9yZGVycyobMjAyNi0xMS0wMVQxNTozMzo0NS4yNDI1MDJaMgRqc29uQiRjMGZmZWUwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDI=";

/// The same with string data, `hello`, without a topic and a correlation id.
const STRING_MESSAGE: &str = "CgVoZWxsbxJzCiQ1ZTZmN2E4Yi0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDQSF2V4YW1wbGUtZW52ZWxvcGUtLTEuMC4wIg1sZWdhY3ktb3JkZXJzKhsyMDI2LTExLTAxVDE1OjMzOjQ1LjI0MjUwMloyBnN0cmluZw==";

/// The serialized `google.protobuf.StringValue` of `"hej"`.
const STRING_VALUE_HEJ: &[u8] = b"\n\x03hej";

fn envelope() -> MetadataProtobuf {
    MetadataProtobuf { version: Some("1.0.0".into()), ..MetadataProtobuf::new("example-envelope") }
}

fn wire(body: impl Into<Bytes>) -> WireMessage {
    WireMessage::new(body, MessageAttributes::new())
}

fn decode_body(body: &[u8]) -> MessageProto {
    MessageProto::decode(Bytes::from(BASE64.decode(body).unwrap())).unwrap()
}

fn header(payload: &Payload, name: &str) -> Option<String> {
    payload.headers.get(name).map(HeaderValue::to_text)
}

fn payload(data: &'static [u8], content_type: &str) -> Payload {
    Payload {
        data: Bytes::from_static(data),
        content_type: Some(content_type.into()),
        message_type: Some("order-created".into()),
        id: Some("0193-id".into()),
        source: Some("billing".into()),
        ..Payload::default()
    }
}

#[test]
fn reads_protobuf_messages() {
    let payload = envelope().unwrap(&wire(PROTO_MESSAGE)).unwrap().unwrap();
    assert_eq!(&payload.data[..], STRING_VALUE_HEJ);
    assert_eq!(payload.content_type.as_deref(), Some("application/x-protobuf"));
    assert_eq!(payload.message_type.as_deref(), Some("order-created"));
    assert_eq!(payload.id.as_deref(), Some("3090053a-24be-4b65-847c-91b951ff0074"));
    assert_eq!(payload.source.as_deref(), Some("legacy-orders"));
    assert_eq!(payload.data_schema.as_deref(), Some("google/protobuf/wrappers.proto"));
    assert_eq!(header(&payload, CORRELATION_ID_HEADER).as_deref(), Some("c0ffee00-0000-4000-8000-000000000001"));
    assert_eq!(header(&payload, DATA_CLASS_HEADER).as_deref(), Some("StringValue"));
    let time = payload.time.unwrap();
    assert_eq!(time.unix_timestamp(), 1_793_547_225);
    assert_eq!(time.microsecond(), 242_502);
}

#[test]
fn reads_json_and_string_messages() {
    let json = envelope().unwrap(&wire(JSON_MESSAGE)).unwrap().unwrap();
    assert_eq!(&json.data[..], br#"{"id": 7, "price": 1.1}"#);
    assert_eq!(json.content_type.as_deref(), Some("application/json"));
    assert_eq!(json.data_schema, None);
    assert_eq!(header(&json, DATA_CLASS_HEADER), None);

    let text = envelope().unwrap(&wire(STRING_MESSAGE)).unwrap().unwrap();
    assert_eq!(&text.data[..], b"hello");
    assert_eq!(text.content_type.as_deref(), Some("text/plain; charset=utf-8"));
    assert_eq!(text.message_type, None, "a message without a topic has no type");
}

#[test]
fn recognises_messages_by_the_envelope_name() {
    // The name is matched as a prefix of metadata.envelope, whatever the version.
    let unversioned = MetadataProtobuf::new("example-envelope");
    assert!(unversioned.unwrap(&wire(JSON_MESSAGE)).unwrap().is_some());
    let other = MetadataProtobuf::new("other-envelope");
    assert!(other.unwrap(&wire(JSON_MESSAGE)).unwrap().is_none());
}

#[test]
fn leaves_other_bodies_to_the_next_envelope() {
    for body in
        ["", "hello", "{\"id\": 1}", "Q2xvdWQ=", "CgVoZWxsbw==", "C!!!", r#"{"Type": "Notification", "Message": "x"}"#]
    {
        assert!(envelope().unwrap(&wire(body)).unwrap().is_none(), "{body:?}");
    }
}

#[test]
fn derives_missing_correlation_ids_from_the_message_uuid() {
    let first = envelope().unwrap(&wire(STRING_MESSAGE)).unwrap().unwrap();
    let again = envelope().unwrap(&wire(STRING_MESSAGE)).unwrap().unwrap();
    let id = header(&first, CORRELATION_ID_HEADER).unwrap();
    assert_eq!(header(&again, CORRELATION_ID_HEADER).as_deref(), Some(id.as_str()), "every delivery gets the same id");
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 5);
    assert_ne!(Some(id), first.id);

    let keep_missing = MetadataProtobuf { generate_missing_correlation_id: false, ..envelope() };
    let payload = keep_missing.unwrap(&wire(STRING_MESSAGE)).unwrap().unwrap();
    assert_eq!(header(&payload, CORRELATION_ID_HEADER), None);
}

#[test]
fn writes_the_metadata() {
    let mut outgoing = payload(STRING_VALUE_HEJ, "application/x-protobuf");
    outgoing.time = Some(time::macros::datetime!(2026-11-01 15:33:45.242502 UTC));
    outgoing.data_schema = Some("google/protobuf/wrappers.proto".into());
    outgoing.headers.insert(CORRELATION_ID_HEADER.into(), HeaderValue::String("corr-1".into()));
    outgoing.headers.insert(DATA_CLASS_HEADER.into(), HeaderValue::String("StringValue".into()));
    outgoing.headers.insert("tenant".into(), HeaderValue::String("acme".into()));
    let wire = envelope().wrap_for(&outgoing, TomodachiDestination::Topic("order-created")).unwrap();

    let message = decode_body(&wire.body);
    let metadata = message.metadata.unwrap();
    assert_eq!(&message.data[..], STRING_VALUE_HEJ);
    assert_eq!(metadata.uuid, "0193-id");
    assert_eq!(metadata.envelope, "example-envelope--1.0.0");
    assert_eq!(metadata.topic, "order-created");
    assert_eq!(metadata.origin, "billing");
    assert_eq!(metadata.timestamp, "2026-11-01T15:33:45.242502Z");
    assert_eq!(metadata.encoding, "proto");
    assert_eq!(metadata.external_path, "");
    assert_eq!(metadata.correlation_id, "corr-1");
    assert_eq!(metadata.data_definition, "google/protobuf/wrappers.proto");
    assert_eq!(metadata.data_class, "StringValue");
    // Headers without a metadata field travel as attributes, next to the promoted type.
    let mut names: Vec<_> = wire.attributes.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["fanferro.type", "tenant"]);
    assert_eq!(wire.attributes.get("tenant"), Some(&AttributeValue::string("acme")));
}

#[test]
fn writes_defaults_for_missing_fields() {
    let outgoing = Payload {
        data: Bytes::from_static(b"{}"),
        content_type: Some("application/json".into()),
        ..Payload::default()
    };
    let configured =
        MetadataProtobuf { service_name: Some("orders".into()), ..MetadataProtobuf::new("example-envelope") };
    let metadata = decode_body(&configured.wrap(&outgoing).unwrap().body).metadata.unwrap();
    assert_eq!(metadata.envelope, "example-envelope", "without a version the name alone");
    assert_eq!(metadata.origin, "orders");
    assert_eq!(metadata.topic, "");
    assert_eq!(uuid::Uuid::parse_str(&metadata.uuid).unwrap().get_version_num(), 7);
    assert_eq!(uuid::Uuid::parse_str(&metadata.correlation_id).unwrap().get_version_num(), 4);
    assert!(OffsetDateTime::parse(&metadata.timestamp, &Rfc3339).is_ok());
    assert_eq!(metadata.data_class, "");
}

#[test]
fn maps_content_types_to_encodings_and_back() {
    let cases: [(&str, &str, &str); 6] = [
        ("application/x-protobuf", "proto", "application/x-protobuf"),
        ("application/protobuf", "proto", "application/x-protobuf"),
        ("application/json", "json", "application/json"),
        ("application/cloudevents+json; charset=utf-8", "json", "application/json"),
        ("text/plain; charset=utf-8", "string", "text/plain; charset=utf-8"),
        ("application/octet-stream", "raw", "application/octet-stream"),
    ];
    for (content_type, encoding, read_back) in cases {
        let wire = envelope().wrap(&payload(b"{}", content_type)).unwrap();
        assert_eq!(decode_body(&wire.body).metadata.unwrap().encoding, encoding, "{content_type}");
        let read = envelope().unwrap(&wire).unwrap().unwrap();
        assert_eq!(read.content_type.as_deref(), Some(read_back), "{content_type}");
        assert_eq!(&read.data[..], b"{}");
    }
    let untyped = Payload { data: Bytes::from_static(b"\x00\x01"), ..Payload::default() };
    let wire = envelope().wrap(&untyped).unwrap();
    assert_eq!(decode_body(&wire.body).metadata.unwrap().encoding, "raw");
}

#[test]
fn refuses_content_types_without_an_encoding() {
    let err = envelope().wrap(&payload(b"\x81\xa1a", "application/msgpack")).unwrap_err();
    assert!(matches!(&err, Error::Envelope(m) if m.contains("application/msgpack")), "{err}");
}

#[test]
fn compresses_large_data_like_the_reference() {
    let data: &'static [u8] = Box::leak(vec![b'x'; 60_001].into_boxed_slice());
    let wire = envelope().wrap(&payload(data, "application/json")).unwrap();
    let message = decode_body(&wire.body);
    assert_eq!(message.metadata.as_ref().unwrap().encoding, "gzip_json");
    assert!(message.data.len() < 1_000);
    // zlib format, as Python's zlib.compress() writes and zlib.decompress() reads.
    assert_eq!(message.data[0], 0x78);
    let read = envelope().unwrap(&wire).unwrap().unwrap();
    assert_eq!(read.data.len(), 60_001);
    assert_eq!(read.content_type.as_deref(), Some("application/json"));

    // At the threshold, or with compression off, the data is written as it is.
    let at_threshold: &'static [u8] = &data[..60_000];
    let wire = envelope().wrap(&payload(at_threshold, "application/json")).unwrap();
    assert_eq!(decode_body(&wire.body).metadata.unwrap().encoding, "json");
    let never = MetadataProtobuf { compress_threshold: None, ..envelope() };
    let wire = never.wrap(&payload(data, "application/json")).unwrap();
    assert_eq!(decode_body(&wire.body).metadata.unwrap().encoding, "json");
}

#[test]
fn round_trips_through_tomodachi_queue_documents() {
    let wire = envelope().wrap_for(&payload(b"{\"id\": 1}", "application/json"), TomodachiDestination::Queue).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&wire.body).unwrap();
    assert_eq!(document["Type"], "Message");
    let inner = document["Message"].as_str().unwrap();
    assert_eq!(decode_body(inner.as_bytes()).metadata.unwrap().topic, "");
    let read = envelope().unwrap(&wire).unwrap().unwrap();
    assert_eq!(&read.data[..], b"{\"id\": 1}");
    assert_eq!(read.message_type.as_deref(), Some("order-created"), "from the type attribute");
}

#[test]
fn writes_topics_as_tomodachi_names_them() {
    let wire = envelope()
        .wrap_for(&payload(b"{}", "application/json"), TomodachiDestination::Topic("order___2e_created.fifo"))
        .unwrap();
    assert_eq!(decode_body(&wire.body).metadata.unwrap().topic, "order.created");
}

#[test]
fn reports_unreadable_messages_of_its_format() {
    let with = |encoding: &str, data: &'static [u8]| {
        let message = MessageProto {
            data: Bytes::from_static(data),
            metadata: Some(MetadataProto {
                uuid: "id-1".into(),
                envelope: "example-envelope--1.0.0".into(),
                encoding: encoding.into(),
                ..MetadataProto::default()
            }),
        };
        wire(BASE64.encode(message.encode_to_vec()))
    };
    let err = envelope().unwrap(&with("oob_gzip_proto", b"bucket/key")).unwrap_err();
    assert!(matches!(&err, Error::Envelope(m) if m.contains("out of band") && m.contains("id-1")), "{err}");
    let err = envelope().unwrap(&with("avro", b"x")).unwrap_err();
    assert!(matches!(&err, Error::Envelope(m) if m.contains("avro")), "{err}");
    let err = envelope().unwrap(&with("gzip_json", b"not zlib")).unwrap_err();
    assert!(matches!(&err, Error::Envelope(m) if m.contains("compressed")), "{err}");
}

#[test]
fn tomodachi_envelopes_and_this_one_tell_their_messages_apart() {
    let set = EnvelopeSet::new(vec![
        Arc::new(TomodachiJson::default()),
        Arc::new(TomodachiProtobuf::default()),
        Arc::new(envelope()),
        Arc::new(RawEnvelope::default()),
    ]);
    for body in [PROTO_MESSAGE, JSON_MESSAGE, STRING_MESSAGE] {
        assert_eq!(set.unwrap_indexed(&wire(body)).unwrap().unwrap().1, 2);
    }
    let tomodachi = TomodachiProtobuf::default().wrap(&payload(STRING_VALUE_HEJ, "application/x-protobuf")).unwrap();
    assert!(envelope().unwrap(&tomodachi).unwrap().is_none());
    let set = EnvelopeSet::new(vec![Arc::new(envelope()), Arc::new(TomodachiProtobuf::default())]);
    assert_eq!(set.unwrap_indexed(&tomodachi).unwrap().unwrap().1, 1);
}

#[test]
fn validates_its_settings() {
    assert!(envelope().validate().is_ok());
    assert!(matches!(MetadataProtobuf::new(" ").validate(), Err(Error::Config(_))));
    let empty_version = MetadataProtobuf { version: Some(String::new()), ..envelope() };
    assert!(matches!(empty_version.validate(), Err(Error::Config(_))));
}
