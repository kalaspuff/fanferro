//! Names of message attributes that fanferro reads or writes.
//!
//! All fanferro-owned attributes use the `fanferro.` prefix, except `content-type`
//! (a widely used convention) and the attribute names used by the AWS extended client
//! libraries, which fanferro supports for interoperability.
//!
//! Keep `docs/reference/attributes.md` in sync when adding names here (CI checks it).

/// Prefix of all fanferro-owned attributes.
pub const PREFIX: &str = "fanferro.";

/// Media type of the wire body (envelope format, or payload type for the raw envelope).
pub const CONTENT_TYPE: &str = "content-type";

/// Logical message type (promoted so SNS filter policies and routers can use it).
pub const MESSAGE_TYPE: &str = "fanferro.type";

/// Message id for envelopes that do not carry one in the body (raw envelope).
pub const MESSAGE_ID: &str = "fanferro.id";

/// Set to `base64` when the transport base64-encoded a binary body.
pub const ENCODING: &str = "fanferro.encoding";

/// Marks a claim-check pointer body (large payload stored externally). Value: format version.
pub const CLAIM_CHECK: &str = "fanferro.claim-check";

/// Retry attempt counter carried across re-enqueues.
pub const ATTEMPT: &str = "fanferro.attempt";

/// JSON document describing why a message was dead-lettered by fanferro.
pub const DEAD_LETTER: &str = "fanferro.dead-letter";

/// Attribute used by the AWS SQS/SNS extended client libraries for offloaded payloads.
pub const EXTENDED_PAYLOAD_SIZE: &str = "ExtendedPayloadSize";

/// Legacy attribute name used by older AWS extended client versions.
pub const LEGACY_EXTENDED_PAYLOAD_SIZE: &str = "SQSLargePayloadSize";

/// W3C trace context attribute names (written when message-attribute propagation is on).
pub const TRACEPARENT: &str = "traceparent";
/// W3C trace state attribute name.
pub const TRACESTATE: &str = "tracestate";
/// W3C baggage attribute name.
pub const BAGGAGE: &str = "baggage";

/// Returns `true` for attributes that are transport/infrastructure details and should be
/// hidden from the application-facing attribute view.
pub fn is_internal(name: &str) -> bool {
    name.starts_with(PREFIX) || name == EXTENDED_PAYLOAD_SIZE || name == LEGACY_EXTENDED_PAYLOAD_SIZE
}

/// Every attribute name defined in this module (used by documentation checks).
pub const ALL: &[&str] = &[
    CONTENT_TYPE,
    MESSAGE_TYPE,
    MESSAGE_ID,
    ENCODING,
    CLAIM_CHECK,
    ATTEMPT,
    DEAD_LETTER,
    EXTENDED_PAYLOAD_SIZE,
    LEGACY_EXTENDED_PAYLOAD_SIZE,
    TRACEPARENT,
    TRACESTATE,
    BAGGAGE,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_detection() {
        assert!(is_internal("fanferro.type"));
        assert!(is_internal(EXTENDED_PAYLOAD_SIZE));
        assert!(!is_internal("content-type"));
        assert!(!is_internal("tenant"));
        assert!(!is_internal("traceparent"));
    }
}
