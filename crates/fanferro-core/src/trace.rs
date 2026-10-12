//! Trace context helpers: converting between W3C `traceparent` and AWS X-Ray headers.
//!
//! fanferro does not create spans in Rust; the Python layer uses OpenTelemetry's API so
//! spans join the application's traces. These helpers let it propagate context through
//! the AWS-native channel (`AWSTraceHeader` system attribute on SQS, `X-Amzn-Trace-Id` on
//! SNS) in addition to message attributes and envelope headers.

/// Converts a W3C `traceparent` (`00-<trace-id>-<span-id>-<flags>`) to an X-Ray header
/// (`Root=1-<8 hex>-<24 hex>;Parent=<span-id>;Sampled=<0|1>`).
pub fn xray_from_traceparent(traceparent: &str) -> Option<String> {
    let mut parts = traceparent.trim().split('-');
    let (Some(version), Some(trace_id), Some(span_id), Some(flags)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let hex = |s: &str, len: usize| s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit());
    if !hex(version, 2) || version == "ff" || !hex(trace_id, 32) || !hex(span_id, 16) || !hex(flags, 2) {
        return None;
    }
    if trace_id.bytes().all(|b| b == b'0') || span_id.bytes().all(|b| b == b'0') {
        return None;
    }
    let sampled = u8::from_str_radix(flags, 16).ok()? & 1;
    let trace_id = trace_id.to_ascii_lowercase();
    Some(format!(
        "Root=1-{}-{};Parent={};Sampled={sampled}",
        &trace_id[..8],
        &trace_id[8..],
        span_id.to_ascii_lowercase()
    ))
}

/// Converts an X-Ray header to a W3C `traceparent`. Returns `None` when the header has no
/// `Parent` (a traceparent needs a parent span id) or is malformed.
pub fn traceparent_from_xray(header: &str) -> Option<String> {
    let mut root = None;
    let mut parent = None;
    let mut sampled = None;
    for part in header.split(';') {
        let Some((key, value)) = part.trim().split_once('=') else { continue };
        match key.trim() {
            "Root" => root = Some(value.trim()),
            "Parent" => parent = Some(value.trim()),
            "Sampled" => sampled = Some(value.trim()),
            _ => {}
        }
    }
    let root = root?;
    let mut segments = root.split('-');
    let (Some("1"), Some(epoch), Some(unique), None) =
        (segments.next(), segments.next(), segments.next(), segments.next())
    else {
        return None;
    };
    let parent = parent?;
    let hex = |s: &str, len: usize| s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit());
    if !hex(epoch, 8) || !hex(unique, 24) || !hex(parent, 16) {
        return None;
    }
    let flags = if sampled == Some("1") { "01" } else { "00" };
    Some(format!(
        "00-{}{}-{}-{flags}",
        epoch.to_ascii_lowercase(),
        unique.to_ascii_lowercase(),
        parent.to_ascii_lowercase()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let traceparent = "00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-01";
        let xray = xray_from_traceparent(traceparent).unwrap();
        assert_eq!(xray, "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1");
        assert_eq!(traceparent_from_xray(&xray).unwrap(), traceparent);
    }

    #[test]
    fn unsampled_and_extra_fields() {
        let header = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=0;Lineage=a:1";
        assert_eq!(traceparent_from_xray(header).unwrap(), "00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-00");
    }

    #[test]
    fn rejects_malformed_input() {
        assert!(xray_from_traceparent("garbage").is_none());
        assert!(xray_from_traceparent("00-00000000000000000000000000000000-53995c3f42cd8ad8-01").is_none());
        assert!(traceparent_from_xray("Root=1-5759e988-bd862e3fe1be46a994272793").is_none());
        assert!(traceparent_from_xray("Root=2-x-y;Parent=53995c3f42cd8ad8").is_none());
    }
}
