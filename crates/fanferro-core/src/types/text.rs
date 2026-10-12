//! Character rules for SQS/SNS message bodies and attribute values.
//!
//! SQS only accepts the XML 1.0 character range in bodies and string attributes:
//! `#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF`. Anything else
//! (including NUL and most control characters, and U+FFFE/U+FFFF) must be transported
//! base64 encoded. SNS applies the same rules, so fanferro uses one predicate for both.

/// Returns `true` if every character of `text` may appear in an SQS/SNS body.
pub fn is_sqs_safe_text(text: &str) -> bool {
    // Fast path: plain ASCII without control characters (except tab, LF, CR).
    let bytes = text.as_bytes();
    if bytes.iter().all(|&b| b.is_ascii() && (b >= 0x20 || matches!(b, b'\t' | b'\n' | b'\r'))) {
        return true;
    }
    text.chars().all(is_sqs_safe_char)
}

/// Returns `true` if `c` may appear in an SQS/SNS body.
pub fn is_sqs_safe_char(c: char) -> bool {
    matches!(c,
        '\t' | '\n' | '\r'
        | '\u{20}'..='\u{D7FF}'
        | '\u{E000}'..='\u{FFFD}'
        | '\u{10000}'..='\u{10FFFF}')
}

/// Interprets `bytes` as SQS-safe text, if possible.
pub fn as_sqs_safe_text(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    is_sqs_safe_text(text).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_regular_text() {
        assert!(is_sqs_safe_text("hello world\n\t\r"));
        assert!(is_sqs_safe_text("héllo — 你好 🚀"));
        assert!(is_sqs_safe_text(""));
    }

    #[test]
    fn rejects_control_and_noncharacters() {
        assert!(!is_sqs_safe_text("nul\u{0}"));
        assert!(!is_sqs_safe_text("bell\u{7}"));
        assert!(!is_sqs_safe_text("\u{FFFE}"));
        assert!(!is_sqs_safe_text("\u{FFFF}"));
    }

    #[test]
    fn bytes_must_be_utf8() {
        assert_eq!(as_sqs_safe_text(b"ok"), Some("ok"));
        assert_eq!(as_sqs_safe_text(&[0xff, 0xfe]), None);
        assert_eq!(as_sqs_safe_text(&[0x00]), None);
    }
}
