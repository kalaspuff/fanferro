//! Character rules for SQS/SNS message bodies and attribute values.
//!
//! SQS only accepts the XML 1.0 character range in bodies and string attributes:
//! `#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF`. Anything else
//! (including NUL and most control characters, and U+FFFE/U+FFFF) must be transported
//! base64 encoded. SNS applies the same rules, so fanferro uses one predicate for both.

/// Returns `true` if every character of `text` may appear in an SQS/SNS body.
///
/// Valid UTF-8 cannot hold surrogates, so only two kinds of characters are refused: control
/// characters other than tab, LF and CR (single bytes below 0x20), and U+FFFE and U+FFFF (`EF BF
/// BE` and `EF BF BF`). Both are found by scanning bytes, without decoding characters.
pub fn is_sqs_safe_text(text: &str) -> bool {
    let bytes = text.as_bytes();
    !has_control_bytes(bytes) && !has_noncharacters(bytes)
}

/// Whether `bytes` holds a byte below 0x20 other than tab, LF and CR. Checks 64 bytes at a time
/// without branching on each, so that the compiler vectorises the loop (about 15 times as fast as
/// decoding characters, on a 256 KiB JSON document with some non-ASCII text).
fn has_control_bytes(bytes: &[u8]) -> bool {
    let refused = |byte: u8| (byte < 0x20) & (byte != b'\t') & (byte != b'\n') & (byte != b'\r');
    let (chunks, rest) = bytes.as_chunks::<64>();
    let refused_in = |chunk: &[u8; 64]| {
        let mut found = 0u8;
        for &byte in chunk {
            found |= u8::from(refused(byte));
        }
        found != 0
    };
    chunks.iter().any(refused_in) || rest.iter().any(|&byte| refused(byte))
}

/// Whether `bytes` (valid UTF-8) holds U+FFFE or U+FFFF.
fn has_noncharacters(bytes: &[u8]) -> bool {
    memchr::memmem::find_iter(bytes, b"\xEF\xBF").any(|at| matches!(bytes.get(at + 2), Some(0xBE | 0xBF)))
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
    fn agrees_with_a_check_of_every_character() {
        // Every character, alone and between others (the 64-byte chunks of the control scan).
        let padding = "x".repeat(70);
        for c in (0..=0x10FFFF).filter_map(char::from_u32) {
            let expected = is_sqs_safe_char(c);
            let alone = c.to_string();
            assert_eq!(is_sqs_safe_text(&alone), expected, "{:#x}", c as u32);
            let embedded = format!("é{}{c}{}", &padding[..(c as usize) % 70], &padding[..3]);
            assert_eq!(is_sqs_safe_text(&embedded), expected, "{:#x} embedded", c as u32);
        }
        // Characters whose encodings start like U+FFFE/U+FFFF.
        for text in ["\u{FFFD}", "\u{FFF0}\u{FFFD}", "\u{EFBF}", "\u{FFFD}\u{FFFE}", "a\u{FFFF}"] {
            assert_eq!(is_sqs_safe_text(text), text.chars().all(is_sqs_safe_char), "{text:?}");
        }
    }

    #[test]
    fn bytes_must_be_utf8() {
        assert_eq!(as_sqs_safe_text(b"ok"), Some("ok"));
        assert_eq!(as_sqs_safe_text(&[0xff, 0xfe]), None);
        assert_eq!(as_sqs_safe_text(&[0x00]), None);
    }
}
