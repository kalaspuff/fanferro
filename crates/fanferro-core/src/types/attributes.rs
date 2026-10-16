//! Message attributes shared by SQS and SNS.
//!
//! SQS and SNS both allow at most [`MAX_ATTRIBUTES`] message attributes per message. Each
//! attribute has a data type (`String`, `Number` or `Binary`, optionally followed by a
//! custom label such as `Number.int`) and a value. Attribute names, types and values all
//! count towards the message size limit.

use std::borrow::Cow;
use std::collections::BTreeMap;

use bytes::Bytes;
use md5::{Digest, Md5};

use crate::error::{Error, Result};
use crate::types::text::is_sqs_safe_text;

/// Maximum number of message attributes per message (SQS and SNS).
pub const MAX_ATTRIBUTES: usize = 10;

/// Maximum length of an attribute name.
pub const MAX_ATTRIBUTE_NAME_LEN: usize = 256;

/// Message attributes keyed by name. A `BTreeMap` keeps them sorted, which is what the
/// SQS MD5 algorithm and deterministic size calculations need.
pub type MessageAttributes = BTreeMap<String, AttributeValue>;

/// The base data type of an attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BaseType {
    /// UTF-8 text.
    String,
    /// A number transported as text.
    Number,
    /// Arbitrary bytes.
    Binary,
}

impl BaseType {
    /// The AWS name of the base type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "String",
            Self::Number => "Number",
            Self::Binary => "Binary",
        }
    }
}

/// The value of an attribute: text for `String`/`Number`, bytes for `Binary`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AttributeData {
    /// Text value (used by `String` and `Number` types).
    Text(String),
    /// Binary value (used by `Binary` types).
    Binary(Bytes),
}

/// A single message attribute value with its data type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AttributeValue {
    /// The plain base types are static strings: creating and cloning them allocates nothing.
    data_type: Cow<'static, str>,
    data: AttributeData,
}

/// `data_type` without an allocation of its own when it is a plain base type.
fn shared_data_type(data_type: String) -> Cow<'static, str> {
    match data_type.as_str() {
        "String" => Cow::Borrowed("String"),
        "Number" => Cow::Borrowed("Number"),
        "Binary" => Cow::Borrowed("Binary"),
        _ => Cow::Owned(data_type),
    }
}

impl AttributeValue {
    /// Creates a `String` attribute.
    pub fn string(value: impl Into<String>) -> Self {
        Self { data_type: Cow::Borrowed("String"), data: AttributeData::Text(value.into()) }
    }

    /// Creates a `Number` attribute from its textual representation.
    pub fn number(value: impl Into<String>) -> Self {
        Self { data_type: Cow::Borrowed("Number"), data: AttributeData::Text(value.into()) }
    }

    /// Creates a `Binary` attribute.
    pub fn binary(value: impl Into<Bytes>) -> Self {
        Self { data_type: Cow::Borrowed("Binary"), data: AttributeData::Binary(value.into()) }
    }

    /// Creates an attribute with an explicit (possibly custom) data type, e.g. `Number.int`.
    ///
    /// The data must match the base type: text for `String`/`Number`, bytes for `Binary`.
    pub fn with_type(data_type: impl Into<String>, data: AttributeData) -> Result<Self> {
        let data_type = shared_data_type(data_type.into());
        let base = parse_base_type(&data_type)?;
        match (base, &data) {
            (BaseType::Binary, AttributeData::Binary(_))
            | (BaseType::String | BaseType::Number, AttributeData::Text(_)) => Ok(Self { data_type, data }),
            (BaseType::Binary, AttributeData::Text(_)) => {
                Err(Error::validation(format!("attribute data type {data_type:?} requires a binary value")))
            }
            _ => Err(Error::validation(format!("attribute data type {data_type:?} requires a text value"))),
        }
    }

    /// The full data type, e.g. `String` or `Number.float`.
    pub fn data_type(&self) -> &str {
        &self.data_type
    }

    /// The base data type.
    pub fn base_type(&self) -> BaseType {
        parse_base_type(&self.data_type).unwrap_or(BaseType::String)
    }

    /// The value.
    pub fn data(&self) -> &AttributeData {
        &self.data
    }

    /// The data type and the value.
    pub fn into_parts(self) -> (String, AttributeData) {
        (self.data_type.into_owned(), self.data)
    }

    /// The text value, if this is a `String` or `Number` attribute.
    pub fn as_text(&self) -> Option<&str> {
        match &self.data {
            AttributeData::Text(text) => Some(text),
            AttributeData::Binary(_) => None,
        }
    }

    /// The binary value, if this is a `Binary` attribute.
    pub fn as_binary(&self) -> Option<&Bytes> {
        match &self.data {
            AttributeData::Binary(bytes) => Some(bytes),
            AttributeData::Text(_) => None,
        }
    }

    /// Number of bytes this value contributes to the message size (type plus value).
    pub fn encoded_len(&self) -> usize {
        self.data_type.len()
            + match &self.data {
                AttributeData::Text(text) => text.len(),
                AttributeData::Binary(bytes) => bytes.len(),
            }
    }

    /// Validates the value against SQS/SNS rules.
    pub fn validate(&self, name: &str) -> Result<()> {
        parse_base_type(&self.data_type)?;
        match &self.data {
            AttributeData::Text(text) => {
                if text.is_empty() {
                    return Err(Error::validation(format!("attribute {name:?} must have a non-empty value")));
                }
                if !is_sqs_safe_text(text) {
                    return Err(Error::validation(format!(
                        "attribute {name:?} contains characters that SQS/SNS do not accept"
                    )));
                }
                if self.base_type() == BaseType::Number && !is_valid_number(text) {
                    return Err(Error::validation(format!("attribute {name:?} is not a valid number: {text:?}")));
                }
            }
            AttributeData::Binary(bytes) => {
                if bytes.is_empty() {
                    return Err(Error::validation(format!("attribute {name:?} must have a non-empty value")));
                }
            }
        }
        Ok(())
    }
}

/// Parses the base type out of a data type such as `Number.int`.
pub fn parse_base_type(data_type: &str) -> Result<BaseType> {
    let (base, label) = match data_type.split_once('.') {
        Some((base, label)) => (base, Some(label)),
        None => (data_type, None),
    };
    if let Some(label) = label
        && (label.is_empty() || label.len() > 256)
    {
        return Err(Error::validation(format!("invalid attribute data type {data_type:?}")));
    }
    match base {
        "String" => Ok(BaseType::String),
        "Number" => Ok(BaseType::Number),
        "Binary" => Ok(BaseType::Binary),
        _ => Err(Error::validation(format!(
            "invalid attribute data type {data_type:?}; must start with String, Number or Binary"
        ))),
    }
}

fn is_valid_number(text: &str) -> bool {
    let trimmed = text.trim();
    !trimmed.is_empty() && trimmed.len() == text.len() && trimmed.parse::<f64>().is_ok_and(f64::is_finite)
}

/// Validates an attribute name against SQS/SNS rules.
pub fn validate_attribute_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_ATTRIBUTE_NAME_LEN {
        return Err(Error::validation(format!(
            "attribute name {name:?} must be 1-{MAX_ATTRIBUTE_NAME_LEN} characters"
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')) {
        return Err(Error::validation(format!(
            "attribute name {name:?} may only contain letters, digits, '_', '-' and '.'"
        )));
    }
    if name.starts_with('.') || name.ends_with('.') || name.contains("..") {
        return Err(Error::validation(format!(
            "attribute name {name:?} must not start or end with '.' or contain '..'"
        )));
    }
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("aws.") || lower.starts_with("amazon.") {
        return Err(Error::validation(format!("attribute name {name:?} uses a reserved prefix (AWS. / Amazon.)")));
    }
    Ok(())
}

/// Validates a full attribute map (names, values and count).
pub fn validate_attributes(attributes: &MessageAttributes) -> Result<()> {
    if attributes.len() > MAX_ATTRIBUTES {
        return Err(Error::validation(format!(
            "a message can carry at most {MAX_ATTRIBUTES} attributes, got {} ({}); reduce user attributes or disable optional fanferro attributes",
            attributes.len(),
            attributes.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    for (name, value) in attributes {
        validate_attribute_name(name)?;
        value.validate(name)?;
    }
    Ok(())
}

/// Number of bytes the attributes contribute to the message size (names, types and values).
pub fn attributes_encoded_len(attributes: &MessageAttributes) -> usize {
    attributes.iter().map(|(name, value)| name.len() + value.encoded_len()).sum()
}

/// Computes the MD5 digest SQS reports as `MD5OfMessageAttributes`.
///
/// Algorithm (from the SQS developer guide): for each attribute sorted by name, append the
/// big-endian `u32` length and bytes of the name, the data type, a transport type byte
/// (1 for String/Number, 2 for Binary) and the length-prefixed value.
pub fn md5_of_attributes(attributes: &MessageAttributes) -> String {
    let mut hasher = Md5::new();
    for (name, value) in attributes {
        update_length_prefixed(&mut hasher, name.as_bytes());
        update_length_prefixed(&mut hasher, value.data_type.as_bytes());
        match &value.data {
            AttributeData::Text(text) => {
                hasher.update([1u8]);
                update_length_prefixed(&mut hasher, text.as_bytes());
            }
            AttributeData::Binary(bytes) => {
                hasher.update([2u8]);
                update_length_prefixed(&mut hasher, bytes);
            }
        }
    }
    hex::encode(hasher.finalize())
}

/// Computes the MD5 digest SQS reports as `MD5OfMessageBody`.
pub fn md5_of_body(body: &str) -> String {
    hex::encode(Md5::digest(body.as_bytes()))
}

fn update_length_prefixed(hasher: &mut Md5, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    hasher.update(len.to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_validated() {
        assert!(validate_attribute_name("tenant").is_ok());
        assert!(validate_attribute_name("fanferro.type").is_ok());
        assert!(validate_attribute_name("content-type").is_ok());
        assert!(validate_attribute_name("").is_err());
        assert!(validate_attribute_name(".x").is_err());
        assert!(validate_attribute_name("x.").is_err());
        assert!(validate_attribute_name("a..b").is_err());
        assert!(validate_attribute_name("AWS.trace").is_err());
        assert!(validate_attribute_name("amazon.x").is_err());
        assert!(validate_attribute_name("with space").is_err());
        assert!(validate_attribute_name(&"a".repeat(257)).is_err());
    }

    #[test]
    fn values_are_validated() {
        assert!(AttributeValue::string("x").validate("a").is_ok());
        assert!(AttributeValue::string("").validate("a").is_err());
        assert!(AttributeValue::number("12.5").validate("a").is_ok());
        assert!(AttributeValue::number("-3e5").validate("a").is_ok());
        assert!(AttributeValue::number("nope").validate("a").is_err());
        assert!(AttributeValue::number(" 1").validate("a").is_err());
        assert!(AttributeValue::binary(vec![1, 2]).validate("a").is_ok());
        assert!(AttributeValue::binary(Vec::<u8>::new()).validate("a").is_err());
        assert!(AttributeValue::string("bad\u{0}char").validate("a").is_err());
    }

    #[test]
    fn custom_types() {
        let value = AttributeValue::with_type("Number.int", AttributeData::Text("5".into())).unwrap();
        assert_eq!(value.base_type(), BaseType::Number);
        assert!(AttributeValue::with_type("Binary.png", AttributeData::Text("x".into())).is_err());
        assert!(AttributeValue::with_type("Strange", AttributeData::Text("x".into())).is_err());
        assert!(AttributeValue::with_type("String.", AttributeData::Text("x".into())).is_err());
    }

    #[test]
    fn too_many_attributes() {
        let attrs: MessageAttributes = (0..11).map(|i| (format!("a{i}"), AttributeValue::string("v"))).collect();
        let err = validate_attributes(&attrs).unwrap_err();
        assert!(err.to_string().contains("at most 10"));
    }

    #[test]
    fn encoded_len_counts_names_types_and_values() {
        let mut attrs = MessageAttributes::new();
        attrs.insert("ab".into(), AttributeValue::string("xyz"));
        attrs.insert("n".into(), AttributeValue::binary(vec![0u8; 4]));
        // "ab" + "String" + "xyz" + "n" + "Binary" + 4 bytes
        assert_eq!(attributes_encoded_len(&attrs), 2 + 6 + 3 + 1 + 6 + 4);
    }

    #[test]
    fn md5_of_body_matches_known_value() {
        assert_eq!(md5_of_body("hello"), "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn md5_of_attributes_matches_aws_reference() {
        // Reference value produced by the AWS SDK algorithm for a single String attribute.
        let mut attrs = MessageAttributes::new();
        attrs.insert("attr".into(), AttributeValue::string("value"));
        let mut hasher = Md5::new();
        hasher.update(4u32.to_be_bytes());
        hasher.update(b"attr");
        hasher.update(6u32.to_be_bytes());
        hasher.update(b"String");
        hasher.update([1u8]);
        hasher.update(5u32.to_be_bytes());
        hasher.update(b"value");
        assert_eq!(md5_of_attributes(&attrs), hex::encode(hasher.finalize()));
    }
}
