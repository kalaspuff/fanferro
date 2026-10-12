//! Comparing desired and actual resource attributes.

use serde_json::Value;

use crate::backend::StringMap;

/// Attributes that cannot be changed after creation.
pub const IMMUTABLE_ATTRIBUTES: &[&str] = &["FifoQueue", "FifoTopic"];

const JSON_ATTRIBUTES: &[&str] = &[
    "Policy",
    "RedrivePolicy",
    "RedriveAllowPolicy",
    "DeliveryPolicy",
    "FilterPolicy",
    "ArchivePolicy",
    "DataProtectionPolicy",
];

/// One attribute whose actual value differs from the desired value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Drift {
    /// Attribute name (tags are reported as `tag:<key>`).
    pub attribute: String,
    /// Desired value.
    pub expected: String,
    /// Actual value (`None` when absent).
    pub actual: Option<String>,
    /// Whether the attribute can be updated in place.
    pub mutable: bool,
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(map.iter().map(|(k, v)| (k.clone(), canonical_json(v))).collect()),
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        Value::String(text) => match text.parse::<i64>() {
            Ok(number) if number.to_string() == *text => Value::from(number),
            _ => Value::String(text.clone()),
        },
        other => other.clone(),
    }
}

/// Normalizes an attribute value for comparison (JSON documents, booleans, whitespace).
pub fn normalize(name: &str, value: &str) -> String {
    let trimmed = value.trim();
    if JSON_ATTRIBUTES.contains(&name) {
        if let Ok(json) = serde_json::from_str::<Value>(trimmed) {
            if json.as_object().is_some_and(serde_json::Map::is_empty) {
                return String::new();
            }
            return canonical_json(&json).to_string();
        }
        return trimmed.to_string();
    }
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return trimmed.to_ascii_lowercase();
    }
    trimmed.to_string()
}

/// Returns the attributes in `desired` whose actual value differs.
pub fn compare(desired: &StringMap, actual: &StringMap) -> Vec<Drift> {
    desired
        .iter()
        .filter_map(|(name, expected)| {
            let actual_value = actual.get(name);
            let wanted = normalize(name, expected);
            let current = actual_value.map(|value| normalize(name, value)).unwrap_or_default();
            (wanted != current).then(|| Drift {
                attribute: name.clone(),
                expected: expected.clone(),
                actual: actual_value.cloned(),
                mutable: !IMMUTABLE_ATTRIBUTES.contains(&name.as_str()),
            })
        })
        .collect()
}

/// Returns tags in `desired` that are missing or different in `actual`.
pub fn compare_tags(desired: &StringMap, actual: &StringMap) -> Vec<Drift> {
    desired
        .iter()
        .filter(|(key, value)| actual.get(*key) != Some(*value))
        .map(|(key, value)| Drift {
            attribute: format!("tag:{key}"),
            expected: value.clone(),
            actual: actual.get(key).cloned(),
            mutable: true,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> StringMap {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[test]
    fn json_and_boolean_normalization() {
        let desired = map(&[
            ("RedrivePolicy", r#"{"maxReceiveCount":5,"deadLetterTargetArn":"arn:x"}"#),
            ("ContentBasedDeduplication", "True"),
            ("VisibilityTimeout", "30"),
        ]);
        let actual = map(&[
            ("RedrivePolicy", r#"{"deadLetterTargetArn":"arn:x","maxReceiveCount":"5"}"#),
            ("ContentBasedDeduplication", "true"),
            ("VisibilityTimeout", "30"),
        ]);
        assert!(compare(&desired, &actual).is_empty());
    }

    #[test]
    fn reports_differences_and_mutability() {
        let desired = map(&[("VisibilityTimeout", "60"), ("FifoQueue", "true"), ("Policy", "")]);
        let actual = map(&[("VisibilityTimeout", "30")]);
        let drift = compare(&desired, &actual);
        assert_eq!(drift.len(), 2);
        assert_eq!(drift.iter().find(|d| d.attribute == "VisibilityTimeout").unwrap().actual.as_deref(), Some("30"));
        assert!(!drift.iter().find(|d| d.attribute == "FifoQueue").unwrap().mutable);
    }

    #[test]
    fn empty_json_documents_equal_absent() {
        assert!(compare(&map(&[("FilterPolicy", "{}")]), &map(&[])).is_empty());
    }

    #[test]
    fn tag_drift() {
        let drift = compare_tags(&map(&[("team", "a"), ("env", "prod")]), &map(&[("team", "a"), ("env", "dev")]));
        assert_eq!(drift.len(), 1);
        assert_eq!(drift[0].attribute, "tag:env");
    }
}
