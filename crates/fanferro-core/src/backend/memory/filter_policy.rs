//! A practical subset of SNS subscription filter policies for the in-memory backend.
//!
//! Supported: exact string/number matches, `prefix`, `suffix`, `equals-ignore-case`,
//! `anything-but` (values, prefix), `numeric` ranges, `exists`, `$or`, nested keys for the
//! `MessageBody` scope, and `String.Array` attributes. Semantics follow
//! <https://docs.aws.amazon.com/sns/latest/dg/sns-subscription-filter-policies.html>.

use serde_json::{Map, Value};

use crate::types::{AttributeData, BaseType, MessageAttributes};

/// Where a filter policy is evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterScope {
    /// Message attributes (default).
    MessageAttributes,
    /// The JSON message body.
    MessageBody,
}

/// Returns whether a message passes the filter policy.
pub fn matches(policy: &Value, scope: FilterScope, attributes: &MessageAttributes, body: &str) -> bool {
    let Value::Object(policy) = policy else { return true };
    match scope {
        FilterScope::MessageAttributes => match_attributes(policy, attributes),
        FilterScope::MessageBody => match serde_json::from_str::<Value>(body) {
            Ok(document) => match_object(policy, &document),
            Err(_) => false,
        },
    }
}

fn match_attributes(policy: &Map<String, Value>, attributes: &MessageAttributes) -> bool {
    policy.iter().all(|(key, conditions)| {
        if key == "$or" {
            return match conditions {
                Value::Array(alternatives) => alternatives.iter().any(|alternative| match alternative {
                    Value::Object(inner) => match_attributes(inner, attributes),
                    _ => false,
                }),
                _ => false,
            };
        }
        let values: Option<Vec<Value>> =
            attributes.get(key).map(|attribute| match (attribute.base_type(), attribute.data()) {
                (BaseType::Number, AttributeData::Text(text)) => {
                    vec![
                        text.parse::<f64>()
                            .ok()
                            .and_then(serde_json::Number::from_f64)
                            .map_or(Value::Null, Value::Number),
                    ]
                }
                (_, AttributeData::Text(text)) if attribute.data_type() == "String.Array" => {
                    match serde_json::from_str::<Value>(text) {
                        Ok(Value::Array(items)) => items,
                        _ => vec![Value::String(text.clone())],
                    }
                }
                (_, AttributeData::Text(text)) => vec![Value::String(text.clone())],
                (_, AttributeData::Binary(_)) => vec![],
            });
        match_conditions(conditions, values.as_deref())
    })
}

fn match_object(policy: &Map<String, Value>, document: &Value) -> bool {
    policy.iter().all(|(key, conditions)| {
        if key == "$or" {
            return match conditions {
                Value::Array(alternatives) => alternatives.iter().any(|alternative| match alternative {
                    Value::Object(inner) => match_object(inner, document),
                    _ => false,
                }),
                _ => false,
            };
        }
        let field = document.get(key);
        match conditions {
            Value::Object(nested) => field.is_some_and(|value| match_object(nested, value)),
            _ => {
                let values = field.map(|value| match value {
                    Value::Array(items) => items.clone(),
                    other => vec![other.clone()],
                });
                match_conditions(conditions, values.as_deref())
            }
        }
    })
}

fn match_conditions(conditions: &Value, values: Option<&[Value]>) -> bool {
    let Value::Array(conditions) = conditions else { return false };
    conditions.iter().any(|condition| match_condition(condition, values))
}

fn match_condition(condition: &Value, values: Option<&[Value]>) -> bool {
    match condition {
        Value::Object(operator) => {
            if let Some(Value::Bool(expected)) = operator.get("exists") {
                return values.is_some() == *expected;
            }
            let Some(values) = values else { return false };
            values.iter().any(|value| match_operator(operator, value))
        }
        literal => values.is_some_and(|values| values.iter().any(|value| literal_equals(literal, value))),
    }
}

fn literal_equals(literal: &Value, value: &Value) -> bool {
    match (literal, value) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Number(a), Value::String(b)) => b.parse::<f64>().ok() == a.as_f64(),
        (a, b) => a == b,
    }
}

fn as_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn match_operator(operator: &Map<String, Value>, value: &Value) -> bool {
    operator.iter().all(|(name, argument)| match name.as_str() {
        "prefix" => matches!((argument, value), (Value::String(p), Value::String(v)) if v.starts_with(p.as_str())),
        "suffix" => matches!((argument, value), (Value::String(s), Value::String(v)) if v.ends_with(s.as_str())),
        "equals-ignore-case" => {
            matches!((argument, value), (Value::String(e), Value::String(v)) if e.eq_ignore_ascii_case(v))
        }
        "anything-but" => match argument {
            Value::Array(excluded) => !excluded.iter().any(|item| literal_equals(item, value)),
            Value::Object(inner) => !match_operator(inner, value),
            single => !literal_equals(single, value),
        },
        "numeric" => {
            let (Value::Array(parts), Some(number)) = (argument, as_number(value)) else { return false };
            parts.chunks(2).all(|pair| match pair {
                [Value::String(op), bound] => {
                    let Some(bound) = as_number(bound) else { return false };
                    match op.as_str() {
                        "=" => (number - bound).abs() < f64::EPSILON,
                        "<" => number < bound,
                        "<=" => number <= bound,
                        ">" => number > bound,
                        ">=" => number >= bound,
                        _ => false,
                    }
                }
                _ => false,
            })
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::AttributeValue;

    fn attrs(pairs: &[(&str, AttributeValue)]) -> MessageAttributes {
        pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
    }

    #[test]
    fn exact_and_prefix_matches() {
        let policy = json!({"type": ["OrderCreated", {"prefix": "Invoice"}]});
        let scope = FilterScope::MessageAttributes;
        assert!(matches(&policy, scope, &attrs(&[("type", AttributeValue::string("OrderCreated"))]), ""));
        assert!(matches(&policy, scope, &attrs(&[("type", AttributeValue::string("InvoicePaid"))]), ""));
        assert!(!matches(&policy, scope, &attrs(&[("type", AttributeValue::string("Other"))]), ""));
        assert!(!matches(&policy, scope, &attrs(&[]), ""));
    }

    #[test]
    fn numeric_anything_but_and_exists() {
        let scope = FilterScope::MessageAttributes;
        let policy = json!({"price": [{"numeric": [">", 10, "<=", 20]}]});
        assert!(matches(&policy, scope, &attrs(&[("price", AttributeValue::number("15"))]), ""));
        assert!(!matches(&policy, scope, &attrs(&[("price", AttributeValue::number("25"))]), ""));
        let policy = json!({"env": [{"anything-but": ["test", "dev"]}]});
        assert!(matches(&policy, scope, &attrs(&[("env", AttributeValue::string("prod"))]), ""));
        assert!(!matches(&policy, scope, &attrs(&[("env", AttributeValue::string("dev"))]), ""));
        let policy = json!({"tenant": [{"exists": false}]});
        assert!(matches(&policy, scope, &attrs(&[]), ""));
        assert!(!matches(&policy, scope, &attrs(&[("tenant", AttributeValue::string("a"))]), ""));
    }

    #[test]
    fn string_arrays_and_or() {
        let scope = FilterScope::MessageAttributes;
        let array = AttributeValue::with_type("String.Array", AttributeData::Text(r#"["a","b"]"#.into())).unwrap();
        let policy = json!({"tags": ["b"]});
        assert!(matches(&policy, scope, &attrs(&[("tags", array.clone())]), ""));
        let policy = json!({"$or": [{"x": ["1"]}, {"tags": ["a"]}]});
        assert!(matches(&policy, scope, &attrs(&[("tags", array)]), ""));
    }

    #[test]
    fn message_body_scope_with_nesting() {
        let policy = json!({"order": {"status": ["paid"], "total": [{"numeric": [">=", 100]}]}});
        let body = r#"{"order":{"status":"paid","total":150}}"#;
        assert!(matches(&policy, FilterScope::MessageBody, &MessageAttributes::new(), body));
        let body = r#"{"order":{"status":"open","total":150}}"#;
        assert!(!matches(&policy, FilterScope::MessageBody, &MessageAttributes::new(), body));
        assert!(!matches(&policy, FilterScope::MessageBody, &MessageAttributes::new(), "not json"));
    }
}
