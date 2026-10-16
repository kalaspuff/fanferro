//! Queue access policies that let SNS topics deliver to SQS queues.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Prefix of the ids of the statements fanferro adds for SNS subscriptions.
const MANAGED_SID_PREFIX: &str = "fanferro-sns-";

/// The statement id fanferro uses for the permission of `topic_arn`.
pub fn statement_id(topic_arn: &str) -> String {
    format!("{MANAGED_SID_PREFIX}{}", &hex::encode(Sha256::digest(topic_arn.as_bytes()))[..16])
}

fn statement_sid(statement: &Value) -> Option<&str> {
    statement.get("Sid").and_then(Value::as_str)
}

/// The policy `desired` (from a queue spec) plus the statements fanferro added to the queue's
/// `actual` policy for SNS subscriptions, so that provisioning the queue does not revoke them (SNS
/// drops messages a queue policy rejects). `None` when there is nothing to add, or when either
/// policy is not a JSON object.
pub fn with_managed_statements(desired: &str, actual: &str) -> Option<String> {
    let mut document = serde_json::from_str::<Value>(desired).ok()?;
    let actual = serde_json::from_str::<Value>(actual).ok()?;
    let object = document.as_object_mut()?;
    let mut statements: Vec<Value> =
        object.get("Statement").map(as_list).unwrap_or_default().into_iter().cloned().collect();
    let managed: Vec<Value> = actual
        .get("Statement")
        .map(as_list)
        .unwrap_or_default()
        .into_iter()
        .filter(|statement| {
            statement_sid(statement).is_some_and(|sid| {
                sid.starts_with(MANAGED_SID_PREFIX)
                    && !statements.iter().any(|existing| statement_sid(existing) == Some(sid))
            })
        })
        .cloned()
        .collect();
    if managed.is_empty() {
        return None;
    }
    statements.extend(managed);
    object.insert("Statement".into(), Value::Array(statements));
    Some(document.to_string())
}

fn as_list(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(items) => items.iter().collect(),
        Value::Null => Vec::new(),
        other => vec![other],
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == text;
    }
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti, mut star, mut mark) = (0, 0, None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn principal_allows_sns(principal: Option<&Value>) -> bool {
    match principal {
        Some(Value::String(any)) => any == "*",
        Some(Value::Object(map)) => {
            map.get("Service")
                .is_some_and(|service| as_list(service).iter().any(|s| s.as_str() == Some("sns.amazonaws.com")))
                || map.get("AWS").is_some_and(|aws| as_list(aws).iter().any(|a| a.as_str() == Some("*")))
        }
        _ => false,
    }
}

fn action_allows_send(action: Option<&Value>) -> bool {
    action.is_some_and(|action| {
        as_list(action).iter().any(|a| {
            a.as_str().is_some_and(|a| {
                let a = a.to_ascii_lowercase();
                a == "*" || a == "sqs:*" || a == "sqs:sendmessage"
            })
        })
    })
}

fn resource_matches(resource: Option<&Value>, queue_arn: &str) -> bool {
    match resource {
        None => true,
        Some(resource) => as_list(resource).iter().any(|r| r.as_str().is_some_and(|r| glob_match(r, queue_arn))),
    }
}

fn condition_allows(condition: Option<&Value>, topic_arn: &str) -> bool {
    let Some(Value::Object(operators)) = condition else { return true };
    operators.iter().all(|(operator, clauses)| {
        let Value::Object(clauses) = clauses else { return false };
        clauses.iter().all(|(key, values)| {
            let key = key.to_ascii_lowercase();
            if key != "aws:sourcearn" {
                // Unknown conditions (e.g. aws:SourceAccount) are assumed to be satisfied.
                return true;
            }
            let wildcard = operator.contains("Like");
            as_list(values)
                .iter()
                .any(|v| v.as_str().is_some_and(|v| if wildcard { glob_match(v, topic_arn) } else { v == topic_arn }))
        })
    })
}

/// Whether `policy` already lets `topic_arn` send messages to `queue_arn`.
pub fn allows_topic(policy: &Value, queue_arn: &str, topic_arn: &str) -> bool {
    policy.get("Statement").map(as_list).unwrap_or_default().into_iter().any(|statement| {
        statement.get("Effect").and_then(Value::as_str) == Some("Allow")
            && principal_allows_sns(statement.get("Principal"))
            && action_allows_send(statement.get("Action"))
            && resource_matches(statement.get("Resource"), queue_arn)
            && condition_allows(statement.get("Condition"), topic_arn)
    })
}

/// Returns the policy with a statement allowing `topic_arn`, or `None` if it already allows it.
pub fn with_topic_permission(policy: Option<&str>, queue_arn: &str, topic_arn: &str) -> Result<Option<String>> {
    let mut document = match policy.map(str::trim).filter(|p| !p.is_empty()) {
        Some(text) => serde_json::from_str::<Value>(text)
            .map_err(|err| Error::validation(format!("queue policy is not valid JSON: {err}")))?,
        None => json!({"Version": "2012-10-17", "Statement": []}),
    };
    if allows_topic(&document, queue_arn, topic_arn) {
        return Ok(None);
    }
    let sid = statement_id(topic_arn);
    let statement = json!({
        "Sid": sid,
        "Effect": "Allow",
        "Principal": {"Service": "sns.amazonaws.com"},
        "Action": "sqs:SendMessage",
        "Resource": queue_arn,
        "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
    });
    let object = document.as_object_mut().ok_or_else(|| Error::validation("queue policy must be a JSON object"))?;
    object.entry("Version").or_insert_with(|| json!("2012-10-17"));
    let mut statements: Vec<Value> =
        object.get("Statement").map(as_list).unwrap_or_default().into_iter().cloned().collect();
    statements.retain(|existing| statement_sid(existing) != Some(sid.as_str()));
    statements.push(statement);
    object.insert("Statement".into(), Value::Array(statements));
    Ok(Some(document.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: &str = "arn:aws:sqs:us-east-1:111111111111:orders";
    const TOPIC: &str = "arn:aws:sns:us-east-1:111111111111:events";

    #[test]
    fn adds_statement_to_empty_policy() {
        let policy = with_topic_permission(None, QUEUE, TOPIC).unwrap().unwrap();
        let document: Value = serde_json::from_str(&policy).unwrap();
        assert!(allows_topic(&document, QUEUE, TOPIC));
        assert!(!allows_topic(&document, QUEUE, "arn:aws:sns:us-east-1:111111111111:other"));
        // Idempotent.
        assert!(with_topic_permission(Some(&policy), QUEUE, TOPIC).unwrap().is_none());
    }

    #[test]
    fn keeps_existing_statements() {
        let existing = r#"{"Version":"2012-10-17","Statement":{"Sid":"other","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1:root"},"Action":"sqs:*","Resource":"*"}}"#;
        let policy = with_topic_permission(Some(existing), QUEUE, TOPIC).unwrap().unwrap();
        let document: Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(document["Statement"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn recognises_equivalent_permissions() {
        let wildcard = serde_json::json!({"Statement": [{
            "Effect": "Allow", "Principal": "*", "Action": ["sqs:SendMessage"], "Resource": "arn:aws:sqs:*:111111111111:*",
            "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:sns:us-east-1:111111111111:*"}}
        }]});
        assert!(allows_topic(&wildcard, QUEUE, TOPIC));
        let deny =
            serde_json::json!({"Statement": [{"Effect": "Deny", "Principal": "*", "Action": "sqs:SendMessage"}]});
        assert!(!allows_topic(&deny, QUEUE, TOPIC));
    }

    #[test]
    fn glob() {
        assert!(glob_match("arn:*:x", "arn:aws:x"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a*d", "abc"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn managed_statements_are_kept_in_desired_policies() {
        let desired = r#"{"Version":"2012-10-17","Statement":{"Sid":"other","Effect":"Allow","Principal":"*","Action":"sqs:ReceiveMessage","Resource":"*"}}"#;
        let actual = with_topic_permission(Some(desired), QUEUE, TOPIC).unwrap().unwrap();
        let merged = with_managed_statements(desired, &actual).unwrap();
        let document: Value = serde_json::from_str(&merged).unwrap();
        assert!(allows_topic(&document, QUEUE, TOPIC));
        assert_eq!(document["Statement"].as_array().unwrap().len(), 2);
        // Nothing to add: the desired policy has the statement already, or the queue has none.
        assert!(with_managed_statements(&merged, &actual).is_none());
        assert!(with_managed_statements(desired, desired).is_none());
        assert!(with_managed_statements("not json", &actual).is_none());
    }

    #[test]
    fn invalid_policy_is_rejected() {
        assert!(with_topic_permission(Some("not json"), QUEUE, TOPIC).is_err());
    }
}
