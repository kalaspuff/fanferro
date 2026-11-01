//! EventBridge events that a rule sends to an SQS queue or SNS topic.
//!
//! Every EventBridge event, whether an AWS service, a SaaS partner or an application (`PutEvents`)
//! sent it, has the same wrapper:
//!
//! ```json
//! {"version": "0", "id": "...", "detail-type": "ECS Task State Change", "source": "aws.ecs",
//!  "account": "111122223333", "time": "2026-11-01T12:00:00Z", "region": "eu-north-1",
//!  "resources": ["arn:..."], "detail": {...}}
//! ```
//!
//! It is an envelope like a CloudEvent: the payload is the `detail` (a slice of the body, not a
//! copy), the message id the event's `id` (kept when EventBridge delivers it again), its time
//! the event's `time` and its source the event's `source`. `detail-type`, `account`, `region` and
//! `resources` (comma-separated) become headers. The message type is:
//!
//! * `aws.<service>.<event>` for AWS services (`source` `aws.<service>`): the snake-cased
//!   `detail-type` without a leading `aws_` or `<service>_` (`ECS Task State Change` from `aws.ecs`
//!   is `aws.ecs.task_state_change`, `AWS Health Event` from `aws.health` is `aws.health.event`).
//!   SES email events keep the types of SES's notifications (`aws.ses.bounce`, ...), which their
//!   `detail` is.
//! * the `detail-type` as it is for every other source (SaaS partners, applications), which
//!   names the event as its producer does.

use std::borrow::Cow;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{AWS_SES_TYPE_PREFIX, lambda_invocation_type, parse_timestamp, snake_case};
use crate::envelope::share;
use crate::types::{HeaderValue, Headers, Payload};

const JSON_CONTENT_TYPE: &str = "application/json";

/// The wrapper of an EventBridge event (the fields every delivered event has are required).
#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(rename = "detail-type", borrow)]
    detail_type: Cow<'a, str>,
    #[serde(borrow)]
    source: Cow<'a, str>,
    #[serde(borrow)]
    time: Cow<'a, str>,
    #[serde(borrow)]
    detail: &'a RawValue,
    #[serde(borrow, default)]
    account: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    region: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    resources: Vec<Cow<'a, str>>,
}

/// The `eventType` of SES email events' `detail`.
#[derive(Deserialize)]
struct SesDetail<'a> {
    #[serde(rename = "eventType", borrow, default)]
    event_type: Option<Cow<'a, str>>,
}

/// Lowercase words joined by `_`: `CloudWatch Alarm State Change` -> `cloudwatch_alarm_state_change`.
///
/// Detail types are words separated by spaces (`EC2 Instance State-change Notification`), so words
/// in camel case stay whole; one without spaces (`DeliveryDelay`) is snake-cased by its capitals.
fn words(text: &str) -> String {
    if !text.contains(' ') {
        return snake_case(text);
    }
    let mut result = String::with_capacity(text.len());
    for word in text.split(|c: char| !c.is_ascii_alphanumeric()).filter(|word| !word.is_empty()) {
        if !result.is_empty() {
            result.push('_');
        }
        result.push_str(&word.to_ascii_lowercase());
    }
    result
}

/// The message type of an event of `source` and `detail_type`.
pub fn message_type(source: &str, detail_type: &str, detail: &str) -> String {
    // Lambda's destinations send their records with the source `lambda`.
    if source == "lambda" {
        match detail_type {
            "Lambda Function Invocation Result - Success" => return lambda_invocation_type(true).to_string(),
            "Lambda Function Invocation Result - Failure" => return lambda_invocation_type(false).to_string(),
            _ => {}
        }
    }
    if source == "aws.ses"
        && let Ok(SesDetail { event_type: Some(event_type) }) = serde_json::from_str::<SesDetail<'_>>(detail)
        && !event_type.is_empty()
    {
        return format!("{AWS_SES_TYPE_PREFIX}{}", snake_case(&event_type));
    }
    match source.strip_prefix("aws.") {
        Some(service) if !service.is_empty() && !service.starts_with("partner/") => {
            let snake = words(detail_type);
            let service_prefix = format!("{}_", words(service));
            let event = snake.strip_prefix("aws_").unwrap_or(&snake);
            let event = event.strip_prefix(&service_prefix).filter(|event| !event.is_empty()).unwrap_or(event);
            format!("aws.{service}.{event}")
        }
        _ => detail_type.to_string(),
    }
}

/// The payload of `body` if it is an EventBridge event.
pub(super) fn unwrap(body: &Bytes) -> Option<Payload> {
    let event = serde_json::from_slice::<Event<'_>>(body).ok()?;
    if event.detail_type.is_empty() || event.source.is_empty() {
        return None;
    }
    let mut headers = Headers::new();
    headers.insert("detail-type".into(), HeaderValue::String(event.detail_type.to_string()));
    if let Some(account) = &event.account {
        headers.insert("account".into(), HeaderValue::String(account.to_string()));
    }
    if let Some(region) = &event.region {
        headers.insert("region".into(), HeaderValue::String(region.to_string()));
    }
    if !event.resources.is_empty() {
        headers.insert("resources".into(), HeaderValue::String(event.resources.join(",")));
    }
    Some(Payload {
        data: share(body, event.detail.get()),
        content_type: Some(JSON_CONTENT_TYPE.to_string()),
        message_type: Some(message_type(&event.source, &event.detail_type, event.detail.get())),
        id: Some(event.id.to_string()).filter(|id| !id.is_empty()),
        time: parse_timestamp(&event.time),
        source: Some(event.source.into_owned()),
        subject: None,
        data_schema: None,
        headers,
    })
}
