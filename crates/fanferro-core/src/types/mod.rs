//! Domain types shared by every layer: attributes, payloads, wire messages, ARNs.

pub mod arn;
pub mod attributes;
pub mod message;
pub mod reserved;
pub mod text;

pub use attributes::{AttributeData, AttributeValue, BaseType, MessageAttributes};
pub use message::{HeaderValue, Headers, Payload, WireMessage, new_message_id};
