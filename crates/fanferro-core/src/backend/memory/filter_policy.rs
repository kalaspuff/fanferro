//! SNS subscription filter policies for the in-memory backend, with the semantics and limits of
//! SNS (<https://docs.aws.amazon.com/sns/latest/dg/sns-subscription-filter-policies.html>).
//!
//! A policy is validated when it is set, as SNS does (`InvalidParameter`): its operators and
//! their arguments, nesting only for the `MessageBody` scope, at most five keys, 150 combinations
//! of values and 100 points of wildcard complexity. Matching supports exact strings, numbers,
//! `true`, `false` and `null`; `prefix`, `suffix`, `equals-ignore-case`, `wildcard` and `cidr`;
//! `anything-but` (values, `prefix`, `suffix`, `wildcard`); `numeric` (`=` and ranges, with five
//! decimals between -1e9 and 1e9); `exists`; `$or`; and, for the `MessageBody` scope, nested keys
//! and arrays (an array matches when one of its elements does). Attributes match by their type:
//! `String` values as strings, `Number` values as numbers, `String.Array` values element by
//! element; `Binary` attributes are ignored.

use std::borrow::Cow;
use std::net::IpAddr;

use serde_json::{Map, Value};

use crate::types::{AttributeData, AttributeValue, BaseType, MessageAttributes};

/// Where a filter policy is evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterScope {
    /// Message attributes (default).
    MessageAttributes,
    /// The JSON message body.
    MessageBody,
}

impl FilterScope {
    /// The scope a `FilterPolicyScope` attribute names (`MessageAttributes` when unset).
    pub fn from_attribute(value: Option<&str>) -> Self {
        match value {
            Some("MessageBody") => Self::MessageBody,
            _ => Self::MessageAttributes,
        }
    }
}

/// The most keys a policy may have (leaf keys of nested policies, in each `$or` alternative).
const MAX_KEYS: usize = 5;
/// The most combinations of values: the sum over `$or` alternatives of the product, over their
/// keys, of the number of values times the nesting level.
const MAX_COMBINATIONS: usize = 150;
/// The most wildcard complexity points, summed over the fields that use wildcards.
const MAX_WILDCARD_COMPLEXITY: usize = 100;
/// The most wildcards in one pattern.
const MAX_WILDCARDS: usize = 3;
/// The largest policy, in bytes.
const MAX_POLICY_BYTES: usize = 256 * 1024;
/// Numbers are compared between -1e9 and 1e9, with five decimals.
const MAX_NUMBER: f64 = 1e9;
const NUMBER_SCALE: f64 = 1e5;
/// A `$or` whose objects use one of these as a key is an ordinary key, as in SNS.
const OPERATORS: [&str; 8] =
    ["anything-but", "cidr", "equals-ignore-case", "exists", "numeric", "prefix", "suffix", "wildcard"];

/// A validated filter policy.
#[derive(Clone, Debug)]
pub struct FilterPolicy {
    scope: FilterScope,
    root: Node,
}

/// One level of a policy: every field matches, and one alternative of each `$or`.
#[derive(Clone, Debug, Default)]
struct Node {
    fields: Vec<(String, Field)>,
    any_of: Vec<Vec<Node>>,
}

#[derive(Clone, Debug)]
enum Field {
    /// Matches when one of the conditions does.
    Conditions(Vec<Condition>),
    /// A nested object (`MessageBody` scope).
    Nested(Node),
}

#[derive(Clone, Debug)]
enum Condition {
    Equals(Literal),
    Prefix(String),
    Suffix(String),
    /// Lowercase.
    EqualsIgnoreCase(String),
    Wildcard(Pattern),
    Cidr(Cidr),
    AnythingBut(Exclusion),
    Numeric(Range),
    Exists(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Literal {
    String(String),
    /// Scaled by [`NUMBER_SCALE`].
    Number(i64),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug)]
enum Exclusion {
    Values(Vec<Literal>),
    Prefix(String),
    Suffix(String),
    Wildcard(Pattern),
}

/// Bounds scaled by [`NUMBER_SCALE`], each with whether it is inclusive.
#[derive(Clone, Copy, Debug)]
struct Range {
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
}

/// The literal parts of a wildcard pattern, between its wildcards.
#[derive(Clone, Debug)]
struct Pattern {
    parts: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
struct Cidr {
    network: IpAddr,
    prefix: u32,
}

/// A value of a message that conditions are matched against.
#[derive(Clone, Debug)]
enum Leaf<'a> {
    String(Cow<'a, str>),
    /// Scaled by [`NUMBER_SCALE`]; `None` outside the range SNS compares.
    Number(Option<i64>),
    Bool(bool),
    Null,
}

impl FilterPolicy {
    /// Validates a policy (the `FilterPolicy` attribute) for `scope`; the error says why SNS would
    /// refuse it.
    pub fn parse(policy: &str, scope: FilterScope) -> Result<Self, String> {
        if policy.len() > MAX_POLICY_BYTES {
            return Err(format!("Filter policy is larger than {} KB", MAX_POLICY_BYTES / 1024));
        }
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(policy) else {
            return Err("Filter policy must be a JSON object".into());
        };
        let root = node(&object, scope)?;
        check_limits(&root)?;
        Ok(Self { scope, root })
    }

    /// Whether a message with these attributes and body passes the policy.
    pub fn matches(&self, attributes: &MessageAttributes, body: &str) -> bool {
        if self.root.is_empty() {
            return true;
        }
        match self.scope {
            // A message without attributes matches no policy, not even `"exists": false`.
            FilterScope::MessageAttributes => {
                attributes.values().any(|attribute| !matches!(attribute.data(), AttributeData::Binary(_)))
                    && self.root.matches(&Context::Attributes(attributes))
            }
            // The body must be a JSON object; an empty one is like a message without attributes.
            FilterScope::MessageBody => match serde_json::from_str::<Value>(body) {
                Ok(Value::Object(object)) => !object.is_empty() && self.root.matches(&Context::Object(Some(&object))),
                _ => false,
            },
        }
    }
}

// -- validation ------------------------------------------------------------------------------

fn node(object: &Map<String, Value>, scope: FilterScope) -> Result<Node, String> {
    let mut node = Node::default();
    for (key, value) in object {
        if key == "$or"
            && let Some(alternatives) = or_alternatives(value)
        {
            let alternatives =
                alternatives.into_iter().map(|alternative| self::node(alternative, scope)).collect::<Result<_, _>>()?;
            node.any_of.push(alternatives);
            continue;
        }
        let field = match value {
            Value::Array(conditions) => Field::Conditions(conditions_of(key, conditions)?),
            Value::Object(nested) if scope == FilterScope::MessageBody => Field::Nested(self::node(nested, scope)?),
            Value::Object(_) => {
                return Err(format!(
                    "\"{key}\": nested filter policies need the FilterPolicyScope MessageBody (attributes are not nested)"
                ));
            }
            _ => return Err(format!("\"{key}\": match values must be in an array, e.g. [\"value\"]")),
        };
        node.fields.push((key.clone(), field));
    }
    Ok(node)
}

/// The alternatives of a `$or` that SNS reads as the operator: an array of at least two objects
/// that do not use operators as keys. Any other `$or` is an ordinary key.
fn or_alternatives(value: &Value) -> Option<Vec<&Map<String, Value>>> {
    let Value::Array(items) = value else { return None };
    let objects: Vec<&Map<String, Value>> = items.iter().filter_map(Value::as_object).collect();
    let is_operator = objects.len() == items.len()
        && objects.len() >= 2
        && objects.iter().all(|object| object.keys().all(|key| !OPERATORS.contains(&key.as_str())));
    is_operator.then_some(objects)
}

fn conditions_of(key: &str, conditions: &[Value]) -> Result<Vec<Condition>, String> {
    if conditions.is_empty() {
        return Err(format!("\"{key}\": empty arrays are not allowed"));
    }
    conditions
        .iter()
        .map(|condition| self::condition(condition).map_err(|error| format!("\"{key}\": {error}")))
        .collect()
}

fn condition(value: &Value) -> Result<Condition, String> {
    let Value::Object(operator) = value else { return literal(value).map(Condition::Equals) };
    let mut entries = operator.iter();
    let (Some((name, argument)), None) = (entries.next(), entries.next()) else {
        return Err("a match object must have exactly one operator, e.g. {\"prefix\": \"abc\"}".into());
    };
    Ok(match name.as_str() {
        "prefix" => Condition::Prefix(string_argument(name, argument)?.to_string()),
        "suffix" => Condition::Suffix(string_argument(name, argument)?.to_string()),
        "equals-ignore-case" => Condition::EqualsIgnoreCase(string_argument(name, argument)?.to_lowercase()),
        "wildcard" => Condition::Wildcard(Pattern::parse(string_argument(name, argument)?)?),
        "cidr" => Condition::Cidr(Cidr::parse(string_argument(name, argument)?)?),
        "numeric" => Condition::Numeric(Range::parse(argument)?),
        "exists" => match argument {
            Value::Bool(expected) => Condition::Exists(*expected),
            _ => return Err("exists takes true or false".into()),
        },
        "anything-but" => Condition::AnythingBut(Exclusion::parse(argument)?),
        other => return Err(format!("unrecognized match type {other}")),
    })
}

fn literal(value: &Value) -> Result<Literal, String> {
    Ok(match value {
        Value::String(text) => Literal::String(text.clone()),
        Value::Number(_) => Literal::Number(policy_number(value)?),
        Value::Bool(flag) => Literal::Bool(*flag),
        Value::Null => Literal::Null,
        _ => return Err("match values must be strings, numbers, true, false, null or match objects".into()),
    })
}

fn string_argument<'a>(operator: &str, argument: &'a Value) -> Result<&'a str, String> {
    argument.as_str().ok_or_else(|| format!("{operator} takes a string"))
}

/// A number of the policy, scaled; SNS compares numbers between -1e9 and 1e9.
fn policy_number(value: &Value) -> Result<i64, String> {
    let Some(number) = value.as_f64() else { return Err(format!("expected a number, got {value}")) };
    scaled(number).ok_or_else(|| format!("numbers must be between -{MAX_NUMBER:e} and {MAX_NUMBER:e}, got {value}"))
}

/// A number scaled by [`NUMBER_SCALE`] (five decimals), or `None` outside the compared range.
#[allow(clippy::cast_possible_truncation)] // within ±1e9, the scaled value is below 2^53
fn scaled(number: f64) -> Option<i64> {
    (number.is_finite() && number.abs() <= MAX_NUMBER).then(|| (number * NUMBER_SCALE).round() as i64)
}

impl Range {
    /// `["=", n]`, `["<" | "<=", n]`, `[">" | ">=", n]` or `[">" | ">=", low, "<" | "<=", high]`.
    fn parse(argument: &Value) -> Result<Self, String> {
        const FORM: &str = "numeric takes [\"=\", n], [\">\", n, \"<=\", m] or a single comparison";
        let Value::Array(parts) = argument else { return Err(FORM.into()) };
        let pair = |index: usize| -> Result<(&str, i64), String> {
            let operator = parts[index].as_str().ok_or(FORM)?;
            Ok((operator, policy_number(&parts[index + 1])?))
        };
        match parts.len() {
            2 => match pair(0)? {
                ("=", value) => Ok(Self { lower: Some((value, true)), upper: Some((value, true)) }),
                (">", value) => Ok(Self { lower: Some((value, false)), upper: None }),
                (">=", value) => Ok(Self { lower: Some((value, true)), upper: None }),
                ("<", value) => Ok(Self { lower: None, upper: Some((value, false)) }),
                ("<=", value) => Ok(Self { lower: None, upper: Some((value, true)) }),
                (operator, _) => Err(format!("unrecognized numeric operator {operator:?}")),
            },
            4 => {
                let (low, low_value) = pair(0)?;
                let (high, high_value) = pair(2)?;
                if !matches!(low, ">" | ">=") || !matches!(high, "<" | "<=") {
                    return Err(format!(
                        "a numeric range starts with > or >= and ends with < or <=, not {low} and {high}"
                    ));
                }
                if low_value >= high_value {
                    return Err("the lower bound of a numeric range must be below its upper bound".into());
                }
                Ok(Self { lower: Some((low_value, low == ">=")), upper: Some((high_value, high == "<=")) })
            }
            _ => Err(FORM.into()),
        }
    }

    fn contains(self, value: i64) -> bool {
        let above = self.lower.is_none_or(|(bound, inclusive)| value > bound || (inclusive && value == bound));
        let below = self.upper.is_none_or(|(bound, inclusive)| value < bound || (inclusive && value == bound));
        above && below
    }
}

impl Exclusion {
    fn parse(argument: &Value) -> Result<Self, String> {
        match argument {
            Value::String(_) | Value::Number(_) => Ok(Self::Values(vec![literal(argument)?])),
            Value::Array(items) => {
                let all_strings = items.iter().all(Value::is_string);
                if items.is_empty() || !(all_strings || items.iter().all(Value::is_number)) {
                    return Err(
                        "anything-but takes a string, a number, or a list of only strings or only numbers".into()
                    );
                }
                Ok(Self::Values(items.iter().map(literal).collect::<Result<_, _>>()?))
            }
            Value::Object(operator) => {
                let mut entries = operator.iter();
                let (Some((name, value)), None) = (entries.next(), entries.next()) else {
                    return Err("anything-but takes one of prefix, suffix or wildcard".into());
                };
                match name.as_str() {
                    "prefix" => Ok(Self::Prefix(string_argument(name, value)?.to_string())),
                    "suffix" => Ok(Self::Suffix(string_argument(name, value)?.to_string())),
                    "wildcard" => Ok(Self::Wildcard(Pattern::parse(string_argument(name, value)?)?)),
                    other => Err(format!("anything-but does not support {other} (prefix, suffix or wildcard)")),
                }
            }
            _ => Err("anything-but takes a string, a number, a list, or prefix, suffix or wildcard".into()),
        }
    }

    fn allows(&self, value: &Leaf<'_>) -> bool {
        match self {
            Self::Values(excluded) => !excluded.iter().any(|literal| literal.equals(value)),
            Self::Prefix(prefix) => value.as_str().is_some_and(|text| !text.starts_with(prefix.as_str())),
            Self::Suffix(suffix) => value.as_str().is_some_and(|text| !text.ends_with(suffix.as_str())),
            Self::Wildcard(pattern) => value.as_str().is_some_and(|text| !pattern.matches(text)),
        }
    }
}

impl Pattern {
    /// `*` matches any characters; `\*` is a literal star and `\\` a backslash.
    fn parse(pattern: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut part = String::new();
        let mut chars = pattern.chars();
        let mut after_wildcard = false;
        while let Some(next) = chars.next() {
            if next == '*' {
                if after_wildcard {
                    return Err(format!("consecutive wildcards are not allowed: {pattern:?}"));
                }
                parts.push(std::mem::take(&mut part));
                after_wildcard = true;
                continue;
            }
            after_wildcard = false;
            if next == '\\' {
                match chars.next() {
                    Some(escaped @ ('*' | '\\')) => part.push(escaped),
                    _ => return Err(format!("only \\* and \\\\ may be escaped in a wildcard: {pattern:?}")),
                }
            } else {
                part.push(next);
            }
        }
        parts.push(part);
        if parts.len() - 1 > MAX_WILDCARDS {
            return Err(format!("a wildcard pattern may have at most {MAX_WILDCARDS} wildcards: {pattern:?}"));
        }
        Ok(Self { parts })
    }

    fn wildcards(&self) -> usize {
        self.parts.len() - 1
    }

    fn matches(&self, text: &str) -> bool {
        let [first, middle @ .., last] = self.parts.as_slice() else {
            return self.parts.first().is_some_and(|literal| literal.as_str() == text);
        };
        if text.len() < first.len() + last.len() || !text.starts_with(first.as_str()) || !text.ends_with(last.as_str())
        {
            return false;
        }
        let mut rest = &text[first.len()..text.len() - last.len()];
        for part in middle {
            match rest.find(part.as_str()) {
                Some(at) => rest = &rest[at + part.len()..],
                None => return false,
            }
        }
        true
    }
}

impl Cidr {
    fn parse(cidr: &str) -> Result<Self, String> {
        let malformed = || format!("malformed CIDR {cidr:?}");
        let (address, prefix) = cidr.split_once('/').map_or((cidr, None), |(address, prefix)| (address, Some(prefix)));
        let network: IpAddr = address.trim().parse().map_err(|_| malformed())?;
        let bits = if network.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(prefix) => prefix.trim().parse::<u32>().ok().filter(|&prefix| prefix <= bits).ok_or_else(malformed)?,
            None => bits,
        };
        Ok(Self { network, prefix })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = u32::MAX.checked_shl(32 - self.prefix).unwrap_or(0);
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = u128::MAX.checked_shl(128 - self.prefix).unwrap_or(0);
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

/// Checks the limits of SNS: keys and combinations per `$or` alternative, wildcard complexity.
fn check_limits(root: &Node) -> Result<(), String> {
    let mut branches = Vec::new();
    expand(root, 1, vec![Vec::new()], &mut branches)?;
    let mut combinations = 0usize;
    for keys in &branches {
        if keys.len() > MAX_KEYS {
            return Err(format!("Filter policy can not have more than {MAX_KEYS} keys"));
        }
        let product =
            keys.iter().fold(1usize, |product, &(values, depth)| product.saturating_mul(values.saturating_mul(depth)));
        combinations = combinations.saturating_add(product);
    }
    if combinations > MAX_COMBINATIONS {
        return Err(format!("Filter policy is too complex: {combinations} combinations (at most {MAX_COMBINATIONS})"));
    }
    let complexity = wildcard_complexity(root);
    if complexity > MAX_WILDCARD_COMPLEXITY {
        return Err(format!(
            "Filter policy is too complex: wildcard complexity {complexity} (at most {MAX_WILDCARD_COMPLEXITY})"
        ));
    }
    Ok(())
}

/// The leaf keys of every `$or` alternative of `node`, as (number of values, nesting level), each
/// branch extending one of `prefixes`.
fn expand(
    node: &Node,
    depth: usize,
    prefixes: Vec<Vec<(usize, usize)>>,
    branches: &mut Vec<Vec<(usize, usize)>>,
) -> Result<(), String> {
    let mut current = prefixes;
    for (_, field) in &node.fields {
        match field {
            Field::Conditions(conditions) => current.iter_mut().for_each(|keys| keys.push((conditions.len(), depth))),
            Field::Nested(nested) => {
                let mut extended = Vec::new();
                expand(nested, depth + 1, current, &mut extended)?;
                current = extended;
            }
        }
    }
    for alternatives in &node.any_of {
        let mut extended = Vec::new();
        for alternative in alternatives {
            expand(alternative, depth, current.clone(), &mut extended)?;
        }
        current = extended;
        // Every alternative adds at least one combination: past the limit, stop expanding.
        if current.len() > MAX_COMBINATIONS {
            return Err(format!("Filter policy is too complex: more than {MAX_COMBINATIONS} combinations"));
        }
    }
    branches.extend(current);
    Ok(())
}

/// Sum over the fields that use wildcards of (their pattern points) × (their number of patterns):
/// a pattern with one wildcard is 1 point, with more 3 points per wildcard, an anything-but 1.
fn wildcard_complexity(node: &Node) -> usize {
    let mut total = 0;
    for (_, field) in &node.fields {
        match field {
            Field::Conditions(conditions) => {
                let points = |pattern: &Pattern| {
                    if pattern.wildcards() > 1 { 3 * pattern.wildcards() } else { pattern.wildcards() }
                };
                let uses_wildcards = conditions.iter().any(|condition| {
                    matches!(condition, Condition::Wildcard(_) | Condition::AnythingBut(Exclusion::Wildcard(_)))
                });
                if uses_wildcards {
                    let sum: usize = conditions
                        .iter()
                        .map(|condition| match condition {
                            Condition::Wildcard(pattern) => points(pattern),
                            Condition::AnythingBut(Exclusion::Wildcard(pattern)) => 1 + points(pattern),
                            Condition::AnythingBut(_) => 1,
                            _ => 0,
                        })
                        .sum();
                    total += sum * conditions.len();
                }
            }
            Field::Nested(nested) => total += wildcard_complexity(nested),
        }
    }
    total + node.any_of.iter().flatten().map(wildcard_complexity).sum::<usize>()
}

// -- matching --------------------------------------------------------------------------------

/// Where the values of a key come from.
enum Context<'a> {
    Attributes(&'a MessageAttributes),
    /// An object of the body (`None`: absent, so are all its keys).
    Object(Option<&'a Map<String, Value>>),
}

impl Node {
    fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.any_of.is_empty()
    }

    fn matches(&self, context: &Context<'_>) -> bool {
        self.fields.iter().all(|(key, field)| match field {
            Field::Conditions(conditions) => {
                let values = context.values(key);
                conditions.iter().any(|condition| condition.matches(&values))
            }
            Field::Nested(node) => context.nested(key, node),
        }) && self.any_of.iter().all(|alternatives| alternatives.iter().any(|node| node.matches(context)))
    }
}

impl<'a> Context<'a> {
    /// The values of `key`: none when it is absent (or an object, which `exists` does not see).
    fn values(&self, key: &str) -> Vec<Leaf<'a>> {
        let mut values = Vec::new();
        match *self {
            Self::Attributes(attributes) => {
                if let Some(attribute) = attributes.get(key) {
                    attribute_values(attribute, &mut values);
                }
            }
            Self::Object(object) => {
                if let Some(value) = object.and_then(|object| object.get(key)) {
                    json_values(value, &mut values);
                }
            }
        }
        values
    }

    /// Whether the object at `key` matches a nested policy: one of the objects of an array may.
    fn nested(&self, key: &str, node: &Node) -> bool {
        let Self::Object(object) = *self else { return false };
        match object.and_then(|object| object.get(key)) {
            Some(Value::Object(inner)) => node.matches(&Context::Object(Some(inner))),
            Some(Value::Array(items)) if items.iter().any(Value::is_object) => {
                items.iter().filter_map(Value::as_object).any(|inner| node.matches(&Context::Object(Some(inner))))
            }
            _ => node.matches(&Context::Object(None)),
        }
    }
}

/// The values of an attribute, by its type (`Binary` attributes have none).
fn attribute_values<'a>(attribute: &'a AttributeValue, values: &mut Vec<Leaf<'a>>) {
    let AttributeData::Text(text) = attribute.data() else { return };
    if attribute.base_type() == BaseType::Number {
        values.push(Leaf::Number(text.trim().parse::<f64>().ok().and_then(scaled)));
    } else if attribute.data_type() == "String.Array"
        && let Ok(Value::Array(items)) = serde_json::from_str::<Value>(text)
    {
        values.extend(items.iter().filter_map(|item| Leaf::from_json(item).map(Leaf::into_owned)));
    } else {
        values.push(Leaf::String(Cow::Borrowed(text)));
    }
}

/// The scalar values of a body value; arrays are flattened, objects have none.
fn json_values<'a>(value: &'a Value, values: &mut Vec<Leaf<'a>>) {
    match value {
        Value::Array(items) => items.iter().for_each(|item| json_values(item, values)),
        other => values.extend(Leaf::from_json(other)),
    }
}

impl<'a> Leaf<'a> {
    fn from_json(value: &'a Value) -> Option<Self> {
        Some(match value {
            Value::String(text) => Self::String(Cow::Borrowed(text)),
            Value::Number(number) => Self::Number(number.as_f64().and_then(scaled)),
            Value::Bool(flag) => Self::Bool(*flag),
            Value::Null => Self::Null,
            Value::Array(_) | Value::Object(_) => return None,
        })
    }

    fn into_owned<'b>(self) -> Leaf<'b> {
        match self {
            Self::String(text) => Leaf::String(Cow::Owned(text.into_owned())),
            Self::Number(number) => Leaf::Number(number),
            Self::Bool(flag) => Leaf::Bool(flag),
            Self::Null => Leaf::Null,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(text) => Some(text.as_ref()),
            _ => None,
        }
    }

    /// Whether `exists` sees the value: not null and not an empty string.
    fn is_present(&self) -> bool {
        match self {
            Self::String(text) => !text.is_empty(),
            Self::Null => false,
            Self::Number(_) | Self::Bool(_) => true,
        }
    }
}

impl Literal {
    fn equals(&self, value: &Leaf<'_>) -> bool {
        match (self, value) {
            (Self::String(expected), Leaf::String(text)) => expected == text,
            (Self::Number(expected), Leaf::Number(Some(number))) => expected == number,
            (Self::Bool(expected), Leaf::Bool(flag)) => expected == flag,
            (Self::Null, Leaf::Null) => true,
            _ => false,
        }
    }
}

impl Condition {
    fn matches(&self, values: &[Leaf<'_>]) -> bool {
        match self {
            Self::Exists(expected) => values.iter().any(Leaf::is_present) == *expected,
            condition => values.iter().any(|value| condition.matches_value(value)),
        }
    }

    fn matches_value(&self, value: &Leaf<'_>) -> bool {
        match self {
            Self::Equals(literal) => literal.equals(value),
            Self::Prefix(prefix) => value.as_str().is_some_and(|text| text.starts_with(prefix.as_str())),
            Self::Suffix(suffix) => value.as_str().is_some_and(|text| text.ends_with(suffix.as_str())),
            Self::EqualsIgnoreCase(expected) => value.as_str().is_some_and(|text| text.to_lowercase() == *expected),
            Self::Wildcard(pattern) => value.as_str().is_some_and(|text| pattern.matches(text)),
            Self::Cidr(cidr) => {
                value.as_str().and_then(|text| text.parse().ok()).is_some_and(|address| cidr.contains(address))
            }
            Self::AnythingBut(exclusion) => exclusion.allows(value),
            Self::Numeric(range) => matches!(value, Leaf::Number(Some(number)) if range.contains(*number)),
            Self::Exists(_) => false,
        }
    }
}

#[cfg(test)]
mod tests;
