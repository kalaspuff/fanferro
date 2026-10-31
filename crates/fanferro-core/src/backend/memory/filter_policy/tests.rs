//! The examples of the SNS documentation, plus validation and limits.

use super::*;
use crate::types::AttributeValue;

fn policy(text: &str) -> FilterPolicy {
    FilterPolicy::parse(text, FilterScope::MessageAttributes).unwrap_or_else(|error| panic!("{text}: {error}"))
}

fn body_policy(text: &str) -> FilterPolicy {
    FilterPolicy::parse(text, FilterScope::MessageBody).unwrap_or_else(|error| panic!("{text}: {error}"))
}

fn error(text: &str) -> String {
    FilterPolicy::parse(text, FilterScope::MessageBody).expect_err(text)
}

fn string(value: &str) -> AttributeValue {
    AttributeValue::string(value)
}

fn number(value: &str) -> AttributeValue {
    AttributeValue::number(value)
}

fn array(json: &str) -> AttributeValue {
    AttributeValue::with_type("String.Array", AttributeData::Text(json.into())).unwrap()
}

fn with(policy: &FilterPolicy, attributes: &[(&str, AttributeValue)]) -> bool {
    let attributes = attributes.iter().map(|(name, value)| ((*name).to_string(), value.clone())).collect();
    policy.matches(&attributes, "")
}

fn body(policy: &FilterPolicy, body: &str) -> bool {
    policy.matches(&MessageAttributes::new(), body)
}

#[test]
fn exact_string_matching() {
    let p = policy(r#"{"customer_interests": ["rugby", "tennis"]}"#);
    assert!(with(&p, &[("customer_interests", string("rugby"))]));
    assert!(with(&p, &[("customer_interests", string("tennis"))]));
    assert!(with(&p, &[("customer_interests", array(r#"["rugby", "tennis"]"#))]));
    assert!(!with(&p, &[("customer_interests", string("baseball"))]));
    assert!(!with(&p, &[("customer_interests", array(r#"["baseball"]"#))]));
    assert!(!with(&p, &[("customer_interests", string("Rugby"))]), "case-sensitive");
    let p = body_policy(r#"{"customer_interests": ["rugby", "tennis"]}"#);
    assert!(body(&p, r#"{"customer_interests": "rugby"}"#));
    assert!(!body(&p, r#"{"customer_interests": "baseball"}"#));
}

#[test]
fn anything_but_matching() {
    let p = policy(r#"{"customer_interests": [{"anything-but": ["rugby", "tennis"]}]}"#);
    assert!(with(&p, &[("customer_interests", string("baseball"))]));
    assert!(with(&p, &[("customer_interests", array(r#"["rugby", "baseball"]"#))]));
    assert!(!with(&p, &[("customer_interests", string("rugby"))]));
    assert!(!with(&p, &[("customer_interests", array(r#"["rugby"]"#))]));
    assert!(!with(&p, &[("other", string("x"))]), "the key must be present");
    let p = body_policy(r#"{"customer_interests": [{"anything-but": ["rugby", "tennis"]}]}"#);
    assert!(body(&p, r#"{"customer_interests": ["rugby", "baseball"]}"#));
    assert!(!body(&p, r#"{"customer_interests": ["rugby"]}"#));
    let p = policy(r#"{"event": [{"anything-but": {"prefix": "order-"}}]}"#);
    assert!(with(&p, &[("event", string("data-entry"))]));
    assert!(with(&p, &[("event", string("order_number"))]));
    assert!(!with(&p, &[("event", string("order-cancelled"))]));
    for exclusion in [r#"{"wildcard": "*ball"}"#, r#"{"suffix": "ball"}"#] {
        let p = body_policy(&format!(r#"{{"customer_interests": [{{"anything-but": {exclusion}}}]}}"#));
        assert!(body(&p, r#"{"customer_interests": ["hockey", "rugby", "soccer"]}"#));
        assert!(!body(&p, r#"{"customer_interests": ["baseball", "basketball"]}"#));
    }
    let p = policy(r#"{"tier": [{"anything-but": "free"}]}"#);
    assert!(with(&p, &[("tier", string("pro"))]) && !with(&p, &[("tier", string("free"))]));
}

#[test]
fn equals_ignore_case_prefix_and_suffix() {
    let p = policy(r#"{"customer_interests": [{"equals-ignore-case": "tennis"}]}"#);
    assert!(with(&p, &[("customer_interests", string("TENNIS"))]));
    assert!(with(&p, &[("customer_interests", string("Tennis"))]));
    assert!(!with(&p, &[("customer_interests", string("tennis!"))]));
    let p = policy(r#"{"customer_interests": [{"prefix": "bas"}]}"#);
    assert!(with(&p, &[("customer_interests", string("baseball"))]));
    assert!(!with(&p, &[("customer_interests", string("rugby"))]));
    let p = body_policy(r#"{"customer_interests": [{"suffix": "ball"}]}"#);
    assert!(body(&p, r#"{"customer_interests": "basketball"}"#));
    assert!(!body(&p, r#"{"customer_interests": "rugby"}"#));
}

#[test]
fn ip_address_matching() {
    let p = policy(r#"{"source_ip": [{"cidr": "10.0.0.0/24"}]}"#);
    assert!(with(&p, &[("source_ip", string("10.0.0.0"))]));
    assert!(with(&p, &[("source_ip", string("10.0.0.255"))]));
    assert!(!with(&p, &[("source_ip", string("10.1.1.0"))]));
    assert!(!with(&p, &[("source_ip", string("not an address"))]));
    let p = body_policy(r#"{"source_ip": [{"cidr": "2001:db8::/32"}]}"#);
    assert!(body(&p, r#"{"source_ip": "2001:db8::1"}"#));
    assert!(!body(&p, r#"{"source_ip": "2001:db9::1"}"#));
    assert!(!body(&p, r#"{"source_ip": "10.0.0.1"}"#), "another address family");
    let p = policy(r#"{"source_ip": [{"cidr": "192.168.1.7"}]}"#);
    assert!(with(&p, &[("source_ip", string("192.168.1.7"))]) && !with(&p, &[("source_ip", string("192.168.1.8"))]));
}

#[test]
fn wildcard_matching() {
    let p = body_policy(r#"{"customer_interests": [{"wildcard": "*ball"}]}"#);
    assert!(body(&p, r#"{"customer_interests": ["baseball", "basketball"]}"#));
    assert!(!body(&p, r#"{"customer_interests": "rugby"}"#));
    let cases = [
        ("*", "", true),
        ("*", "anything", true),
        ("a*", "abc", true),
        ("*c", "abc", true),
        ("a*b*c", "a-b-c", true),
        ("a*b*c", "a-c-b", false),
        ("a*ab", "aab", true),
        ("ab*ba", "aba", false),
        ("abc", "abc", true),
        ("abc", "abcd", false),
        (r"\\*literal", "*literal", true),
        (r"\\*literal", "xliteral", false),
        (r"a\\\\b", r"a\b", true),
        ("ü*ö", "üßö", true),
    ];
    for (pattern, text, expected) in cases {
        let p = body_policy(&format!(r#"{{"k": [{{"wildcard": "{pattern}"}}]}}"#));
        assert_eq!(body(&p, &serde_json::json!({ "k": text }).to_string()), expected, "{pattern:?} on {text:?}");
    }
}

#[test]
fn numeric_matching() {
    let p = policy(r#"{"price_usd": [{"numeric": ["=", 301.5]}]}"#);
    assert!(with(&p, &[("price_usd", number("301.5"))]));
    assert!(with(&p, &[("price_usd", number("3.015e2"))]));
    let p = body_policy(r#"{"price_usd": [{"numeric": ["=", 301.5]}]}"#);
    assert!(body(&p, r#"{"price_usd": 3.015e2}"#));
    let p = policy(r#"{"price": [{"anything-but": [100, 500]}]}"#);
    assert!(with(&p, &[("price", number("101"))]));
    assert!(with(&p, &[("price", number("100.1"))]));
    assert!(with(&p, &[("price", array("[100, 50]"))]));
    assert!(!with(&p, &[("price", number("100"))]));
    let p = body_policy(r#"{"price": [{"anything-but": [100, 500]}]}"#);
    assert!(body(&p, r#"{"price": [100, 50]}"#) && !body(&p, r#"{"price": 100}"#));
    let p = policy(r#"{"price_usd": [{"numeric": [">", 0, "<=", 150]}]}"#);
    assert!(with(&p, &[("price_usd", number("150"))]));
    assert!(with(&p, &[("price_usd", number("0.00001"))]));
    assert!(!with(&p, &[("price_usd", number("0"))]));
    assert!(!with(&p, &[("price_usd", number("150.00001"))]));
    let p = policy(r#"{"price_usd": [{"numeric": ["<", 0]}]}"#);
    assert!(with(&p, &[("price_usd", number("-3"))]) && !with(&p, &[("price_usd", number("0"))]));
    // Five decimals: 1.000001 is 1, and 300 == 300.0 == 3e2.
    let p = policy(r#"{"n": [{"numeric": ["=", 1.000001]}], "m": [300]}"#);
    assert!(with(&p, &[("n", number("1")), ("m", number("3e2"))]));
    assert!(!with(&policy(r#"{"n": [{"numeric": ["=", 1.00001]}]}"#), &[("n", number("1"))]));
    assert!(!with(&p, &[("n", number("1e10")), ("m", number("300"))]), "beyond 1e9 nothing matches");
}

#[test]
fn values_match_by_type() {
    // Numbers match Number attributes and JSON numbers, strings match strings: no conversions.
    let p = policy(r#"{"n": [5]}"#);
    assert!(with(&p, &[("n", number("5.0"))]));
    assert!(!with(&p, &[("n", string("5"))]));
    assert!(!with(&policy(r#"{"n": ["5"]}"#), &[("n", number("5"))]));
    assert!(!with(&policy(r#"{"n": [{"numeric": [">", 1]}]}"#), &[("n", string("5"))]));
    assert!(!with(&policy(r#"{"n": [{"prefix": "5"}]}"#), &[("n", number("5"))]));
    let p = body_policy(r#"{"n": [5], "flag": [true], "gone": [null]}"#);
    assert!(body(&p, r#"{"n": 5.0, "flag": true, "gone": null}"#));
    assert!(!body(&p, r#"{"n": "5", "flag": true, "gone": null}"#));
    assert!(!body(&p, r#"{"n": 5, "flag": "true", "gone": null}"#));
    let p = policy(r#"{"tags": [true, 7]}"#);
    assert!(with(&p, &[("tags", array(r#"["x", true]"#))]));
    assert!(with(&p, &[("tags", array("[7]"))]));
    assert!(!with(&p, &[("tags", array(r#"["true", "7"]"#))]));
}

#[test]
fn key_matching() {
    let p = policy(r#"{"store": [{"exists": true}]}"#);
    assert!(with(&p, &[("store", string("fans")), ("customer_interests", array(r#"["baseball"]"#))]));
    assert!(!with(&p, &[("customer_interests", array(r#"["baseball"]"#))]));
    let p = policy(r#"{"store": [{"exists": false}]}"#);
    assert!(!with(&p, &[("store", string("fans")), ("customer_interests", array(r#"["baseball"]"#))]));
    assert!(with(&p, &[("customer_interests", array(r#"["baseball"]"#))]));
    assert!(!with(&p, &[]), "a message without attributes matches no policy");
    assert!(with(&p, &[("store", AttributeValue::binary(&b"x"[..])), ("other", string("y"))]), "Binary is ignored");
    assert!(!with(&p, &[("store", AttributeValue::binary(&b"x"[..]))]));
    let p = body_policy(r#"{"store": [{"exists": true}]}"#);
    assert!(body(&p, r#"{"store": "fans", "customer_interests": ["baseball", "basketball"]}"#));
    assert!(!body(&p, r#"{"customer_interests": ["baseball", "basketball"]}"#));
    assert!(!body(&p, r#"{"store": null}"#) && !body(&p, r#"{"store": ""}"#) && !body(&p, r#"{"store": []}"#));
    assert!(!body(&p, r#"{"store": {"id": 1}}"#), "exists only sees leaves");
    let p = body_policy(r#"{"store": [{"exists": false}]}"#);
    assert!(body(&p, r#"{"customer_interests": ["baseball"]}"#));
    assert!(!body(&p, r#"{"store": "fans"}"#));
    assert!(!body(&p, "{}") && !body(&p, "not json"));
    let p = policy(r#"{"tier": [{"anything-but": "free"}, {"exists": false}]}"#);
    assert!(with(&p, &[("other", string("x"))]) && with(&p, &[("tier", string("pro"))]));
    assert!(!with(&p, &[("tier", string("free"))]));
}

#[test]
fn and_or_logic() {
    let p = policy(r#"{"customer_interests": ["rugby"], "price_usd": [{"numeric": [">", 100]}]}"#);
    assert!(with(&p, &[("customer_interests", string("rugby")), ("price_usd", number("101"))]));
    assert!(!with(&p, &[("customer_interests", string("rugby")), ("price_usd", number("100"))]));
    let p = policy(
        r#"{"source": ["aws.cloudwatch"], "$or": [{"metricName": ["CPUUtilization"]}, {"namespace": ["AWS/EC2"]}]}"#,
    );
    assert!(with(&p, &[("source", string("aws.cloudwatch")), ("metricName", string("CPUUtilization"))]));
    assert!(with(&p, &[("source", string("aws.cloudwatch")), ("namespace", string("AWS/EC2"))]));
    assert!(!with(&p, &[("source", string("aws.cloudwatch")), ("namespace", string("AWS/ES"))]));
    assert!(!with(&p, &[("metricName", string("CPUUtilization"))]));
    // Nested $or alternatives, also inside nested objects of the body.
    let p = body_policy(
        r#"{"$or": [{"metricName": ["CPUUtilization", "ReadLatency"]}, {"namespace": ["AWS/EC2", "AWS/ES"]}],
            "detail": {"scope": ["Service"], "$or": [{"source": ["aws.cloudwatch"]}, {"type": ["Alarm"]}]}}"#,
    );
    assert!(body(&p, r#"{"namespace": "AWS/ES", "detail": {"scope": "Service", "type": "Alarm"}}"#));
    assert!(!body(&p, r#"{"namespace": "AWS/ES", "detail": {"scope": "Service", "type": "Other"}}"#));
    assert!(!body(&p, r#"{"namespace": "AWS/ES", "detail": {"scope": "Other", "source": "aws.cloudwatch"}}"#));
    // A $or of operators, or of fewer than two objects, is an ordinary key.
    let p = body_policy(r#"{"$or": [{"numeric": [">", 1]}, {"prefix": "abc"}]}"#);
    assert!(body(&p, r#"{"$or": "abcdef"}"#) && body(&p, r#"{"$or": 2}"#) && !body(&p, r#"{"$or": 1}"#));
    assert!(error(r#"{"$or": [{"metricName": ["CPUUtilization"]}]}"#).contains("Unrecognized match type metricName"));
}

#[test]
fn nested_keys_and_arrays_of_objects() {
    let p = body_policy(r#"{"order": {"status": ["paid"], "total": [{"numeric": [">=", 100]}]}}"#);
    assert!(body(&p, r#"{"order": {"status": "paid", "total": 150}}"#));
    assert!(!body(&p, r#"{"order": {"status": "open", "total": 150}}"#));
    assert!(!body(&p, r#"{"order": "paid"}"#));
    // One element of an array matches all of a nested policy.
    let p = body_policy(r#"{"employees": {"firstName": ["Anna"], "lastName": ["Smith"]}}"#);
    assert!(body(
        &p,
        r#"{"employees": [{"firstName": "John", "lastName": "Doe"}, {"firstName": "Anna", "lastName": "Smith"}]}"#
    ));
    assert!(!body(
        &p,
        r#"{"employees": [{"firstName": "Anna", "lastName": "Doe"}, {"firstName": "John", "lastName": "Smith"}]}"#
    ));
    let p = body_policy(r#"{"detail": {"error": [{"exists": false}]}}"#);
    assert!(body(&p, r#"{"detail": {"ok": true}}"#) && body(&p, r#"{"other": 1}"#));
    assert!(!body(&p, r#"{"detail": {"error": "boom"}}"#));
}

#[test]
fn invalid_policies_are_refused() {
    let cases = [
        ("[]", "must be a JSON object"),
        ("not json", "failed to parse JSON. "),
        (r#"{"a": "x"}"#, "match values must be in an array"),
        (r#"{"a": []}"#, "Empty arrays are not allowed"),
        (r#"{"a": [["x"]]}"#, "match values must be strings"),
        (r#"{"a": [{"foo": "x"}]}"#, "Unrecognized match type foo"),
        (r#"{"a": [{"prefix": "x", "suffix": "y"}]}"#, "exactly one operator"),
        (r#"{"a": [{"prefix": 1}]}"#, "prefix takes a string"),
        (r#"{"a": [{"exists": "yes"}]}"#, "exists match pattern must be either true or false."),
        (r#"{"a": [{"numeric": ["<", 5, ">", 1]}]}"#, "a numeric range starts with > or >="),
        (r#"{"a": [{"numeric": [">", 5, "<", 1]}]}"#, "lower bound"),
        (r#"{"a": [{"numeric": ["~", 1]}]}"#, "unrecognized numeric operator"),
        (r#"{"a": [{"numeric": 5}]}"#, "numeric takes"),
        (r#"{"a": [{"numeric": ["=", 1e10]}]}"#, "numbers must be between"),
        (r#"{"a": [{"numeric": ["=", "5"]}]}"#, "Value of = must be numeric"),
        (r#"{"a": [-2e9]}"#, "numbers must be between"),
        (r#"{"a": [{"anything-but": ["x", 1]}]}"#, "only strings or only numbers"),
        (r#"{"a": [{"anything-but": []}]}"#, "only strings or only numbers"),
        (r#"{"a": [{"anything-but": {"equals-ignore-case": "x"}}]}"#, "does not support equals-ignore-case"),
        (r#"{"a": [{"cidr": "10.0.0.0/33"}]}"#, "malformed CIDR"),
        (r#"{"a": [{"cidr": "10.0.0/8"}]}"#, "malformed CIDR"),
        (r#"{"a": [{"wildcard": "a**b"}]}"#, "consecutive wildcards"),
        (r#"{"a": [{"wildcard": "a\\b"}]}"#, "may be escaped"),
        (r#"{"a": [{"wildcard": "*a*b*c*"}]}"#, "at most 3 wildcards"),
        (r#"{"a": ["1"], "b": ["1"], "c": ["1"], "d": ["1"], "e": ["1"], "f": ["1"]}"#, "more than 5 keys"),
    ];
    for (text, expected) in cases {
        let error = error(text);
        assert!(error.contains(expected), "{text}: {error:?} should contain {expected:?}");
    }
    let nested = FilterPolicy::parse(r#"{"a": {"b": ["x"]}}"#, FilterScope::MessageAttributes).unwrap_err();
    assert_eq!(nested, NESTED_IN_ATTRIBUTE_SCOPE);
    // SNS's own texts (docs/parity S5) stand alone; ours name the key.
    assert_eq!(error(r#"{"a": [{"glob": "x"}]}"#), "Unrecognized match type glob");
    assert_eq!(error(r#"{"a": [{"prefix": 1}]}"#), "\"a\": prefix takes a string");
    let large = format!(r#"{{"a": ["{}"]}}"#, "x".repeat(MAX_POLICY_BYTES));
    assert!(error(&large).contains("larger than 256 KB"));
}

#[test]
fn complexity_limits() {
    // The examples of the documentation: 6, 72, 7 and 32 combinations.
    body_policy(r#"{"key_a": ["1", "2", "3"], "key_b": ["1"], "key_c": ["1", "2"]}"#);
    body_policy(r#"{"key_a": {"key_b": {"key_c": ["1", "2", "3", "4"]}}, "key_d": {"key_e": ["1", "2", "3"]}}"#);
    body_policy(
        r#"{"source": ["aws.cloudwatch"], "$or": [{"metricName": ["CPUUtilization", "ReadLatency"]},
            {"metricType": ["MetricType"], "$or": [{"metricId": [1234, 4321]}, {"spaceId": [1000, 2000, 3000]}]}]}"#,
    );
    let values = |count: usize| (0..count).map(|value| format!("\"{value}\"")).collect::<Vec<_>>().join(", ");
    body_policy(&format!(r#"{{"a": [{}], "b": [{}]}}"#, values(15), values(10)));
    let error = error(&format!(r#"{{"a": [{}], "b": [{}]}}"#, values(16), values(10)));
    assert_eq!(error, "Filter policy is too complex", "160 combinations");
    // The nesting level counts: 75 values two levels deep are 150 combinations, 76 are 152.
    body_policy(&format!(r#"{{"a": {{"b": [{}]}}}}"#, values(75)));
    let error = self::error(&format!(r#"{{"a": {{"b": [{}]}}}}"#, values(76)));
    assert_eq!(error, "Filter policy is too complex", "152 combinations");
    // Wildcards: 4 points for two single-wildcard patterns; 3 points per wildcard beyond one.
    body_policy(r#"{"filename": [{"wildcard": "*.txt"}, {"wildcard": "log*"}]}"#);
    body_policy(r#"{"greeting": [{"anything-but": {"prefix": "Hello"}}, {"wildcard": "H*"}]}"#);
    let patterns = (0..4).map(|index| format!(r#"{{"wildcard": "*{index}*{index}*"}}"#)).collect::<Vec<_>>().join(", ");
    let error = self::error(&format!(r#"{{"name": [{patterns}]}}"#));
    assert!(error.contains("wildcard complexity 144"), "{error}");
}
