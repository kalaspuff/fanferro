//! ARN and queue URL helpers.

use crate::error::{Error, Result};

/// A parsed Amazon Resource Name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Arn {
    /// Partition, e.g. `aws`, `aws-cn`, `aws-us-gov`.
    pub partition: String,
    /// Service, e.g. `sqs` or `sns`.
    pub service: String,
    /// Region.
    pub region: String,
    /// Account id.
    pub account: String,
    /// Resource part (everything after the account).
    pub resource: String,
}

impl Arn {
    /// Parses an ARN of the form `arn:partition:service:region:account:resource`.
    pub fn parse(arn: &str) -> Result<Self> {
        let mut parts = arn.splitn(6, ':');
        let (Some("arn"), Some(partition), Some(service), Some(region), Some(account), Some(resource)) =
            (parts.next(), parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(Error::validation(format!("invalid ARN {arn:?}")));
        };
        if partition.is_empty() || service.is_empty() || resource.is_empty() {
            return Err(Error::validation(format!("invalid ARN {arn:?}")));
        }
        Ok(Self {
            partition: partition.into(),
            service: service.into(),
            region: region.into(),
            account: account.into(),
            resource: resource.into(),
        })
    }

    /// The last segment of the resource (e.g. the queue or topic name).
    pub fn resource_name(&self) -> &str {
        self.resource.rsplit([':', '/']).next().unwrap_or(&self.resource)
    }
}

impl std::fmt::Display for Arn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arn:{}:{}:{}:{}:{}", self.partition, self.service, self.region, self.account, self.resource)
    }
}

/// Returns the partition for a region (`aws`, `aws-cn`, `aws-us-gov`, `aws-eusc`, ...).
pub fn partition_for_region(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eu-isoe-") {
        "aws-iso-e"
    } else if region.starts_with("us-isof-") {
        "aws-iso-f"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// The DNS suffix of a partition's service endpoints (`amazonaws.com`, `amazonaws.com.cn`, ...).
pub fn dns_suffix(partition: &str) -> &'static str {
    match partition {
        "aws-cn" => "amazonaws.com.cn",
        "aws-eusc" => "amazonaws.eu",
        "aws-iso" => "c2s.ic.gov",
        "aws-iso-b" => "sc2s.sgov.gov",
        "aws-iso-e" => "cloud.adc-e.uk",
        "aws-iso-f" => "csp.hci.ic.gov",
        _ => "amazonaws.com",
    }
}

/// Builds an SNS topic ARN.
pub fn topic_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:{}:sns:{region}:{account}:{name}", partition_for_region(region))
}

/// Builds an SQS queue ARN.
pub fn queue_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:{}:sqs:{region}:{account}:{name}", partition_for_region(region))
}

/// Extracts `(account, queue name)` from a queue URL such as
/// `https://sqs.eu-west-1.amazonaws.com/123456789012/orders` or an emulator URL like
/// `http://localhost:4566/000000000000/orders`.
pub fn parse_queue_url(url: &str) -> Result<(String, String)> {
    let without_query = url.split(['?', '#']).next().unwrap_or(url);
    let mut segments = without_query.trim_end_matches('/').rsplit('/');
    let name = segments.next().unwrap_or_default();
    let account = segments.next().unwrap_or_default();
    if name.is_empty() || account.is_empty() || !without_query.contains("://") {
        return Err(Error::validation(format!("invalid queue URL {url:?}")));
    }
    Ok((account.to_string(), name.to_string()))
}

/// Returns the queue name of a queue URL.
pub fn queue_name_from_url(url: &str) -> Result<String> {
    parse_queue_url(url).map(|(_, name)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arns() {
        let arn = Arn::parse("arn:aws:sns:eu-west-1:123456789012:orders.fifo").unwrap();
        assert_eq!(arn.service, "sns");
        assert_eq!(arn.account, "123456789012");
        assert_eq!(arn.resource_name(), "orders.fifo");
        assert_eq!(arn.to_string(), "arn:aws:sns:eu-west-1:123456789012:orders.fifo");
        let sub = Arn::parse("arn:aws:sns:eu-west-1:123456789012:orders:3f1c-1").unwrap();
        assert_eq!(sub.resource, "orders:3f1c-1");
        assert!(Arn::parse("nope").is_err());
        assert!(Arn::parse("arn:aws:sns").is_err());
    }

    #[test]
    fn partitions() {
        assert_eq!(partition_for_region("eu-west-1"), "aws");
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
        assert_eq!(partition_for_region("us-gov-west-1"), "aws-us-gov");
        assert_eq!(topic_arn("cn-north-1", "1", "t"), "arn:aws-cn:sns:cn-north-1:1:t");
        assert_eq!(queue_arn("us-east-1", "1", "q"), "arn:aws:sqs:us-east-1:1:q");
        assert_eq!(partition_for_region("eusc-de-east-1"), "aws-eusc");
        assert_eq!(topic_arn("eusc-de-east-1", "1", "t"), "arn:aws-eusc:sns:eusc-de-east-1:1:t");
        let suffixes = [
            ("us-east-1", "amazonaws.com"),
            ("us-gov-west-1", "amazonaws.com"),
            ("cn-north-1", "amazonaws.com.cn"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
            ("us-isob-east-1", "sc2s.sgov.gov"),
            ("eu-isoe-west-1", "cloud.adc-e.uk"),
            ("us-isof-south-1", "csp.hci.ic.gov"),
        ];
        for (region, suffix) in suffixes {
            assert_eq!(dns_suffix(partition_for_region(region)), suffix, "{region}");
        }
    }

    #[test]
    fn parses_queue_urls() {
        assert_eq!(
            parse_queue_url("https://sqs.eu-west-1.amazonaws.com/123456789012/orders").unwrap(),
            ("123456789012".to_string(), "orders".to_string())
        );
        assert_eq!(
            parse_queue_url("http://localhost:4566/000000000000/orders.fifo/").unwrap(),
            ("000000000000".to_string(), "orders.fifo".to_string())
        );
        assert!(parse_queue_url("orders").is_err());
    }
}
