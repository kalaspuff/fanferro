"""Notifications as AWS documents them (examples of the S3 and SES developer guides), for tests."""

from __future__ import annotations

from typing import Any

S3_PUT = '{"Records":[{"eventVersion":"2.6","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":"1970-01-01T00:00:00.000Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AIDAJDPLRKLG7UEXAMPLE"},"requestParameters":{"sourceIPAddress":"172.16.0.1"},"responseElements":{"x-amz-request-id":"C3D13FE58DE4C810","x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"},"s3":{"s3SchemaVersion":"1.0","configurationId":"testConfigRule","bucket":{"name":"amzn-s3-demo-bucket","ownerIdentity":{"principalId":"A3NL1KOZZKExample"},"arn":"arn:aws:s3:::amzn-s3-demo-bucket"},"object":{"key":"HappyFace.jpg","size":1024,"eTag":"d41d8cd98f00b204e9800998ecf8427e","versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko","sequencer":"0055AED6DCD90281E5"}}}]}'  # noqa: E501 - verbatim
S3_TEST = '{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2014-10-13T15:57:02.089Z","Bucket":"amzn-s3-demo-bucket","RequestId":"5582815E1AEA5ADF","HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"}'  # noqa: E501 - verbatim
MAIL: dict[str, Any] = {
    "timestamp": "2018-10-08T14:05:45 +0000",
    "messageId": "000001378603177f-7a5433e7-8edb-42ae-af10-f0181f34d6ee-000000",
    "source": "sender@example.com",
    "sourceArn": "arn:aws:ses:us-east-1:888888888888:identity/example.com",
    "sourceIp": "127.0.3.0",
    "sendingAccountId": "123456789012",
    "destination": ["recipient@example.com"],
    "headersTruncated": False,
    "headers": [{"name": "From", "value": '"Sender Name" <sender@example.com>'}, {"name": "Subject", "value": "Hello"}],
    "commonHeaders": {
        "from": ["Sender Name <sender@example.com>"],
        "date": "Mon, 08 Oct 2018 14:05:45 +0000",
        "to": ["Recipient Name <recipient@example.com>"],
        "messageId": " custom-message-ID",
        "subject": "Message sent using SES",
    },
}
BOUNCE: dict[str, Any] = {
    "bounceType": "Permanent",
    "bounceSubType": "General",
    "bouncedRecipients": [
        {
            "status": "5.0.0",
            "action": "failed",
            "diagnosticCode": "smtp; 550 user unknown",
            "emailAddress": "recipient1@example.com",
        },
        {"status": "4.0.0", "action": "delayed", "emailAddress": "recipient2@example.com"},
    ],
    "reportingMTA": "example.com",
    "timestamp": "2012-05-25T14:59:38.605Z",
    "feedbackId": "000001378603176d-5a4b5ad9-6f30-4198-a8c3-b1eb0c270a1d-000000",
    "remoteMtaIp": "127.0.2.0",
}
COMPLAINT: dict[str, Any] = {
    "userAgent": "ExampleCorp Feedback Loop (V0.01)",
    "complainedRecipients": [{"emailAddress": "recipient1@example.com"}],
    "complaintFeedbackType": "abuse",
    "arrivalDate": "2009-12-03T04:24:21.000-05:00",
    "timestamp": "2012-05-25T14:59:38.623Z",
    "feedbackId": "000001378603177f-18c07c78-fa81-4a58-9dd1-fedc3cb8f49a-000000",
}
DELIVERY: dict[str, Any] = {
    "timestamp": "2014-05-28T22:41:01.184Z",
    "processingTimeMillis": 546,
    "recipients": ["success@simulator.amazonses.com"],
    "smtpResponse": "250 ok:  Message 64111812 accepted",
    "reportingMTA": "a8-70.smtp-out.amazonses.com",
    "remoteMtaIp": "127.0.2.0",
}


def ses(kind: str, details: dict[str, Any] | None, *, event_publishing: bool = False) -> dict[str, Any]:
    """An SES notification: feedback notifications (``notificationType``) or event publishing (``eventType``)."""
    key = "failure" if kind == "Rendering Failure" else kind[0].lower() + kind[1:]
    document: dict[str, Any] = {("eventType" if event_publishing else "notificationType"): kind, "mail": MAIL}
    if details is not None:
        document[key] = details
    return document


# AWS's example of an S3 `Object Created` event sent through EventBridge.
EVENTBRIDGE_S3_CREATED = '{"version":"0","id":"17793124-05d4-b198-2fde-7ededc63b103","detail-type":"Object Created","source":"aws.s3","account":"111122223333","time":"2021-11-12T00:00:00Z","region":"ca-central-1","resources":["arn:aws:s3:::amzn-s3-demo-bucket1"],"detail":{"version":"0","event-version":"1.2","bucket":{"name":"amzn-s3-demo-bucket1","aws-generated-tags":{"aws:cloudformation:stack-name":"amzn-s3-demo-stack"}},"object":{"key":"example-key","size":5,"etag":"b1946ac92492d2347c6235b4d2611184","version-id":"IYV3p45BT0ac8hjHg1houSdS1a.Mro8e","sequencer":"617f08299329d189"},"request-id":"N4N7GDK58NMKJ12R","requester":"123456789012","source-ip-address":"192.0.2.1","reason":"PutObject"}}'  # noqa: E501 - verbatim

# AWS's example of an S3 `Object Deleted` event (lifecycle expiration) sent through EventBridge.
EVENTBRIDGE_S3_DELETED = '{"version":"0","id":"ad1de317-e409-eba2-9552-30113f8d88e3","detail-type":"Object Deleted","source":"aws.s3","account":"111122223333","time":"2021-11-12T00:00:00Z","region":"ca-central-1","resources":["arn:aws:s3:::amzn-s3-demo-bucket1"],"detail":{"version":"0","event-version":"1.2","bucket":{"name":"amzn-s3-demo-bucket1"},"object":{"key":"example-key","etag":"d41d8cd98f00b204e9800998ecf8427e","version-id":"mtB0cV.jejK63XkRNceanNMC.qXPWLeK","sequencer":"617b398000000000"},"request-id":"20EB74C14654DC47","requester":"s3.amazonaws.com","reason":"Lifecycle Expiration","deletion-type":"Delete Marker Created"}}'  # noqa: E501 - verbatim


def eventbridge(source: str, detail_type: str, detail: Any, **fields: Any) -> dict[str, Any]:
    """An EventBridge event as a rule delivers it to a queue or topic."""
    return {
        "version": "0",
        "id": fields.get("id", "0d079340-135a-c8c6-95c2-41fb8f496c53"),
        "detail-type": detail_type,
        "source": source,
        "account": fields.get("account", "111122223333"),
        "time": fields.get("time", "2026-11-01T12:00:00Z"),
        "region": fields.get("region", "eu-north-1"),
        "resources": fields.get("resources", []),
        "detail": detail,
    }


# The `detail` of AWS's example events (ECS, Step Functions, EC2, Auto Scaling, AWS Health).
ECS_TASK_RUNNING: dict[str, Any] = {
    "attachments": [
        {
            "id": "fe3a9a46-6a47-40ee-afd9-7952ae90a75a",
            "type": "eni",
            "status": "ATTACHED",
            "details": [
                {"name": "subnetId", "value": "subnet-0d0eab1bb38d5ca64"},
                {"name": "privateIPv4Address", "value": "10.0.1.163"},
            ],
        }
    ],
    "attributes": [{"name": "ecs.cpu-architecture", "value": "x86_64"}],
    "availabilityZone": "us-east-1b",
    "capacityProviderName": "FARGATE",
    "clusterArn": "arn:aws:ecs:us-east-1:123456789012:cluster/example-cluster",
    "connectivity": "CONNECTED",
    "connectivityAt": "2025-05-06T11:02:17.19Z",
    "containers": [
        {
            "containerArn": "arn:aws:ecs:us-east-1:123456789012:container/example-cluster/a1173316d40a45dea9/a0a99b87-baa8-4bf6-b9f1-a9a95917a635",  # noqa: E501 - verbatim
            "lastStatus": "RUNNING",
            "name": "web",
            "image": "nginx",
            "imageDigest": "sha256:c15da6c91de8d2f436196f3a768483ad32c258ed4e1beb3d367a27ed67253e66",
            "runtimeId": "a1173316d40a45dea9-0265927825",
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/example-cluster/a1173316d40a45dea9",
            "networkInterfaces": [
                {"attachmentId": "fe3a9a46-6a47-40ee-afd9-7952ae90a75a", "privateIpv4Address": "10.0.1.163"}
            ],
            "cpu": "99",
            "memory": "100",
        }
    ],
    "cpu": "256",
    "createdAt": "2025-05-06T11:02:13.877Z",
    "desiredStatus": "RUNNING",
    "enableExecuteCommand": False,
    "ephemeralStorage": {"sizeInGiB": 20},
    "group": "family:webserver",
    "launchType": "FARGATE",
    "lastStatus": "RUNNING",
    "memory": "512",
    "overrides": {"containerOverrides": [{"name": "web"}]},
    "platformVersion": "1.4.0",
    "pullStartedAt": "2025-05-06T11:02:24.162Z",
    "pullStoppedAt": "2025-05-06T11:02:33.493Z",
    "startedAt": "2025-05-06T11:02:34.325Z",
    "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/example-cluster/a1173316d40a45dea9",
    "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/webserver:5",
    "updatedAt": "2025-05-06T11:02:34.325Z",
    "version": 3,
}

STEP_FUNCTIONS_FAILED: dict[str, Any] = {
    "executionArn": "arn:aws:states:us-east-2:123456789012:execution:state-machine-name:execution-name",
    "stateMachineArn": "arn:aws:states:us-east-2:123456789012:stateMachine:state-machine",
    "name": "execution-name",
    "status": "FAILED",
    "startDate": 1551225146847,
    "stopDate": 1551225151881,
    "input": "{}",
    "inputDetails": {"included": True},
    "output": None,
    "outputDetails": None,
    "stateMachineVersionArn": None,
    "stateMachineAliasArn": None,
    "redriveCount": 0,
    "redriveDate": None,
    "redriveStatus": "REDRIVABLE",
    "redriveStatusReason": "Execution is FAILED and is redrivable",
    "error": "ErrorCode",
    "cause": "An error occurred.",
}

AUTO_SCALING_LAUNCHING: dict[str, Any] = {
    "Origin": "EC2",
    "LifecycleHookName": "my-hook",
    "Destination": "AutoScalingGroup",
    "AccountId": "123456789012",
    "RequestId": "18b2ec17-3e9b-4c15-8024-ff2e8ce8786a",
    "LifecycleTransition": "autoscaling:EC2_INSTANCE_LAUNCHING",
    "AutoScalingGroupName": "my-asg",
    "Service": "AWS Auto Scaling",
    "Time": "2021-01-19T00:36:26.533Z",
    "EC2InstanceId": "i-0598c7d356eba48d7",
    "LifecycleActionToken": "71514b9d-6a40-4b26-8523-05e7ee35fa40",
    "NotificationMetadata": "hook message metadata",
}
"""What a lifecycle hook sends an SQS queue or SNS topic directly."""

AUTO_SCALING_TEST: dict[str, Any] = {
    "AccountId": "123456789012",
    "RequestId": "18b2ec17-3e9b-4c15-8024-ff2e8ce8786a",
    "AutoScalingGroupARN": "arn:aws:autoscaling:us-west-2:123456789012:autoScalingGroup:042cba90-ad2f-431c-9b4d-6d9055bcc9fb:autoScalingGroupName/my-asg",  # noqa: E501 - verbatim
    "AutoScalingGroupName": "my-asg",
    "Service": "AWS Auto Scaling",
    "Event": "autoscaling:TEST_NOTIFICATION",
    "Time": "2021-01-19T00:35:52.359Z",
}

HEALTH_INSTANCE_RETIREMENT: dict[str, Any] = {
    "eventArn": "arn:aws:health:us-east-1::event/AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED_90353408594353983",
    "service": "EC2",
    "eventTypeCode": "AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED",
    "eventTypeCategory": "scheduledChange",
    "eventScopeCode": "ACCOUNT_SPECIFIC",
    "communicationId": "1234abc01232a4012345678-1",
    "startTime": "Thu, 27 Aug 2026 13:19:03 GMT",
    "lastUpdatedTime": "Thu, 27 Jan 2026 13:44:13 GMT",
    "statusCode": "open",
    "eventRegion": "us-east-1",
    "eventDescription": [
        {"language": "en_US", "latestDescription": "A description of the event will be provided here"}
    ],
    "eventMetadata": {"keystring1": "valuestring1", "truncated": "true"},
    "affectedEntities": [
        {
            "entityValue": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
            "lastUpdatedTime": "Thu, 26 Jan 2026 19:01:55 GMT",
            "status": "PENDING",
        }
    ],
    "affectedAccount": "123456789012",
    "page": "1",
    "totalPages": "1",
    "backupEvent": "false",
    "personas": ["OPERATIONS"],
    "actionability": "ACTION_REQUIRED",
}
