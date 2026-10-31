# Parity with real AWS

fanferro runs on real AWS, on AWS emulators (MiniStack, LocalStack) and on its in-memory broker.
This page records how each of them behaves where fanferro depends on it, as measured by the probes,
and what fanferro does about each difference. Real AWS is the reference
([ADR 0011](../adr/0011-real-aws-is-the-reference.md)): the in-memory broker follows it, and
fanferro's own tests must not rely on an emulator where the emulator differs.

## The probes

* `scripts/probe_backends.py` drives fanferro's public API on one target (`memory`, `ministack`,
  `localstack`, `aws`) and records facts: outcomes, exception classes, error kinds, codes, HTTP
  statuses, messages, what was delivered where. `compare` lists the facts that differ from the
  first file.
* `scripts/probe_aws_cli.sh` makes the raw SQS, SNS, S3 and STS calls fanferro relies on with the
  AWS CLI and prints normalised outcomes; compare two targets with `diff -u`.

How to run them, and the IAM policy a run on AWS needs, are in the
`fanferro-e2e-testing` skill (`.claude/skills/fanferro-e2e-testing/SKILL.md`). Compare a new
memory run with the recorded AWS run without AWS access:

```bash
uv run python scripts/probe_backends.py run --target memory
uv run python scripts/probe_backends.py compare docs/parity/2026-10-31/probe-aws.json probe-memory.json
```

## Recorded runs

| Directory | Targets | Versions |
| --- | --- | --- |
| [2026-10-31](2026-10-31/) | AWS `eu-north-1`, LocalStack, MiniStack, memory | fanferro 0.6.0, AWS CLI v2, MiniStack 1.5.27, LocalStack 2026.x |
| [2026-10-31-second](2026-10-31-second/) | AWS `eu-north-1` (with the cases of the open questions), memory | fanferro with #146-#154, AWS CLI v2 |

The second run used fanferro with the fixes of #146-#154. Where those changed fanferro itself (the 60
second limits as throttling, FIFO ids refused before sending, payload buckets with unknown
credentials), it shows the fixes on AWS; it answered the open questions of the first run.

Each directory holds `probe-<target>.json` (scenarios through fanferro) and
`probe-cli-<target>.txt` (raw AWS CLI calls). Names, account ids, ids, receipt handles and times are
replaced by placeholders; the files contain no credentials.

## Findings

Each finding states what real AWS does (the reference), how the other targets differed, and its
status: **Open** with the planned fix, **Fixed** with the pull request that fixed it, or **No
change** with the reason. "memory" is fanferro's in-memory broker as of 0.6.0. AWS error texts are
quoted as the services return them; `<v>` stands for a value from the request.

### SQS queues

| ID | Real AWS | Differences | Status |
| --- | --- | --- | --- |
| Q1 | `CreateQueue` and `SetQueueAttributes` refuse out-of-range values with `InvalidAttributeValue` (400) "Invalid value for the parameter `<Name>`.": `VisibilityTimeout` 0-43200, `MessageRetentionPeriod` 60-1209600, `MaximumMessageSize` 1024-1048576, `DelaySeconds` 0-900, `ReceiveMessageWaitTimeSeconds` 0-20 | memory, LocalStack accept them | **Fixed** (#146): memory validates like SQS |
| Q2 | Unknown attribute names, FIFO attributes on a standard queue (`ContentBasedDeduplication`) and changing `FifoQueue` fail with `InvalidAttributeName` "Unknown Attribute `<Name>`." | memory, MiniStack accept them (MiniStack turns the queue FIFO) | **Fixed** (#146): memory validates like SQS |
| Q3 | `FifoThroughputLimit=perMessageGroupId` with `DeduplicationScope=queue`: `InvalidAttributeValue` "Invalid value for the parameter FifoThroughputLimit. Reason: To set FifoThroughputLimit to perMessageGroupId, the DeduplicationScope must be messageGroup." | memory, MiniStack, LocalStack accept it | **Fixed** (#146): memory validates like SQS |
| Q4 | A redrive policy names a dead-letter queue of the other type: `InvalidParameterValue` "Value `<policy>` for parameter RedrivePolicy is invalid. Reason: Dead-letter queue must be same type of queue as the source.."; `maxReceiveCount` 0: "... Reason: Invalid value for maxReceiveCount: 0, valid values are from 1 to 1000 both inclusive." | MiniStack, LocalStack accept the other type; memory's texts differ | **Fixed** (#146): memory uses the AWS texts |
| Q5 | Queue names: `InvalidParameterValue` "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length" (also for a `.fifo` name without `FifoQueue=true`); `FifoQueue=true` without the suffix: "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length." | memory's texts differ; MiniStack accepts invalid names | **Fixed** (#146): memory uses the AWS texts (fanferro validates names before the network anyway) |
| Q6 | A queue encrypted with a KMS key reports `SqsManagedSseEnabled=false` | memory reports `true`; MiniStack omits the attribute | **Fixed** (#146): memory reports `false` |
| Q7 | Creating a queue within 60 seconds of deleting one of the same name fails: `AWS.SimpleQueueService.QueueDeletedRecently` (400) "You must wait 60 seconds after deleting a queue before you can create another with the same name." | memory, MiniStack, LocalStack create it | **Fixed** (#148): memory refuses for 60 seconds; fanferro classifies the error as throttling (retryable) |
| Q8 | A second `PurgeQueue` within 60 seconds fails: `AWS.SimpleQueueService.PurgeQueueInProgress` (403) "Only one PurgeQueue operation on `<queue name>` is allowed every 60 seconds." | memory, MiniStack, LocalStack purge again | **Fixed** (#148): memory refuses for 60 seconds; fanferro classifies the error as throttling (retryable) |
| Q9 | A missing queue is `AWS.SimpleQueueService.NonExistentQueue` with status 400 "The specified queue does not exist." | memory reports status 404; MiniStack adds "for this wsdl version" | **Fixed** (#150): memory reports 400 |
| Q10 | A queue URL or owner id of an account that does not exist (`123456789012`): `InvalidAddress` (404) "The address https://sqs.`<region>`.amazonaws.com/ is not valid for this endpoint." | memory, emulators: `NonExistentQueue` | No change: an artefact of the made-up account; queues of real accounts answer `NonExistentQueue` or access denied |

### SQS messages and receipt handles

| ID | Real AWS | Differences | Status |
| --- | --- | --- | --- |
| M1 | `Number` attributes come back without leading and trailing zeros (`1.50` as `1.5`, `3.0` from SNS as `3`); `MD5OfMessageAttributes` of a received message is computed over the values as returned, the one `SendMessage` returns over the values as sent. The second run confirmed `-0.50` as `-0.5`, `0.000` as `0`, `.5` as `0.5` and `100` as `100`. | memory, emulators return the value as sent | **Fixed** (#149): memory returns normalised numbers with their MD5 |
| M2 | Standard queue, message received again after its visibility timeout ran out: the earlier receipt handle still works: `ChangeMessageVisibility` succeeds, `DeleteMessage`/`DeleteMessageBatch` succeed and delete the message; a visibility change with the earlier handle applies to the message (second run) | memory: batch entry `InvalidParameterValue` "The receipt handle has expired." and the message stays; MiniStack: `ReceiptHandleIsInvalid`; LocalStack like AWS | **Fixed** (#147): memory follows AWS |
| M3 | FIFO queue, same situation: the earlier handle fails; batch entries `ReceiptHandleIsInvalid` "The receipt handle has expired"; single calls `InvalidParameterValue` "Value `<handle>` for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired." | memory: `InvalidParameterValue` for deletes, "The input receipt handle is invalid." for visibility changes; LocalStack accepts the handle | **Fixed** (#147): memory uses the AWS codes and texts |
| M4 | `ChangeMessageVisibility` for a deleted message: `InvalidParameterValue` "Value `<handle>` for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change." | memory, MiniStack: `ReceiptHandleIsInvalid` | **Fixed** (#147): memory follows AWS |
| M5 | A handle that is no receipt handle: single calls `ReceiptHandleIsInvalid` (404) "The input receipt handle is invalid."; batch entries "The input receipt handle "`<handle>`" is not a valid receipt handle." | memory's batch text differs | **Fixed** (#147): memory uses the AWS text |
| M6 | `ChangeMessageVisibility` on a standard-queue message that is visible again (released with timeout 0), with its latest handle, succeeds and hides it again. A FIFO queue answers `InvalidParameterValue` "Value `<handle>` for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired." (second run) | memory: `MessageNotInflight` | **Fixed** (#147) for standard queues. FIFO queues: Open. Plan: memory answers like SQS (`ReceiptHandleIsInvalid` "The receipt handle has expired" in batches) |
| M7 | Visibility limits: a timeout over 43200: `InvalidParameterValue` "Value 43201 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be an integer between 0 and 43200."; a timeout that ends more than 12 hours after the receive: "Value 43200 for parameter VisibilityTimeout is invalid. Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds]." | memory checks only the first; emulators neither | **Fixed** (#147): memory checks both (fanferro's heartbeats already stop 12 hours after the receive) |
| M8 | FIFO `MessageGroupId` and `MessageDeduplicationId`: 1-128 alphanumeric or punctuation characters, else `InvalidParameterValue` "Value `<v>` for parameter MessageGroupId is invalid. Reason: MessageGroupId can only include alphanumeric and punctuation characters. 1 to 128 in length." | fanferro sends them unchecked; memory, MiniStack accept them | **Fixed** (#151): fanferro validates them before sending (architecture invariant 6); memory refuses them like SQS |
| M9 | Batch entry errors carry only the service's text, e.g. `EmptyValue` "No Value Found for SendMessageBatchRequestEntry.2.MessageBody."; a too large message: "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes." | memory's entry texts include fanferro's own error formatting; its size text lacks the first sentence | **Fixed** (#150): memory uses the AWS texts. Attribute names, numbers and receive parameters SQS refuses keep the broker's texts: fanferro refuses them before the network with its own |
| M10 | A long poll that is aborted on the client goes on on the server until its wait time ends, and a message sent meanwhile goes to it: it comes back after its visibility timeout, with a receive count of 2 (ADR 0007; the receive count is from the second run) | memory, MiniStack: the poll ends with the connection; LocalStack like AWS | **Fixed** (#155) as an option: `MemoryBroker(orphaned_polls=True)` serves aborted polls like SQS, and the engine protects released messages on any backend that does. Off by default, so that tests that stop a consumer and go on at once do not wait for visibility timeouts |
| M11 | A `Number` value with more than 38 significant digits fails: `InvalidParameterValue` "Message (user) attribute 'n' number value [`<v>`] must contain fewer than 38 precision digits." (39 digits; 38 pass; second run) | fanferro sends it; memory, emulators accept it | Open. Plan: fanferro refuses it before sending (architecture invariant 6); memory refuses it with the AWS text |

### SNS

| ID | Real AWS | Differences | Status |
| --- | --- | --- | --- |
| S1 | Errors use the code `InvalidParameter`: topic names "Invalid parameter: Topic Name" (also a `.fifo` name without `FifoTopic=true`, or the reverse); an existing topic "Invalid parameter: Attributes Reason: Topic already exists with different attributes"; `ContentBasedDeduplication` on a standard topic "Invalid parameter: Attributes Reason: Unknown attribute ContentBasedDeduplication"; `SignatureVersion` 3: "Invalid parameter: Attributes Reason: SignatureVersion: Invalid value '[3]'. Must be 1 or 2." (`SetTopicAttributes`: "Invalid parameter: SignatureVersion: ..."); changing `FifoTopic`: "Invalid parameter: AttributeName" | memory uses `InvalidParameterValue`, other texts, and accepts the attributes; MiniStack accepts most and turns a topic FIFO | **Fixed** (#152): memory follows AWS |
| S2 | Topics report `Policy` (the default policy) and `EffectiveDeliveryPolicy`; subscriptions report `SubscriptionPrincipal`, and `FilterPolicyScope=MessageAttributes` once they have a filter policy | memory omits them | **Fixed** (#152): memory reports them, with AWS's default values (second run). Open. Plan: memory writes the default `Policy` with the keys in AWS's order (its text differs, its content does not). No change: `SubscriptionPrincipal` is the subscriber's IAM ARN on AWS, while the broker has no caller and reports the account root |
| S3 | Every topic operation with a topic ARN of another region (`Publish`, `PublishBatch`, `GetTopicAttributes`, `SetTopicAttributes`, `Subscribe`, `ListSubscriptionsByTopic`, `DeleteTopic`; second run) answers `InvalidParameter` (400) "Invalid parameter: TopicArn" | memory, MiniStack: `NotFound`; LocalStack like AWS | **Fixed** (#152) for `Publish` and `PublishBatch`. Open. Plan: the other operations |
| S4 | Subscribing a FIFO queue to a standard topic: "Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics"; again with other attributes: "Invalid parameter: Attributes Reason: Subscription already exists with different attributes" | memory's code and texts differ; MiniStack accepts both | **Fixed** (#152): memory follows AWS (fanferro refuses the first before the network) |
| S5 | Filter policies are validated with "Invalid parameter: Attributes Reason: FilterPolicy: `<reason>`": "Unrecognized match type glob", "Filter policy can not have more than 5 keys", "Filter policy is too complex", "Value of > must be numeric", "Empty arrays are not allowed", "exists match pattern must be either true or false.", "failed to parse JSON. ..."; nested in the attribute scope: "Invalid parameter: Attributes Reason: Filter policy scope MessageAttributes does not support nested filter policy" | memory refuses the same policies with other texts; MiniStack accepts them all; LocalStack does not support `wildcard` | **Fixed** (#152): memory uses the AWS reasons; it leaves out the JSON parser position SNS appends to some (" at [Source: ...; line: 1, column: N]"), which differs between AWS APIs |
| S6 | Filter policy matching, 19 policies (attributes and body, every operator) over 8 messages | memory matches AWS exactly; MiniStack lets numeric operators match `String` attributes | No change |
| S7 | With `TracingConfig=Active`, SNS passes the X-Ray trace header on with its own `Parent` (same `Root` and `Sampled`); with `PassThrough` unchanged | memory passes it unchanged; MiniStack drops it | **Fixed** (#153): memory replaces `Parent` on active topics |
| S8 | SNS does not deliver to a queue encrypted with the AWS managed key `alias/aws/sqs` (its key policy cannot allow SNS); nothing reports it | memory, emulators deliver | **Fixed** (#153): memory drops such deliveries; provisioning emits a `config.warning` for such subscriptions |
| S9 | Notifications (raw delivery off) keep `Number` values as published (`3.0`) and carry `Binary` values in base64; the document is pretty-printed | memory like AWS; MiniStack sends `Binary` values empty | No change (fanferro reads both layouts) |
| S10 | FIFO: a duplicate publish returns the first message id; deliveries to FIFO queues carry a `SequenceNumber` | MiniStack: no `SequenceNumber`; LocalStack: a new id, and duplicates reach standard queues | No change |
| S11 | SNS FIFO ids: `InvalidParameter` "Invalid parameter: The MessageGroupId parameter can only include alphanumeric and punctuation characters. 1 to 128 in length" (and the same for `MessageDeduplicationId`); `PublishBatch` refuses the whole call with it (second run) | memory (#151) uses another text and fails only the entries | Open. Plan: memory uses the AWS text and refuses the call (fanferro refuses such ids before sending) |

### S3 and payloads

| ID | Real AWS | Differences | Status |
| --- | --- | --- | --- |
| P1 | `HeadBucket` with credentials AWS does not know answers 403 without a body, like for a bucket of another account; `GetBucketLocation` answers `InvalidAccessKeyId` (403) for such credentials (second run). | emulators do not check credentials | **Fixed** (#154): after a 403 fanferro asks `GetBucketLocation`, whose error names unknown credentials, so provisioning no longer reports the bucket as found |
| P2 | A presigned URL of a missing object: 404 "Not Found" (`NoSuchKey`); an expired one: 403 `AccessDenied` | memory's text says "HTTP 404" instead of "HTTP 404 Not Found"; LocalStack does not expire URLs | **Fixed** (#150): memory uses the AWS status text |
| P3 | `CreateBucket` for an owned bucket outside `us-east-1`: 409 `BucketAlreadyOwnedByYou` | emulators succeed | No change (fanferro treats both as success) |

### Credentials

| ID | Real AWS | Differences | Status |
| --- | --- | --- | --- |
| C1 | Unknown credentials: `InvalidClientTokenId` (403) "The security token included in the request is invalid." from STS, SQS and SNS, `InvalidAccessKeyId` (403) from S3; fanferro reports access denied | emulators accept any credentials; memory has none | No change |

## Questions the second run answered

* **O1** A FIFO queue refuses a visibility change for a released message with its latest handle:
  the handle has expired (M6).
* **O2** Every SNS topic operation answers "Invalid parameter: TopicArn" for an ARN of another
  region (S3).
* **O3** On a standard queue, a visibility change with an earlier handle applies to the message, and
  one on a released message hides it again (M2, M6).
* **O4** SQS normalises `Number` values as the broker does; it refuses values with more than 38
  significant digits (M1, M11).
* **O5** S3 answers `GetBucketLocation` with unknown credentials with `InvalidAccessKeyId` (P1).
* **O6** A message an aborted long poll took comes back with a receive count of 2 (M10).
* **O7** The default `Policy` and `EffectiveDeliveryPolicy` are what the broker reports;
  `SubscriptionPrincipal` is the subscriber's IAM ARN; SNS refuses invalid FIFO ids with its own
  text (S2, S11).

None of the first run's questions is still open; the findings they led to are in the tables above, some of them open.

## Emulator differences fanferro's tests must not rely on

MiniStack (1.5.27), used by fanferro's end-to-end tests, differs from AWS in: queue attribute and
name validation (Q1-Q5), `SqsManagedSseEnabled`, `QueueDeletedRecently` and the purge limit,
receipt handles of earlier receives (M2-M4), `Number` normalisation (M1), FIFO id validation (M8),
aborted long polls (M10), SNS validation (S1, S4, S5), numeric filters on `String` attributes (S6),
X-Ray headers through SNS, empty `Binary` values in notifications (S9), no `SequenceNumber` on SNS
FIFO deliveries (S10), KMS-encrypted subscribers (S8) and credentials (C1). A test of such
behaviour runs on the in-memory broker only, with a comment naming the finding.

LocalStack (2026.x) differs in: queue attribute ranges (Q1), the other dead-letter queue type (Q4),
`QueueDeletedRecently` and the purge limit, FIFO receipt handles (M3), `Number` normalisation,
presigned URL expiry (P2), `wildcard` filters (S5), FIFO duplicates (S10), KMS-encrypted
subscribers and credentials.
