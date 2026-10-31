#!/usr/bin/env bash
# Probes the raw SQS, SNS, S3 and STS behaviour fanferro relies on with the AWS CLI, so that real
# AWS can be compared with MiniStack and LocalStack call by call (scripts/probe_backends.py compares
# what fanferro itself reports).
#
#   AWS_PROFILE=sandbox scripts/probe_aws_cli.sh --region eu-north-1 > probe-cli-aws.txt
#   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
#     scripts/probe_aws_cli.sh --endpoint-url http://localhost:4566 > probe-cli-ministack.txt
#   diff -u probe-cli-aws.txt probe-cli-ministack.txt
#
# Every case prints its outcome (ok or error), the HTTP status, the error code and message, and
# selected fields of the response. Names, account ids, the region, ids, receipt handles and
# timestamps are replaced by placeholders, so the files of two targets differ only where the
# services do. The output contains no credentials: --debug output (which has the signed headers) is
# only read to find HTTP statuses, in a private temporary directory that is deleted on exit.
#
# Needs bash (3.2 or later), the AWS CLI (v2; v1 works, but its presigned URLs may differ) and
# curl. Every resource is named ffprobe-cli-<run>-... (inside the `probe_backends.py iam-policy`)
# and deleted on exit; set AWS_CLI to run another command than `aws` (AWS_CLI="uvx --from awscli aws").

# shellcheck disable=SC2016 # JMESPath literals (`null`) are not shell expansions

set -u

ENDPOINT=""
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
KEEP=0
while [ $# -gt 0 ]; do
    case "$1" in
        --endpoint-url) ENDPOINT="${2%/}"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        -h | --help) sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
    esac
done
if [ -z "$REGION" ]; then
    if [ -n "$ENDPOINT" ]; then REGION=us-east-1; else echo "pass --region (or set AWS_REGION)" >&2; exit 2; fi
fi
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
AWS_CLI="${AWS_CLI:-aws}"
export AWS_PAGER=""

WORK="$(mktemp -d "${TMPDIR:-/tmp}/ffprobe-cli.XXXXXX")" || exit 2
chmod 700 "$WORK"
RUN="$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
P="ffprobe-cli-$RUN"
OTHER_REGION=us-west-2
[ "$REGION" = us-west-2 ] && OTHER_REGION=us-east-1
FOREIGN_ACCOUNT=123456789012
TRACE="Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
COUNT='length(Messages || `[]`)'

# The AWS CLI with the target's region and endpoint.
awscli() {
    # shellcheck disable=SC2086 # AWS_CLI may be a command with arguments
    if [ -n "$ENDPOINT" ]; then
        $AWS_CLI --region "$REGION" --endpoint-url "$ENDPOINT" "$@"
    else
        $AWS_CLI --region "$REGION" "$@"
    fi
}

case "$(awscli --version 2>&1 | grep -o 'aws-cli/[0-9]*' | head -n 1)" in
    aws-cli/1) CLI_MAJOR=1 ;;
    *) CLI_MAJOR=2 ;;
esac
# Binary values: v2 takes base64, v1 the raw bytes.
if [ "$CLI_MAJOR" = 2 ]; then BIN='AQI='; else BIN='\u0001\u0002'; fi

ACCOUNT="$(awscli sts get-caller-identity --query Account --output text 2>/dev/null)"
if [ -z "$ACCOUNT" ] || [ "$ACCOUNT" = None ]; then
    echo "no usable credentials: sts get-caller-identity failed" >&2
    exit 2
fi

ENDPOINT_RE="$(printf '%s' "$ENDPOINT" | sed -E 's/[][\.*^$/]/\\&/g')"
# Replaces what differs between runs and targets with placeholders.
normalize() {
    if [ -n "$ENDPOINT" ]; then sed -E "s/$ENDPOINT_RE/<endpoint>/g"; else cat; fi | sed -E \
        -e 's#https?://[a-z0-9.-]*localhost[a-z0-9.-]*(:[0-9]+)?#<endpoint>#g' \
        -e "s/$P/<run>/g" \
        -e 's/"[0-9]{13}"/"<ms>"/g' \
        -e 's/"[0-9]{20,}"/"<sequence>"/g' \
        -e "s/$ACCOUNT/<account>/g" \
        -e 's#arn:aws[a-z-]*:(iam|sts)::<account>:[^" ]*#<caller-arn>#g' \
        -e "s/$REGION/<region>/g" \
        -e 's#https://(sqs|sns)\.<region>\.amazonaws\.com#<endpoint>#g' \
        -e 's/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<uuid>/g' \
        -e 's#[A-Za-z0-9+/=_%-]{80,}#<long>#g' \
        -e 's/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z/<time>/g' \
        -e 's/\?[^" ]*X-Amz-[^" ]*/?<query>/g'
}

# probe NAME QUERY AWS-CLI-ARGS...: runs one call and prints its outcome. QUERY is a JMESPath
# expression applied to the response, "-" for all of it. The raw response stays in $WORK/last.json.
probe() {
    local name="$1" query="$2"
    shift 2
    if [ "$query" = - ]; then
        awscli "$@" --output json --debug >"$WORK/last.json" 2>"$WORK/debug.txt"
    else
        awscli "$@" --query "$query" --output json --debug >"$WORK/last.json" 2>"$WORK/debug.txt"
    fi
    local code=$?
    local status error line
    status="$(sed -n -E 's/.*HTTP\/1\.1" ([0-9]{3}).*/\1/p' "$WORK/debug.txt" | tail -n 1)"
    line="$(grep -n -E '^(aws: \[ERROR\]: |An error occurred \(|Parameter validation failed|aws: error|usage: aws|fatal error|Could not connect|Error parsing)' "$WORK/debug.txt" | tail -n 1 | cut -d: -f1)"
    error=""
    [ -n "$line" ] && error="$(tail -n "+$line" "$WORK/debug.txt" | grep -v '^[[:space:]]*$' | sed -e 's/^aws: \[ERROR\]: //')"
    rm -f "$WORK/debug.txt"
    {
        echo "### $name"
        if [ "$code" = 0 ]; then echo "result: ok (http ${status:-?})"; else echo "result: error (http ${status:-?})"; fi
        [ -n "$error" ] && printf '%s\n' "$error" | sed -e 's/^/error: /'
        [ -s "$WORK/last.json" ] && sed -e 's/^/  /' "$WORK/last.json"
        echo
    } | normalize
    echo "  $name" >&2
    return "$code"
}

# fact NAME VALUE: prints a value the script derived (comparisons, counts).
fact() {
    printf '### %s\nvalue: %s\n\n' "$1" "$2" | normalize
}

# last_field NAME: a top-level string field of the last response (requested with a query).
last_field() {
    sed -n -E 's/^[[:space:]]*"'"$1"'": "([^"]*)",?$/\1/p' "$WORK/last.json" | head -n 1
}

# last_string: the last response when it is a single string (a query such as TopicArn).
last_string() {
    sed -n -E 's/^[[:space:]]*"([^"]*)"[[:space:]]*$/\1/p' "$WORK/last.json" | head -n 1
}

# value QUERY AWS-CLI-ARGS...: a single value (empty on errors).
value() {
    local query="$1"
    shift
    awscli "$@" --query "$query" --output text 2>/dev/null | head -n 1
}

# json_string TEXT: TEXT as a JSON string literal.
json_string() {
    printf '"%s"' "$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
}

repeat() { printf "%$2s" '' | tr ' ' "$1"; }

cleanup() {
    rm -rf "$WORK"
    [ "$KEEP" = 1 ] && return
    echo "cleaning up $P-*" >&2
    # One name per line: emulators may have accepted names with spaces.
    awscli sns list-topics --query "Topics[?contains(TopicArn, ':$P')].TopicArn" --output text 2>/dev/null | tr '\t' '\n' |
        while IFS= read -r arn; do [ -n "$arn" ] && awscli sns delete-topic --topic-arn "$arn" >/dev/null 2>&1; done
    awscli sqs list-queues --queue-name-prefix "$P" --query 'QueueUrls[]' --output text 2>/dev/null | tr '\t' '\n' |
        while IFS= read -r url; do [ -n "$url" ] && awscli sqs delete-queue --queue-url "$url" >/dev/null 2>&1; done
    awscli s3api list-buckets --query "Buckets[?starts_with(Name, '$P')].Name" --output text 2>/dev/null | tr '\t' '\n' |
        while IFS= read -r bucket; do [ -n "$bucket" ] && awscli s3 rb "s3://$bucket" --force >/dev/null 2>&1; done
}
trap cleanup EXIT

queue_url() { value QueueUrl sqs get-queue-url --queue-name "$1"; }
queue_arn() { value Attributes.QueueArn sqs get-queue-attributes --queue-url "$1" --attribute-names QueueArn; }

# allow_topic QUEUE_URL QUEUE_ARN TOPIC_ARN: the queue policy SNS needs to deliver.
allow_topic() {
    local policy
    policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage","Resource":"'"$2"'","Condition":{"ArnEquals":{"aws:SourceArn":"'"$3"'"}}}]}'
    awscli sqs set-queue-attributes --queue-url "$1" --attributes "{\"Policy\": $(json_string "$policy")}" >/dev/null
}

# receive_until NAME QUEUE_URL QUERY PUBLISH-COMMAND...: publishes until the queue receives a
# message (new subscriptions and queue policies take a moment on AWS), then prints it.
receive_until() {
    local name="$1" url="$2" query="$3" tries=0 count
    shift 3
    while [ "$tries" -lt 18 ]; do
        tries=$((tries + 1))
        "$@" >/dev/null 2>&1
        count="$(value "$COUNT" sqs receive-message --queue-url "$url" --wait-time-seconds 5 \
            --visibility-timeout 0 --max-number-of-messages 1)"
        [ -n "$count" ] && [ "$count" != 0 ] && break
    done
    probe "$name" "$query" sqs receive-message --queue-url "$url" --wait-time-seconds 5 --max-number-of-messages 1 \
        --attribute-names All --message-attribute-names All
}

echo "probing $([ -n "$ENDPOINT" ] && echo "$ENDPOINT" || echo "AWS") ($REGION) with $(awscli --version 2>&1 | cut -d' ' -f1), run $RUN" >&2
VERSION=""
[ -n "$ENDPOINT" ] && VERSION="$(curl -s "$ENDPOINT/_localstack/health" | sed -n -E 's/.*"version": *"([^"]*)".*/\1/p')"
printf '# probe_aws_cli.sh: %s%s, AWS CLI v%s\n\n' "$([ -n "$ENDPOINT" ] && echo endpoint || echo aws)" "${VERSION:+ $VERSION}" "$CLI_MAJOR"

# -- STS and credentials ------------------------------------------------------------------------

fake() {
    env -u AWS_PROFILE -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
        AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY "$@"
}
probe sts.caller_identity '{account:Account}' sts get-caller-identity
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.sts - sts get-caller-identity
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.sqs - sqs get-queue-url --queue-name "$P-q"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.sns - sns get-topic-attributes \
    --topic-arn "arn:aws:sns:$REGION:$ACCOUNT:$P-t"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.s3_head_bucket - s3api head-bucket --bucket "$P-b"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.s3_put_object - s3api put-object --bucket "$P-b" --key k

# -- SQS: queues --------------------------------------------------------------------------------

Q="$P-q"
probe sqs.create_queue QueueUrl sqs create-queue --queue-name "$Q" --attributes VisibilityTimeout=2
QURL="$(queue_url "$Q")"
probe sqs.default_attributes 'Attributes.{VisibilityTimeout:VisibilityTimeout,MessageRetentionPeriod:MessageRetentionPeriod,DelaySeconds:DelaySeconds,MaximumMessageSize:MaximumMessageSize,ReceiveMessageWaitTimeSeconds:ReceiveMessageWaitTimeSeconds,SqsManagedSseEnabled:SqsManagedSseEnabled}' \
    sqs get-queue-attributes --queue-url "$QURL" --attribute-names All
probe sqs.attribute_names 'sort(keys(Attributes))' sqs get-queue-attributes --queue-url "$QURL" --attribute-names All
probe sqs.create_queue.same_attributes QueueUrl sqs create-queue --queue-name "$Q" --attributes VisibilityTimeout=2
# Probes that could change a queue or topic where an emulator accepts them use throwaway ones.
MQ="$P-mut"
awscli sqs create-queue --queue-name "$MQ" --attributes VisibilityTimeout=2 >/dev/null
MQURL="$(queue_url "$MQ")"
probe sqs.create_queue.different_attributes QueueUrl sqs create-queue --queue-name "$MQ" --attributes VisibilityTimeout=40
probe sqs.create_queue.invalid_name QueueUrl sqs create-queue --queue-name "$P-bad name"
probe sqs.create_queue.name_too_long QueueUrl sqs create-queue --queue-name "$P-$(repeat x 80)"
probe sqs.create_queue.fifo_suffix_without_attribute QueueUrl sqs create-queue --queue-name "$P-a.fifo"
probe sqs.create_queue.fifo_attribute_without_suffix QueueUrl sqs create-queue --queue-name "$P-b" --attributes FifoQueue=true
probe sqs.create_queue.visibility_over_12h QueueUrl sqs create-queue --queue-name "$P-c" --attributes VisibilityTimeout=43201
probe sqs.create_queue.max_size_over_1mib QueueUrl sqs create-queue --queue-name "$P-d" --attributes MaximumMessageSize=1048577
probe sqs.create_queue.retention_under_minute QueueUrl sqs create-queue --queue-name "$P-e" --attributes MessageRetentionPeriod=59
probe sqs.create_queue.content_dedup_on_standard QueueUrl sqs create-queue --queue-name "$P-f" --attributes ContentBasedDeduplication=true
probe sqs.create_queue.unknown_attribute QueueUrl sqs create-queue --queue-name "$P-g" --attributes NoSuchAttribute=1
probe sqs.get_queue_url.missing QueueUrl sqs get-queue-url --queue-name "$P-missing"
probe sqs.get_queue_url.foreign_owner QueueUrl sqs get-queue-url --queue-name "$Q" --queue-owner-aws-account-id "$FOREIGN_ACCOUNT"
probe sqs.get_queue_attributes.missing - sqs get-queue-attributes --queue-url "${QURL}-missing" --attribute-names All
probe sqs.send_message.foreign_queue - sqs send-message --queue-url "$(printf '%s' "$QURL" | sed "s/$ACCOUNT/$FOREIGN_ACCOUNT/")" --message-body x
probe sqs.set_queue_attributes.fifo - sqs set-queue-attributes --queue-url "$MQURL" --attributes FifoQueue=true
probe sqs.attributes_after_mutations 'Attributes.{VisibilityTimeout:VisibilityTimeout,FifoQueue:FifoQueue}' \
    sqs get-queue-attributes --queue-url "$MQURL" --attribute-names All
probe sqs.tag_queue - sqs tag-queue --queue-url "$QURL" --tags team=probe,purpose=parity
probe sqs.list_queue_tags Tags sqs list-queue-tags --queue-url "$QURL"

# -- SQS: messages ------------------------------------------------------------------------------

ATTRS='{"text":{"DataType":"String","StringValue":"hello"},"unicode":{"DataType":"String","StringValue":"räksmörgås ✓"},"decimal":{"DataType":"Number","StringValue":"1.50"},"big":{"DataType":"Number","StringValue":"12345678901234567890"},"bin":{"DataType":"Binary","BinaryValue":"'"$BIN"'"},"custom":{"DataType":"String.custom","StringValue":"x"},"int":{"DataType":"Number.int","StringValue":"7"}}'
SYSTEM='{"AWSTraceHeader":{"DataType":"String","StringValue":"'"$TRACE"'"}}'
probe sqs.send_message '{md5_body:MD5OfMessageBody,md5_attributes:MD5OfMessageAttributes,md5_system:MD5OfMessageSystemAttributes}' \
    sqs send-message --queue-url "$QURL" --message-body '{"label":"attributes"}' --message-attributes "$ATTRS" \
    --message-system-attributes "$SYSTEM"
probe sqs.receive_message 'Messages[0].{handle:ReceiptHandle,body:Body,md5_body:MD5OfBody,md5_attributes:MD5OfMessageAttributes,attributes:MessageAttributes.{text:text,unicode:unicode,decimal:decimal,big:big,bin:bin,custom:custom,int:int},system:sort(keys(Attributes)),receive_count:Attributes.ApproximateReceiveCount,trace:Attributes.AWSTraceHeader,sender_is_set:Attributes.SenderId != `null`}' \
    sqs receive-message --queue-url "$QURL" --attribute-names All --message-attribute-names All --wait-time-seconds 5
H1="$(last_field handle)"
probe sqs.change_visibility.over_12h - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H1" --visibility-timeout 43201
probe sqs.change_visibility.12h - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H1" --visibility-timeout 43200
probe sqs.change_visibility.zero - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H1" --visibility-timeout 0
sleep 1
probe sqs.change_visibility.released_message - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H1" --visibility-timeout 10
# Where that was accepted the message is hidden again: release it for the next receive.
awscli sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H1" --visibility-timeout 0 >/dev/null 2>&1
probe sqs.receive_message.again 'Messages[0].{handle:ReceiptHandle,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$QURL" --attribute-names All --wait-time-seconds 5 --visibility-timeout 30
H2="$(last_field handle)"
probe sqs.delete_message.earlier_handle - sqs delete-message --queue-url "$QURL" --receipt-handle "$H1"
probe sqs.counts_after_earlier_handle 'Attributes.{visible:ApproximateNumberOfMessages,in_flight:ApproximateNumberOfMessagesNotVisible}' \
    sqs get-queue-attributes --queue-url "$QURL" --attribute-names All
probe sqs.delete_message.current_handle - sqs delete-message --queue-url "$QURL" --receipt-handle "$H2"
probe sqs.delete_message.again - sqs delete-message --queue-url "$QURL" --receipt-handle "$H2"
probe sqs.change_visibility.deleted_message - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$H2" --visibility-timeout 10
probe sqs.delete_message.garbage_handle - sqs delete-message --queue-url "$QURL" --receipt-handle not-a-handle

# A released message (visibility 0) whose latest receipt handle is used once more: is it hidden again?
VQ="$P-vis"
awscli sqs create-queue --queue-name "$VQ" --attributes VisibilityTimeout=2 >/dev/null
VQURL="$(queue_url "$VQ")"
awscli sqs send-message --queue-url "$VQURL" --message-body '{"label":"released"}' >/dev/null
probe sqs.released.receive 'Messages[0].{handle:ReceiptHandle}' sqs receive-message --queue-url "$VQURL" --wait-time-seconds 5 --visibility-timeout 30
VR="$(last_field handle)"
awscli sqs change-message-visibility --queue-url "$VQURL" --receipt-handle "$VR" --visibility-timeout 0 >/dev/null 2>&1
sleep 1
probe sqs.released.change_visibility_again - sqs change-message-visibility --queue-url "$VQURL" --receipt-handle "$VR" --visibility-timeout 20
probe sqs.released.received_afterwards "$COUNT" sqs receive-message --queue-url "$VQURL" --wait-time-seconds 2
awscli sqs purge-queue --queue-url "$VQURL" >/dev/null 2>&1
# An earlier receipt handle after the message was received again: does a change of visibility apply?
awscli sqs send-message --queue-url "$VQURL" --message-body '{"label":"earlier"}' >/dev/null
sleep 1
probe sqs.earlier_handle.first_receive 'Messages[0].{handle:ReceiptHandle,body:Body}' sqs receive-message --queue-url "$VQURL" --wait-time-seconds 5
VE1="$(last_field handle)"
sleep 3
probe sqs.earlier_handle.second_receive 'Messages[0].{handle:ReceiptHandle,body:Body}' sqs receive-message --queue-url "$VQURL" --wait-time-seconds 5 --visibility-timeout 30
probe sqs.earlier_handle.release - sqs change-message-visibility --queue-url "$VQURL" --receipt-handle "$VE1" --visibility-timeout 0
probe sqs.earlier_handle.received_after_release 'Messages[].{body:Body,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$VQURL" --attribute-names All --wait-time-seconds 2 --visibility-timeout 30

# Number attributes: which values SQS accepts, and how it returns them.
NQ="$P-numbers"
awscli sqs create-queue --queue-name "$NQ" >/dev/null
NQURL="$(queue_url "$NQ")"
for number in 007 -0.50 1.0E2 +5 0.000 100 .5 -0 1e-3 12345678901234567890123456789012345678 123456789012345678901234567890123456789; do
    probe "sqs.number.send.$number" '{md5_attributes:MD5OfMessageAttributes}' sqs send-message --queue-url "$NQURL" \
        --message-body "$number" --message-attributes '{"n":{"DataType":"Number","StringValue":"'"$number"'"}}'
done
probe sqs.number.received 'sort_by(Messages, &Body)[].{sent:Body,received:MessageAttributes.n.StringValue,md5_attributes:MD5OfMessageAttributes}' \
    sqs receive-message --queue-url "$NQURL" --message-attribute-names All --max-number-of-messages 10 --wait-time-seconds 2
probe sqs.number.received_more 'sort_by(Messages, &Body)[].{sent:Body,received:MessageAttributes.n.StringValue,md5_attributes:MD5OfMessageAttributes}' \
    sqs receive-message --queue-url "$NQURL" --message-attribute-names All --max-number-of-messages 10 --wait-time-seconds 2

# The visibility timeout ran out and the message was received again: the first receipt handle.
awscli sqs send-message --queue-url "$QURL" --message-body '{"label":"expiry"}' >/dev/null
probe sqs.expiry.first_receive 'Messages[0].{handle:ReceiptHandle,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$QURL" --attribute-names All --wait-time-seconds 5
E1="$(last_field handle)"
sleep 3
probe sqs.expiry.second_receive 'Messages[0].{handle:ReceiptHandle,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$QURL" --attribute-names All --wait-time-seconds 5 --visibility-timeout 30
E2="$(last_field handle)"
probe sqs.expiry.change_visibility_first_handle - sqs change-message-visibility --queue-url "$QURL" --receipt-handle "$E1" --visibility-timeout 30
probe sqs.expiry.delete_first_handle - sqs delete-message --queue-url "$QURL" --receipt-handle "$E1"
probe sqs.expiry.delete_batch_first_handle 'Failed' sqs delete-message-batch --queue-url "$QURL" \
    --entries "[{\"Id\":\"a\",\"ReceiptHandle\":$(json_string "$E1")}]"
probe sqs.expiry.counts 'Attributes.{visible:ApproximateNumberOfMessages,in_flight:ApproximateNumberOfMessagesNotVisible}' \
    sqs get-queue-attributes --queue-url "$QURL" --attribute-names All
probe sqs.expiry.delete_second_handle - sqs delete-message --queue-url "$QURL" --receipt-handle "$E2"

# Requests SQS refuses go to a queue of their own: where an emulator accepts them, the messages
# must not get in the way of other cases.
awscli sqs create-queue --queue-name "$P-bad" >/dev/null
BADURL="$(queue_url "$P-bad")"
probe sqs.send_message.empty_body - sqs send-message --queue-url "$BADURL" --message-body ""
probe sqs.send_message.invalid_character - sqs send-message --queue-url "$BADURL" --message-body "$(printf 'bad \357\277\276 char')"
probe sqs.send_message.delay_over_15min - sqs send-message --queue-url "$BADURL" --message-body x --delay-seconds 901
probe sqs.send_message.attribute_name_with_space - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{"bad name":{"DataType":"String","StringValue":"x"}}'
probe sqs.send_message.attribute_name_aws_prefix - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{"AWS.reserved":{"DataType":"String","StringValue":"x"}}'
probe sqs.send_message.attribute_name_leading_dot - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{".dot":{"DataType":"String","StringValue":"x"}}'
probe sqs.send_message.empty_attribute_value - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{"empty":{"DataType":"String","StringValue":""}}'
probe sqs.send_message.invalid_number - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{"n":{"DataType":"Number","StringValue":"ten"}}'
probe sqs.send_message.unknown_data_type - sqs send-message --queue-url "$BADURL" --message-body x \
    --message-attributes '{"t":{"DataType":"Boolean","StringValue":"true"}}'
ELEVEN="{$(i=0; while [ $i -lt 11 ]; do [ $i -gt 0 ] && printf ','; printf '"a%s":{"DataType":"String","StringValue":"v"}' "$i"; i=$((i + 1)); done)}"
probe sqs.send_message.eleven_attributes - sqs send-message --queue-url "$BADURL" --message-body x --message-attributes "$ELEVEN"
probe sqs.receive_message.wait_21s - sqs receive-message --queue-url "$BADURL" --wait-time-seconds 21
probe sqs.receive_message.eleven_messages - sqs receive-message --queue-url "$BADURL" --max-number-of-messages 11
probe sqs.receive_message.visibility_over_12h - sqs receive-message --queue-url "$BADURL" --visibility-timeout 43201

# Batches.
probe sqs.send_batch.partial_failure '{successful:length(Successful),failed:Failed}' sqs send-message-batch --queue-url "$BADURL" \
    --entries '[{"Id":"good","MessageBody":"x"},{"Id":"empty","MessageBody":""},{"Id":"delay","MessageBody":"x","DelaySeconds":901}]'
probe sqs.send_batch.duplicate_ids - sqs send-message-batch --queue-url "$BADURL" \
    --entries '[{"Id":"a","MessageBody":"x"},{"Id":"a","MessageBody":"y"}]'
probe sqs.send_batch.invalid_id - sqs send-message-batch --queue-url "$BADURL" --entries '[{"Id":"bad id","MessageBody":"x"}]'
probe sqs.send_batch.empty - sqs send-message-batch --queue-url "$BADURL" --entries '[]'
ENTRIES="[$(i=0; while [ $i -lt 11 ]; do [ $i -gt 0 ] && printf ','; printf '{"Id":"m%s","MessageBody":"x"}' "$i"; i=$((i + 1)); done)]"
probe sqs.send_batch.eleven_entries - sqs send-message-batch --queue-url "$BADURL" --entries "$ENTRIES"
probe sqs.delete_batch.garbage_handles 'Failed' sqs delete-message-batch --queue-url "$BADURL" \
    --entries '[{"Id":"a","ReceiptHandle":"garbage"},{"Id":"b","ReceiptHandle":"garbage2"}]'
probe sqs.change_visibility_batch.garbage_handle 'Failed' sqs change-message-visibility-batch --queue-url "$BADURL" \
    --entries '[{"Id":"a","ReceiptHandle":"garbage","VisibilityTimeout":10}]'

# Size limits (a 1 KiB queue: body plus attribute name, type and value).
SQ="$P-small"
awscli sqs create-queue --queue-name "$SQ" --attributes MaximumMessageSize=1024 >/dev/null
SQURL="$(queue_url "$SQ")"
repeat x 1000 >"$WORK/body-1000"
repeat x 1024 >"$WORK/body-1024"
repeat x 1025 >"$WORK/body-1025"
probe sqs.size.body_at_limit MD5OfMessageBody sqs send-message --queue-url "$SQURL" --message-body "file://$WORK/body-1024"
probe sqs.size.body_over_limit MD5OfMessageBody sqs send-message --queue-url "$SQURL" --message-body "file://$WORK/body-1025"
probe sqs.size.with_attribute_at_limit MD5OfMessageBody sqs send-message --queue-url "$SQURL" --message-body "file://$WORK/body-1000" \
    --message-attributes '{"a":{"DataType":"String","StringValue":"'"$(repeat v 17)"'"}}'
probe sqs.size.with_attribute_over_limit MD5OfMessageBody sqs send-message --queue-url "$SQURL" --message-body "file://$WORK/body-1000" \
    --message-attributes '{"a":{"DataType":"String","StringValue":"'"$(repeat v 18)"'"}}'
probe sqs.size.batch_over_limit - sqs send-message-batch --queue-url "$SQURL" \
    --entries '[{"Id":"a","MessageBody":"'"$(repeat x 600)"'"},{"Id":"b","MessageBody":"'"$(repeat x 600)"'"}]'

# Purging and deleting.
probe sqs.purge - sqs purge-queue --queue-url "$SQURL"
probe sqs.purge.again - sqs purge-queue --queue-url "$SQURL"
probe sqs.delete_queue - sqs delete-queue --queue-url "$SQURL"
probe sqs.delete_queue.again - sqs delete-queue --queue-url "$SQURL"
probe sqs.deleted_queue.send - sqs send-message --queue-url "$SQURL" --message-body x
probe sqs.deleted_queue.get_queue_url QueueUrl sqs get-queue-url --queue-name "$SQ"
probe sqs.deleted_queue.create_again QueueUrl sqs create-queue --queue-name "$SQ" --attributes MaximumMessageSize=1024

# -- SQS: FIFO ----------------------------------------------------------------------------------

F="$P-f.fifo"
probe sqs.fifo.create_queue QueueUrl sqs create-queue --queue-name "$F" --attributes FifoQueue=true
FURL="$(queue_url "$F")"
probe sqs.fifo.default_attributes 'Attributes.{ContentBasedDeduplication:ContentBasedDeduplication,DeduplicationScope:DeduplicationScope,FifoThroughputLimit:FifoThroughputLimit}' \
    sqs get-queue-attributes --queue-url "$FURL" --attribute-names All
awscli sqs create-queue --queue-name "$P-fbad.fifo" --attributes FifoQueue=true >/dev/null
FBADURL="$(queue_url "$P-fbad.fifo")"
probe sqs.fifo.send.no_group - sqs send-message --queue-url "$FBADURL" --message-body x --message-deduplication-id d0
probe sqs.fifo.send.no_deduplication_id - sqs send-message --queue-url "$FBADURL" --message-body x --message-group-id g
probe sqs.fifo.send.delay - sqs send-message --queue-url "$FBADURL" --message-body x --message-group-id g --message-deduplication-id d0 --delay-seconds 2
probe sqs.fifo.send.group_id_129 - sqs send-message --queue-url "$FBADURL" --message-body x --message-group-id "$(repeat g 129)" --message-deduplication-id d0
probe sqs.fifo.send.group_id_with_space - sqs send-message --queue-url "$FBADURL" --message-body x --message-group-id "a b" --message-deduplication-id d0
probe sqs.fifo.send.deduplication_id_129 - sqs send-message --queue-url "$FBADURL" --message-body x --message-group-id g --message-deduplication-id "$(repeat d 129)"
probe sqs.fifo.send.first '{id:MessageId,sequence:SequenceNumber}' sqs send-message --queue-url "$FURL" --message-body '{"label":"m1"}' --message-group-id g --message-deduplication-id d1
FIRST_ID="$(last_field id)"
FIRST_SEQ="$(last_field sequence)"
probe sqs.fifo.send.duplicate '{id:MessageId,sequence:SequenceNumber}' sqs send-message --queue-url "$FURL" --message-body '{"label":"m1-again"}' --message-group-id g --message-deduplication-id d1
fact sqs.fifo.duplicate_returns_first_message_id "$([ "$(last_field id)" = "$FIRST_ID" ] && echo true || echo false)"
fact sqs.fifo.duplicate_returns_first_sequence_number "$([ "$(last_field sequence)" = "$FIRST_SEQ" ] && echo true || echo false)"
awscli sqs send-message --queue-url "$FURL" --message-body '{"label":"m2"}' --message-group-id g --message-deduplication-id d2 >/dev/null
probe sqs.fifo.receive 'Messages[].{body:Body,group:Attributes.MessageGroupId,dedup:Attributes.MessageDeduplicationId,sequence:Attributes.SequenceNumber,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$FURL" --attribute-names All --max-number-of-messages 10 --wait-time-seconds 2 --visibility-timeout 3
probe sqs.fifo.receive_while_group_in_flight "$COUNT" sqs receive-message --queue-url "$FURL" --max-number-of-messages 10 --wait-time-seconds 1
awscli sqs create-queue --queue-name "$P-fx.fifo" --attributes FifoQueue=true >/dev/null
FXURL="$(queue_url "$P-fx.fifo")"
awscli sqs send-message --queue-url "$FXURL" --message-body '{"label":"expiry"}' --message-group-id h --message-deduplication-id e1 >/dev/null
probe sqs.fifo.expiry.first_receive 'Messages[0].{handle:ReceiptHandle,body:Body,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$FXURL" --attribute-names All --max-number-of-messages 1 --wait-time-seconds 2 --visibility-timeout 2
FE1="$(last_field handle)"
sleep 3
probe sqs.fifo.expiry.second_receive 'Messages[0].{handle:ReceiptHandle,body:Body,receive_count:Attributes.ApproximateReceiveCount}' \
    sqs receive-message --queue-url "$FXURL" --attribute-names All --max-number-of-messages 1 --wait-time-seconds 2 --visibility-timeout 30
probe sqs.fifo.expiry.delete_first_handle - sqs delete-message --queue-url "$FXURL" --receipt-handle "$FE1"
probe sqs.fifo.expiry.change_visibility_first_handle - sqs change-message-visibility --queue-url "$FXURL" --receipt-handle "$FE1" --visibility-timeout 0
# A FIFO message made visible again (visibility 0) whose latest receipt handle is used once more.
awscli sqs send-message --queue-url "$FXURL" --message-body '{"label":"released"}' --message-group-id k --message-deduplication-id r1 >/dev/null
# The expiry message's group is still in flight: only this one can be received.
probe sqs.fifo.released.receive 'Messages[0].{handle:ReceiptHandle,body:Body}' \
    sqs receive-message --queue-url "$FXURL" --max-number-of-messages 10 --wait-time-seconds 2 --visibility-timeout 30
FR="$(last_field handle)"
probe sqs.fifo.released.change_visibility_zero - sqs change-message-visibility --queue-url "$FXURL" --receipt-handle "$FR" --visibility-timeout 0
sleep 1
probe sqs.fifo.released.change_visibility_again - sqs change-message-visibility --queue-url "$FXURL" --receipt-handle "$FR" --visibility-timeout 10
probe sqs.fifo.released.received_afterwards 'Messages[].Body' \
    sqs receive-message --queue-url "$FXURL" --max-number-of-messages 10 --wait-time-seconds 2
probe sqs.fifo.create_queue.per_group_limit_with_queue_scope QueueUrl sqs create-queue --queue-name "$P-g.fifo" \
    --attributes FifoQueue=true,DeduplicationScope=queue,FifoThroughputLimit=perMessageGroupId
probe sqs.fifo.create_queue.high_throughput QueueUrl sqs create-queue --queue-name "$P-h.fifo" \
    --attributes FifoQueue=true,DeduplicationScope=messageGroup,FifoThroughputLimit=perMessageGroupId

# -- SQS: dead-letter queues --------------------------------------------------------------------

DLQ="$P-dlq"
awscli sqs create-queue --queue-name "$DLQ" >/dev/null
DLQURL="$(queue_url "$DLQ")"
DLQARN="$(queue_arn "$DLQURL")"
REDRIVE="{\"deadLetterTargetArn\":\"$DLQARN\",\"maxReceiveCount\":\"1\"}"
probe sqs.redrive.create_source QueueUrl sqs create-queue --queue-name "$P-src" \
    --attributes "{\"RedrivePolicy\": $(json_string "$REDRIVE"), \"VisibilityTimeout\": \"1\"}"
SRCURL="$(queue_url "$P-src")"
probe sqs.redrive.policy_as_returned 'Attributes.RedrivePolicy' sqs get-queue-attributes --queue-url "$SRCURL" --attribute-names RedrivePolicy
SENT_ID="$(value MessageId sqs send-message --queue-url "$SRCURL" --message-body '{"label":"poison"}' \
    --message-attributes '{"tenant":{"DataType":"String","StringValue":"acme"}}')"
probe sqs.redrive.first_receive 'Messages[0].Attributes.ApproximateReceiveCount' sqs receive-message --queue-url "$SRCURL" --attribute-names All --wait-time-seconds 5
sleep 2
probe sqs.redrive.second_receive "$COUNT" sqs receive-message --queue-url "$SRCURL" --wait-time-seconds 3
probe sqs.redrive.in_dead_letter_queue 'Messages[0].{handle:ReceiptHandle,body:Body,receive_count:Attributes.ApproximateReceiveCount,source:Attributes.DeadLetterQueueSourceArn,attributes:MessageAttributes,message_id:MessageId}' \
    sqs receive-message --queue-url "$DLQURL" --attribute-names All --message-attribute-names All --wait-time-seconds 10
fact sqs.redrive.keeps_message_id "$([ "$(last_field message_id)" = "$SENT_ID" ] && echo true || echo false)"
FDLQARN="$(queue_arn "$FURL")"
probe sqs.redrive.fifo_target_for_standard QueueUrl sqs create-queue --queue-name "$P-src2" \
    --attributes "{\"RedrivePolicy\": $(json_string "{\"deadLetterTargetArn\":\"$FDLQARN\",\"maxReceiveCount\":3}")}"
probe sqs.redrive.missing_target QueueUrl sqs create-queue --queue-name "$P-src3" \
    --attributes "{\"RedrivePolicy\": $(json_string "{\"deadLetterTargetArn\":\"$DLQARN-missing\",\"maxReceiveCount\":3}")}"
probe sqs.redrive.max_receive_count_zero QueueUrl sqs create-queue --queue-name "$P-src4" \
    --attributes "{\"RedrivePolicy\": $(json_string "{\"deadLetterTargetArn\":\"$DLQARN\",\"maxReceiveCount\":0}")}"

# -- SNS: topics --------------------------------------------------------------------------------

T="$P-t"
probe sns.create_topic TopicArn sns create-topic --name "$T"
TARN="$(last_string)"
probe sns.create_topic.same TopicArn sns create-topic --name "$T"
MT="$P-mut"
MTARN="$(value TopicArn sns create-topic --name "$MT")"
probe sns.create_topic.different_attributes TopicArn sns create-topic --name "$MT" --attributes DisplayName=Other
probe sns.create_topic.invalid_name TopicArn sns create-topic --name "$P bad"
probe sns.create_topic.fifo_attribute_without_suffix TopicArn sns create-topic --name "$P-t2" --attributes FifoTopic=true
probe sns.create_topic.fifo_suffix_without_attribute TopicArn sns create-topic --name "$P-t3.fifo"
probe sns.topic_attribute_names 'sort(keys(Attributes))' sns get-topic-attributes --topic-arn "$TARN"
probe sns.topic_attributes 'Attributes.{DisplayName:DisplayName,SignatureVersion:SignatureVersion,TracingConfig:TracingConfig,SubscriptionsConfirmed:SubscriptionsConfirmed}' \
    sns get-topic-attributes --topic-arn "$TARN"
probe sns.topic_default_policy 'Attributes.Policy' sns get-topic-attributes --topic-arn "$TARN"
probe sns.topic_default_delivery_policy 'Attributes.EffectiveDeliveryPolicy' sns get-topic-attributes --topic-arn "$TARN"
probe sns.set_topic_attributes.fifo - sns set-topic-attributes --topic-arn "$MTARN" --attribute-name FifoTopic --attribute-value true
probe sns.set_topic_attributes.signature_version_3 - sns set-topic-attributes --topic-arn "$MTARN" --attribute-name SignatureVersion --attribute-value 3
probe sns.attributes_after_mutations 'Attributes.{DisplayName:DisplayName,FifoTopic:FifoTopic,SignatureVersion:SignatureVersion}' \
    sns get-topic-attributes --topic-arn "$MTARN"
probe sns.get_topic_attributes.missing - sns get-topic-attributes --topic-arn "$TARN-missing"
probe sns.publish.missing_topic MessageId sns publish --topic-arn "$TARN-missing" --message x
probe sns.publish.other_region MessageId sns publish --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T" --message x
probe sns.other_region.publish_batch - sns publish-batch --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T" \
    --publish-batch-request-entries '[{"Id":"a","Message":"x"}]'
probe sns.other_region.get_topic_attributes - sns get-topic-attributes --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T"
probe sns.other_region.list_subscriptions - sns list-subscriptions-by-topic --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T"
probe sns.other_region.subscribe - sns subscribe --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T" --protocol sqs \
    --notification-endpoint "arn:aws:sqs:$REGION:$ACCOUNT:$P-raw"
probe sns.other_region.set_topic_attributes - sns set-topic-attributes --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T" \
    --attribute-name DisplayName --attribute-value x
probe sns.other_region.delete_topic - sns delete-topic --topic-arn "arn:aws:sns:$OTHER_REGION:$ACCOUNT:$T"
probe sns.publish.foreign_account MessageId sns publish --topic-arn "arn:aws:sns:$REGION:$FOREIGN_ACCOUNT:$T" --message x
probe sns.publish.empty_message MessageId sns publish --topic-arn "$TARN" --message ""
repeat x 262144 >"$WORK/body-262144"
repeat x 262145 >"$WORK/body-262145"
probe sns.publish.at_limit 'MessageId != `null`' sns publish --topic-arn "$TARN" --message "file://$WORK/body-262144"
probe sns.publish.over_limit 'MessageId != `null`' sns publish --topic-arn "$TARN" --message "file://$WORK/body-262145"
probe sns.publish.eleven_attributes 'MessageId != `null`' sns publish --topic-arn "$TARN" --message x --message-attributes "$ELEVEN"
probe sns.publish.invalid_number_attribute 'MessageId != `null`' sns publish --topic-arn "$TARN" --message x \
    --message-attributes '{"n":{"DataType":"Number","StringValue":"ten"}}'
probe sns.publish.invalid_string_array 'MessageId != `null`' sns publish --topic-arn "$TARN" --message x \
    --message-attributes '{"a":{"DataType":"String.Array","StringValue":"not an array"}}'
probe sns.publish_batch.partial_failure '{successful:length(Successful),failed:Failed}' sns publish-batch --topic-arn "$TARN" \
    --publish-batch-request-entries '[{"Id":"good","Message":"x"},{"Id":"empty","Message":""}]'
probe sns.publish_batch.duplicate_ids - sns publish-batch --topic-arn "$TARN" \
    --publish-batch-request-entries '[{"Id":"a","Message":"x"},{"Id":"a","Message":"y"}]'
PENTRIES="[$(i=0; while [ $i -lt 11 ]; do [ $i -gt 0 ] && printf ','; printf '{"Id":"m%s","Message":"x"}' "$i"; i=$((i + 1)); done)]"
probe sns.publish_batch.eleven_entries - sns publish-batch --topic-arn "$TARN" --publish-batch-request-entries "$PENTRIES"

# -- SNS: subscriptions and delivery -------------------------------------------------------------

RAWQ="$P-raw"; WRAPQ="$P-wrapped"; FILTQ="$P-filter"
for name in "$RAWQ" "$WRAPQ" "$FILTQ"; do awscli sqs create-queue --queue-name "$name" >/dev/null; done
RAWURL="$(queue_url "$RAWQ")"; RAWARN="$(queue_arn "$RAWURL")"
WRAPURL="$(queue_url "$WRAPQ")"; WRAPARN="$(queue_arn "$WRAPURL")"
FILTURL="$(queue_url "$FILTQ")"; FILTARN="$(queue_arn "$FILTURL")"
allow_topic "$RAWURL" "$RAWARN" "$TARN"
allow_topic "$WRAPURL" "$WRAPARN" "$TARN"
probe sns.queue_policy_as_returned 'Attributes.Policy' sqs get-queue-attributes --queue-url "$RAWURL" --attribute-names Policy
probe sns.subscribe.raw SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs --notification-endpoint "$RAWARN" \
    --attributes RawMessageDelivery=true --return-subscription-arn
RAWSUB="$(last_string)"
probe sns.subscribe.wrapped SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs --notification-endpoint "$WRAPARN" --return-subscription-arn
probe sns.subscribe.same_again SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs --notification-endpoint "$RAWARN" \
    --attributes RawMessageDelivery=true --return-subscription-arn
fact sns.subscribe.same_again_returns_same_arn "$([ "$(last_string)" = "$RAWSUB" ] && echo true || echo false)"
DUPQ="$P-dup"
awscli sqs create-queue --queue-name "$DUPQ" >/dev/null
DUPARN="$(queue_arn "$(queue_url "$DUPQ")")"
awscli sns subscribe --topic-arn "$TARN" --protocol sqs --notification-endpoint "$DUPARN" --attributes RawMessageDelivery=true >/dev/null
probe sns.subscribe.different_attributes SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs --notification-endpoint "$DUPARN" \
    --attributes RawMessageDelivery=false --return-subscription-arn
probe sns.subscribe.after_different_attributes 'Subscriptions[?Endpoint==`'"$DUPARN"'`] | length(@)' sns list-subscriptions-by-topic --topic-arn "$TARN"
for arn in $(awscli sns list-subscriptions-by-topic --topic-arn "$TARN" --query "Subscriptions[?Endpoint=='$DUPARN'].SubscriptionArn" --output text 2>/dev/null); do
    awscli sns unsubscribe --subscription-arn "$arn" >/dev/null 2>&1
done
probe sns.subscribe.fifo_queue_to_standard_topic SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs \
    --notification-endpoint "$(queue_arn "$FURL")" --return-subscription-arn
# Where it was accepted, it would get in the way of the deliveries below.
case "$(last_string)" in arn:*) awscli sns unsubscribe --subscription-arn "$(last_string)" >/dev/null 2>&1 ;; esac
probe sns.subscription_attribute_names 'sort(keys(Attributes))' sns get-subscription-attributes --subscription-arn "$RAWSUB"
probe sns.subscription_attributes 'Attributes.{RawMessageDelivery:RawMessageDelivery,PendingConfirmation:PendingConfirmation,ConfirmationWasAuthenticated:ConfirmationWasAuthenticated,FilterPolicyScope:FilterPolicyScope}' \
    sns get-subscription-attributes --subscription-arn "$RAWSUB"
probe sns.subscription_principal 'Attributes.SubscriptionPrincipal' sns get-subscription-attributes --subscription-arn "$RAWSUB"
probe sns.list_subscriptions_by_topic 'sort_by(Subscriptions, &Endpoint)[].{protocol:Protocol,endpoint:Endpoint}' sns list-subscriptions-by-topic --topic-arn "$TARN"

PUBLISH_ATTRS='{"region":{"DataType":"String","StringValue":"eu"},"n":{"DataType":"Number","StringValue":"3.0"},"bin":{"DataType":"Binary","BinaryValue":"'"$BIN"'"},"arr":{"DataType":"String.Array","StringValue":"[\"a\",1,true]"}}'
receive_until sns.delivery.raw "$RAWURL" 'Messages[0].{body:Body,attributes:MessageAttributes.{region:region,n:n,bin:bin,arr:arr},md5_attributes:MD5OfMessageAttributes,system:sort(keys(Attributes))}' \
    awscli sns publish --topic-arn "$TARN" --message '{"label":"delivered"}' --subject "Probe subject" --message-attributes "$PUBLISH_ATTRS"
receive_until sns.delivery.wrapped "$WRAPURL" 'Messages[0].{body:Body,attributes:MessageAttributes}' \
    awscli sns publish --topic-arn "$TARN" --message '{"label":"delivered"}' --subject "Probe subject" --message-attributes "$PUBLISH_ATTRS"
probe sns.delivery.wrapped.notification_keys 'Messages[0].Body' sqs receive-message --queue-url "$WRAPURL" --wait-time-seconds 5

# Filter policies SNS refuses.
allow_topic "$FILTURL" "$FILTARN" "$TARN"
filter_case() {
    local name="$1" policy="$2" scope="${3:-}" attributes
    attributes="{\"FilterPolicy\": $(json_string "$policy")"
    [ -n "$scope" ] && attributes="$attributes, \"FilterPolicyScope\": \"$scope\""
    probe "sns.filter_policy.$name" SubscriptionArn sns subscribe --topic-arn "$TARN" --protocol sqs \
        --notification-endpoint "$FILTARN" --attributes "$attributes}" --return-subscription-arn
    local arn
    arn="$(last_string)"
    case "$arn" in arn:*) awscli sns unsubscribe --subscription-arn "$arn" >/dev/null 2>&1 ;; esac
}
filter_case unknown_operator '{"color":[{"glob":"r*"}]}'
filter_case six_keys '{"k0":["v"],"k1":["v"],"k2":["v"],"k3":["v"],"k4":["v"],"k5":["v"]}'
filter_case 151_combinations "{\"color\":[$(i=0; while [ $i -lt 151 ]; do [ $i -gt 0 ] && printf ','; printf '"c%s"' "$i"; i=$((i + 1)); done)]}"
filter_case numeric_not_a_number '{"price":[{"numeric":[">","ten"]}]}'
filter_case nested_in_attribute_scope '{"order":{"total":[1]}}'
filter_case empty_values '{"color":[]}'
filter_case exists_not_boolean '{"color":[{"exists":"yes"}]}'
filter_case flat_in_body_scope '{"total":[{"numeric":[">",1]}]}' MessageBody
filter_case valid_body_scope '{"order":{"total":[{"numeric":[">",1]}]}}' MessageBody
filter_case not_json '{"color": ["red"'

# Unsubscribing and deleting.
probe sns.unsubscribe - sns unsubscribe --subscription-arn "$RAWSUB"
probe sns.unsubscribe.again - sns unsubscribe --subscription-arn "$RAWSUB"
probe sns.get_subscription_attributes.unsubscribed - sns get-subscription-attributes --subscription-arn "$RAWSUB"

# -- SNS: FIFO topics ---------------------------------------------------------------------------

FT="$P-ft.fifo"
probe sns.fifo.create_topic TopicArn sns create-topic --name "$FT" --attributes FifoTopic=true
FTARN="$(last_string)"
probe sns.fifo.topic_attributes 'Attributes.{FifoTopic:FifoTopic,ContentBasedDeduplication:ContentBasedDeduplication,FifoThroughputScope:FifoThroughputScope}' \
    sns get-topic-attributes --topic-arn "$FTARN"
probe sns.fifo.publish.no_group - sns publish --topic-arn "$FTARN" --message x --message-deduplication-id d
probe sns.fifo.publish.no_deduplication_id - sns publish --topic-arn "$FTARN" --message x --message-group-id g
probe sns.fifo.publish.group_id_129 - sns publish --topic-arn "$FTARN" --message x --message-group-id "$(repeat g 129)" --message-deduplication-id d0
probe sns.fifo.publish.group_id_with_space - sns publish --topic-arn "$FTARN" --message x --message-group-id "a b" --message-deduplication-id d0
probe sns.fifo.publish.deduplication_id_129 - sns publish --topic-arn "$FTARN" --message x --message-group-id g --message-deduplication-id "$(repeat d 129)"
probe sns.fifo.publish_batch.group_id_with_space '{successful:length(Successful),failed:Failed}' sns publish-batch --topic-arn "$FTARN" \
    --publish-batch-request-entries '[{"Id":"a","Message":"x","MessageGroupId":"a b","MessageDeduplicationId":"d9"}]'
probe sns.fifo.publish.first '{id:MessageId,has_sequence:SequenceNumber != `null`}' sns publish --topic-arn "$FTARN" --message x --message-group-id g --message-deduplication-id d1
FTFIRST="$(last_field id)"
probe sns.fifo.publish.duplicate '{id:MessageId,has_sequence:SequenceNumber != `null`}' sns publish --topic-arn "$FTARN" --message y --message-group-id g --message-deduplication-id d1
fact sns.fifo.duplicate_returns_first_message_id "$([ "$(last_field id)" = "$FTFIRST" ] && echo true || echo false)"
FQ="$P-fsub.fifo"
awscli sqs create-queue --queue-name "$FQ" --attributes FifoQueue=true >/dev/null
FQURL="$(queue_url "$FQ")"; FQARN="$(queue_arn "$FQURL")"
allow_topic "$FQURL" "$FQARN" "$FTARN"
probe sns.fifo.subscribe 'SubscriptionArn != `null`' sns subscribe --topic-arn "$FTARN" --protocol sqs --notification-endpoint "$FQARN" \
    --attributes RawMessageDelivery=true --return-subscription-arn
FTN=0
fifo_publish() {
    FTN=$((FTN + 1))
    awscli sns publish --topic-arn "$FTARN" --message '{"label":"fifo"}' --message-group-id g --message-deduplication-id "delivery-$FTN"
}
receive_until sns.fifo.delivery "$FQURL" 'Messages[0].{body:Body,group:Attributes.MessageGroupId,has_sequence:Attributes.SequenceNumber != `null`,system:sort(keys(Attributes))}' \
    fifo_publish

# -- SNS: deleted topics ------------------------------------------------------------------------

probe sns.delete_topic - sns delete-topic --topic-arn "$TARN"
probe sns.delete_topic.again - sns delete-topic --topic-arn "$TARN"
probe sns.deleted_topic.publish MessageId sns publish --topic-arn "$TARN" --message x
probe sns.deleted_topic.subscriptions 'Subscriptions[].Endpoint' sns list-subscriptions-by-topic --topic-arn "$TARN"

# -- S3 -----------------------------------------------------------------------------------------

B="$P-b"
if [ "$REGION" = us-east-1 ]; then
    probe s3.create_bucket - s3api create-bucket --bucket "$B"
    probe s3.create_bucket.again - s3api create-bucket --bucket "$B"
else
    probe s3.create_bucket - s3api create-bucket --bucket "$B" --create-bucket-configuration "LocationConstraint=$REGION"
    probe s3.create_bucket.again - s3api create-bucket --bucket "$B" --create-bucket-configuration "LocationConstraint=$REGION"
    probe s3.create_bucket.without_location - s3api create-bucket --bucket "$P-b2"
fi
probe s3.create_bucket.invalid_name - s3api create-bucket --bucket "$P-Bad_Name"
probe s3.head_bucket - s3api head-bucket --bucket "$B"
probe s3.head_bucket.missing - s3api head-bucket --bucket "$P-missing"
probe s3.get_bucket_location - s3api get-bucket-location --bucket "$B"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.s3_head_existing_bucket - s3api head-bucket --bucket "$B"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.s3_get_bucket_location - s3api get-bucket-location --bucket "$B"
AWS_CLI="fake $AWS_CLI" probe credentials.invalid.s3_list_objects - s3api list-objects-v2 --bucket "$B" --max-keys 0
printf '%s' '{"label":"payload"}' >"$WORK/payload"
probe s3.put_object '{etag:ETag,encryption:ServerSideEncryption}' s3api put-object --bucket "$B" --key fanferro/m1/sha --body "$WORK/payload" --content-type application/json
probe s3.put_object.sse_s3 '{encryption:ServerSideEncryption}' s3api put-object --bucket "$B" --key fanferro/m2/sha --body "$WORK/payload" --server-side-encryption AES256
probe s3.put_object.sse_kms '{encryption:ServerSideEncryption,bucket_key:BucketKeyEnabled,key_is_set:SSEKMSKeyId != `null`}' s3api put-object --bucket "$B" --key fanferro/m3/sha --body "$WORK/payload" \
    --server-side-encryption aws:kms --ssekms-key-id alias/aws/s3 --bucket-key-enabled
probe s3.put_object.missing_bucket - s3api put-object --bucket "$P-missing" --key k --body "$WORK/payload"
probe s3.get_object '{length:ContentLength,type:ContentType}' s3api get-object --bucket "$B" --key fanferro/m1/sha "$WORK/downloaded"
probe s3.get_object.sse_kms '{length:ContentLength,encryption:ServerSideEncryption}' s3api get-object --bucket "$B" --key fanferro/m3/sha "$WORK/downloaded"
probe s3.get_object.missing_key - s3api get-object --bucket "$B" --key fanferro/missing "$WORK/downloaded"
probe s3.get_object.missing_bucket - s3api get-object --bucket "$P-missing" --key k "$WORK/downloaded"
probe s3.head_object.missing_key - s3api head-object --bucket "$B" --key fanferro/missing
probe s3.delete_object.missing_key - s3api delete-object --bucket "$B" --key fanferro/missing

# presigned URLs, fetched as fanferro fetches them (plain GET).
fetch() {
    local name="$1" url="$2" status code
    status="$(curl -s -o "$WORK/fetched" -w '%{http_code}' "$url")"
    code="$(sed -n -E 's/.*<Code>([^<]*)<\/Code>.*/\1/p' "$WORK/fetched" | head -n 1)"
    fact "$name" "http $status${code:+ $code}"
}
fetch s3.presigned.valid "$(awscli s3 presign "s3://$B/fanferro/m1/sha" --expires-in 300)"
fetch s3.presigned.sse_kms "$(awscli s3 presign "s3://$B/fanferro/m3/sha" --expires-in 300)"
fetch s3.presigned.missing_key "$(awscli s3 presign "s3://$B/fanferro/missing" --expires-in 300)"
fetch s3.presigned.missing_bucket "$(awscli s3 presign "s3://$P-missing/k" --expires-in 300)"
EXPIRING="$(awscli s3 presign "s3://$B/fanferro/m1/sha" --expires-in 1)"
sleep 3
fetch s3.presigned.expired "$EXPIRING"
fact s3.presigned.url_shape "$(awscli s3 presign "s3://$B/fanferro/m1/sha" --expires-in 60 | sed -E 's/\?.*/?<query>/')"

echo "done" >&2
