"""CloudWatch alarms: notifications to SNS and state changes through EventBridge."""

from __future__ import annotations

import json
from collections.abc import Mapping  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from typing import Any, ClassVar

from fanferro.aws._document import AwsDocument, _at
from fanferro.message_types import message_type


@dataclass(frozen=True, slots=True, kw_only=True)
class CloudWatchDimension(AwsDocument):
    """A dimension of an alarm's metric."""

    name: str
    value: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CloudWatchAlarmTrigger(AwsDocument):
    """What a metric alarm evaluates (``Trigger`` of its SNS notification)."""

    metric_name: str | None = field(default=None, metadata=_at("MetricName"))
    namespace: str | None = field(default=None, metadata=_at("Namespace"))
    statistic_type: str | None = field(default=None, metadata=_at("StatisticType"))
    statistic: str | None = field(default=None, metadata=_at("Statistic"))
    extended_statistic: str | None = field(default=None, metadata=_at("ExtendedStatistic"))
    unit: str | None = field(default=None, metadata=_at("Unit"))
    dimensions: tuple[CloudWatchDimension, ...] = field(default=(), metadata=_at("Dimensions"))
    period: int | None = field(default=None, metadata=_at("Period"))
    evaluation_periods: int | None = field(default=None, metadata=_at("EvaluationPeriods"))
    datapoints_to_alarm: int | None = field(default=None, metadata=_at("DatapointsToAlarm"))
    comparison_operator: str | None = field(default=None, metadata=_at("ComparisonOperator"))
    threshold: float | None = field(default=None, metadata=_at("Threshold"))
    treat_missing_data: str | None = field(default=None, metadata=_at("TreatMissingData"))
    metrics: tuple[Mapping[str, Any], ...] = field(default=(), metadata=_at("Metrics"))
    """The metrics and expressions of a metric math alarm."""


@dataclass(frozen=True, slots=True, kw_only=True)
class CloudWatchAlarm(AwsDocument):
    """A CloudWatch alarm's notification to an SNS topic (message type ``aws.cloudwatch.alarm``)."""

    aws_message_type: ClassVar[str | None] = "aws.cloudwatch.alarm"

    alarm_name: str = field(metadata=_at("AlarmName"))
    new_state_value: str = field(metadata=_at("NewStateValue"))
    """``ALARM``, ``OK`` or ``INSUFFICIENT_DATA``."""
    new_state_reason: str | None = field(default=None, metadata=_at("NewStateReason"))
    old_state_value: str | None = field(default=None, metadata=_at("OldStateValue"))
    state_change_time: datetime | None = field(default=None, metadata=_at("StateChangeTime"))
    alarm_description: str | None = field(default=None, metadata=_at("AlarmDescription"))
    alarm_arn: str | None = field(default=None, metadata=_at("AlarmArn"))
    aws_account_id: str | None = field(default=None, metadata=_at("AWSAccountId"))
    region: str | None = field(default=None, metadata=_at("Region"))
    """The region's name, e.g. ``US East (N. Virginia)``."""
    alarm_configuration_updated_timestamp: datetime | None = field(
        default=None, metadata=_at("AlarmConfigurationUpdatedTimestamp")
    )
    alarm_rule: str | None = field(default=None, metadata=_at("AlarmRule"))
    """The rule of a composite alarm."""
    trigger: CloudWatchAlarmTrigger | None = field(default=None, metadata=_at("Trigger"))
    ok_actions: tuple[str, ...] = field(default=(), metadata=_at("OKActions"))
    alarm_actions: tuple[str, ...] = field(default=(), metadata=_at("AlarmActions"))
    insufficient_data_actions: tuple[str, ...] = field(default=(), metadata=_at("InsufficientDataActions"))

    @property
    def alarming(self) -> bool:
        """Whether the alarm went into the ``ALARM`` state."""
        return self.new_state_value == "ALARM"

    @property
    def ok(self) -> bool:
        """Whether the alarm went back to ``OK``."""
        return self.new_state_value == "OK"


@dataclass(frozen=True, slots=True, kw_only=True)
class CloudWatchAlarmState(AwsDocument):
    """A state of an alarm."""

    value: str
    """``ALARM``, ``OK`` or ``INSUFFICIENT_DATA``."""
    reason: str | None = None
    reason_data: str | None = None
    """Details of the evaluation, a JSON document (see :meth:`reason_data_json`)."""
    timestamp: datetime | None = None
    actions_suppressed_by: str | None = None
    actions_suppressed_reason: str | None = None

    def reason_data_json(self) -> Any:
        """The reason data, parsed (``None`` when there is none)."""
        return json.loads(self.reason_data) if self.reason_data else None


@message_type("aws.cloudwatch.alarm_state_change", "aws.cloudwatch.alarm_contributor_state_change")
@dataclass(frozen=True, slots=True, kw_only=True)
class CloudWatchAlarmStateChange(AwsDocument):
    """A CloudWatch alarm's state changed (EventBridge; ``aws.cloudwatch.alarm_state_change``).

    Contributors of multi time series alarms change state with
    ``aws.cloudwatch.alarm_contributor_state_change`` (and :attr:`alarm_contributor`).
    """

    aws_message_type: ClassVar[str | None] = "aws.cloudwatch.alarm_state_change"

    alarm_name: str
    state: CloudWatchAlarmState
    previous_state: CloudWatchAlarmState | None = None
    configuration: Mapping[str, Any] = field(default_factory=dict)
    """The alarm's configuration (``description``, ``metrics``, ``alarmRule``...)."""
    alarm_contributor: Mapping[str, Any] | None = None
    alarm_tags: Mapping[str, str] = field(default_factory=dict)

    @property
    def alarming(self) -> bool:
        """Whether the alarm went into the ``ALARM`` state."""
        return self.state.value == "ALARM"

    @property
    def ok(self) -> bool:
        """Whether the alarm went back to ``OK``."""
        return self.state.value == "OK"

    def __fanferro_message_type__(self) -> str:
        """The message type of this change: of the alarm, or of one of its contributors."""
        if self.alarm_contributor is not None:
            return "aws.cloudwatch.alarm_contributor_state_change"
        return "aws.cloudwatch.alarm_state_change"
