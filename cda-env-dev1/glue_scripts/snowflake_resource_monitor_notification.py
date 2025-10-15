from __future__ import annotations

import dataclasses
import logging
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from typing import ClassVar

import structlog
from boto3.session import Session as AWSSession
from snowflake.snowpark import Session as SPSession

from utilities.SNSClient import SNSClient
from utilities.oauth_utils import get_assumed_role_session, get_token
from utilities.logging_utils import get_structlog_logger

try:
    GLUE_JOB = True
    # noinspection PyUnresolvedReferences
    from awsglue.transforms import *

    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions

    # noinspection PyUnresolvedReferences
    from pyspark.context import SparkContext

    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job

    # noinspection PyUnresolvedReferences
    from awsglue import DynamicFrame
except ModuleNotFoundError:
    GLUE_JOB = False

for noisy_logger in [
    "snowflake",
    "boto3",
    "botocore",
    "urllib3",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


def get_snowpark_session(
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    authorization_token: str,
) -> SPSession:
    snowflake_params = {
        "account": snowflake_account,
        "user": snowflake_user,
        "role": snowflake_role,
        "database": snowflake_database,
        "warehouse": snowflake_warehouse,
        "authenticator": "oauth",
        "token": authorization_token,
    }
    return SPSession.builder.configs(snowflake_params).create()


TWOPLACES = Decimal(10) ** -2  # same as Decimal('0.01')


def get_percentage_decimal(pct: str) -> Decimal | None:
    if pct is None or pct == "":
        return None
    try:
        suspend_at_int = Decimal(pct.strip().replace("%", ""))
        return suspend_at_int / 100
    except Exception as exc:
        raise Exception(f"Failed to convert percentage to int: {pct}") from exc


@dataclasses.dataclass
class ResourceMonitor:
    width: ClassVar[int] = 50
    fill_char: ClassVar[str] = "▓"
    empty_char: ClassVar[str] = "░"
    monitor_name: str
    credit_quota: Decimal
    used_credits: Decimal
    remaining_credits: Decimal
    level: str
    frequency: str
    start_time: datetime
    end_time: datetime
    notify_at: str
    suspend_at_pct: Decimal | None
    suspend_immediately_at_pct: Decimal | None
    created_on: datetime
    owner: str
    comment: str
    notify_users: str

    @property
    def frequency_duration(self) -> timedelta:
        if self.frequency == "WEEKLY":
            return timedelta(weeks=1)
        elif self.frequency == "DAILY":
            return timedelta(days=1)
        else:
            raise Exception(f"unhandled frequency: {self.frequency}")

    @property
    def time_from_start(self) -> timedelta:
        total_time_from_start = (
            datetime.now(tz=self.start_time.tzinfo) - self.start_time
        )
        frequency_multiple = total_time_from_start // self.frequency_duration
        result = total_time_from_start - (frequency_multiple * self.frequency_duration)
        return result

    @property
    def percent_from_start(self) -> Decimal:
        return Decimal(self.time_from_start / self.frequency_duration)

    @property
    def used_percent(self) -> Decimal:
        return self.used_credits / self.credit_quota

    @property
    def suspend_at_credits(self) -> Decimal | None:
        if self.suspend_at_pct is None:
            return None
        return self.credit_quota * self.suspend_at_pct

    @property
    def suspend_immediately_at_credits(self) -> Decimal | None:
        if self.suspend_immediately_at_pct is None:
            return None
        return self.credit_quota * self.suspend_immediately_at_pct

    def get_progress_bar(self, fill_percentage: Decimal) -> str:
        fill_count = round(fill_percentage * self.width)
        bar = list((self.fill_char * fill_count).ljust(self.width, self.empty_char))

        if self.suspend_at_pct is not None and self.suspend_at_pct != 0:
            suspend_at_place = int(self.suspend_at_pct * self.width)
            if suspend_at_place < len(bar):
                bar[suspend_at_place] = "⚠️"
            elif suspend_at_place == len(bar):
                bar.append("⚠️")

        if (
            self.suspend_immediately_at_pct is not None
            and self.suspend_immediately_at_pct != 0
        ):
            suspend_immediately_at_place = int(
                self.suspend_immediately_at_pct * self.width
            )
            if suspend_immediately_at_place < len(bar):
                bar[suspend_immediately_at_place] = "🛑"
            elif suspend_immediately_at_place == len(bar):
                bar.append("🛑")

        return "".join(bar)

    @property
    def time_progress_bar(self) -> str:
        return self.get_progress_bar(fill_percentage=self.percent_from_start)

    @property
    def credit_progress_bar(self) -> str:
        return self.get_progress_bar(fill_percentage=self.used_percent)

    @property
    def status(self) -> str:
        status_contents = {
            "Resource Monitor Name": self.monitor_name,
            "Monitor Frequency": self.frequency,
            "Frequency Start Time": self.start_time,
            "Credit Limit": self.credit_quota.quantize(TWOPLACES),
            "Credits Consumed": self.used_credits.quantize(TWOPLACES),
        }

        if self.suspend_at_credits is not None:
            status_contents["Suspend At"] = self.suspend_at_credits.quantize(TWOPLACES)

        if self.suspend_immediately_at_credits is not None:
            status_contents[
                "Suspend Immediately At"
            ] = self.suspend_immediately_at_credits.quantize(TWOPLACES)

        status_list = [
            f"{header}: {value}" for header, value in status_contents.items()
        ]
        status_list.extend(["Time:", self.time_progress_bar])
        status_list.extend(["Credits:", self.credit_progress_bar])
        status_list.append("-" * self.width)
        return "\n".join(status_list)

    @classmethod
    def factory(cls, **kwargs) -> "ResourceMonitor":
        suspend_at_pct = get_percentage_decimal(pct=kwargs.pop("suspend_at"))
        suspend_immediately_at_pct = get_percentage_decimal(
            pct=kwargs.pop("suspend_immediately_at")
        )
        credit_quota = Decimal(kwargs.pop("credit_quota"))
        used_credits = Decimal(kwargs.pop("used_credits"))
        remaining_credits = Decimal(kwargs.pop("remaining_credits"))

        return cls(
            monitor_name=kwargs.pop("name"),
            credit_quota=credit_quota,
            used_credits=used_credits,
            remaining_credits=remaining_credits,
            suspend_at_pct=suspend_at_pct,
            suspend_immediately_at_pct=suspend_immediately_at_pct,
            **kwargs,
        )


def main(
    logger: structlog.BoundLogger,
    sns_topic_name: str,
    aws_account_number: str,
    aws_session: AWSSession,
    secret_name: str,
    token_endpoint: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
):
    authorization_token = get_token(
        session=aws_session,
        secret_name=secret_name,
        token_endpoint=token_endpoint,
        role_name=snowflake_role,
    )
    snowpark_session = get_snowpark_session(
        snowflake_account=snowflake_account,
        snowflake_user=snowflake_user,
        snowflake_database=snowflake_database,
        snowflake_warehouse=snowflake_warehouse,
        snowflake_role=snowflake_role,
        authorization_token=authorization_token,
    )
    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )
    df = snowpark_session.sql("SHOW RESOURCE MONITORS;")
    resource_monitors = [
        ResourceMonitor.factory(**row.as_dict()) for row in df.to_local_iterator()
    ]
    all_statuses = "\n\n".join([rm.status for rm in resource_monitors])
    message = f"{aws_account_number} Environment Resource Monitor Status Report:\n\n{all_statuses}"
    sns_client.send_notification(
        subject=f"{aws_account_number}: Resource Monitor Status Report", message=message
    )
    snowpark_session.close()


if __name__ == "__main__" and GLUE_JOB:
    # @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "sns_topic_name",
            "token_endpoint",
            "secret_name",
            "aws_account_number",
            "region_name",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "snowflake_role",
        ],
    )
    sc = SparkContext()
    glue_context = GlueContext(sc)
    structlogger = get_structlog_logger(glue_context.get_logger())
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["aws_account_number"], args)

    main(
        logger=structlogger,
        secret_name=args["secret_name"],
        token_endpoint=args["token_endpoint"],
        sns_topic_name=(
            args["sns_topic_name"] if args["sns_topic_name"] != "N/A" else None
        ),
        aws_account_number=args["aws_account_number"],
        aws_session=AWSSession(region_name=args["region_name"]),
        snowflake_account=args["snowflake_account"],
        snowflake_user=args["snowflake_user"],
        snowflake_database=args["snowflake_database"],
        snowflake_warehouse=args["snowflake_warehouse"],
        snowflake_role=args["snowflake_role"],
    )

    job.commit()
elif __name__ == "__main__" and not GLUE_JOB:
    structlogger = get_structlog_logger(logger=_logger)
    assumed_role_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )
    main(
        logger=structlogger,
        sns_topic_name="sns_topic_name",
        secret_name="dev-aws-glue",
        token_endpoint="https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token",
        aws_account_number="aws_account_number",
        aws_session=assumed_role_session,
        snowflake_account="encova-dev.privatelink",
        snowflake_user="0oa25sh1ca4BsKnGC0h8",
        snowflake_database="DEV1E2E",
        snowflake_warehouse="WH_GLUE",
        snowflake_role="DEV1E2E__GLUE__SVC_ROLE",
    )
