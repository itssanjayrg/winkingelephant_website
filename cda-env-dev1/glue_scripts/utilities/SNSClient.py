from __future__ import annotations

import dataclasses
from textwrap import dedent

import structlog
from boto3.session import Session as AWSSession
from botocore.client import BaseClient


@dataclasses.dataclass
class SNSClient:
    _client: BaseClient | None
    _topic_arn: str | None
    _logger: structlog.BoundLogger

    @staticmethod
    def get_topic_arn(
        client: BaseClient,
        topic_name: str,
        logger: structlog.BoundLogger,
    ):
        response = client.list_topics()
        topics = response["Topics"]
        topic_arns = [
            topic["TopicArn"]
            for topic in topics
            if topic["TopicArn"].split(":")[-1] == topic_name
        ]

        if len(topic_arns) == 0:
            exception = f"SNS Topic Not Found: {topic_name}"
            logger.exception(exception)
            raise ValueError(exception)

        if len(topic_arns) > 1:
            exception = f"Multiple SNS Topic Matches Found: {topic_name}\n{', '.join(topic_arns)}"
            logger.exception(exception)
            raise ValueError(exception)

        return topic_arns[0]

    @classmethod
    def factory(
        cls,
        session: AWSSession,
        topic_name: str | None,
        logger: structlog.BoundLogger,
    ) -> SNSClient:
        if topic_name is None:
            return cls(_client=None, _topic_arn=topic_name, _logger=logger)

        # This SNS endpoint URL resides in the Networking account and won't need to change.
        sns_endpoint_url = (
            "https://vpce-083c550c684051773-u0gbdnwd.sns.us-east-1.vpce.amazonaws.com"
        )

        client = session.client(
            "sns",
            endpoint_url=sns_endpoint_url,
        )
        topic_arn = cls.get_topic_arn(
            client=client, topic_name=topic_name, logger=logger
        )
        logger = logger.bind(topic_arn=topic_arn)
        logger.info("returning SNSClient")
        return cls(_client=client, _topic_arn=topic_arn, _logger=logger)

    def send_notification(self, subject: str, message: str):
        if len(subject) > 100:
            self._logger.info(
                "truncating subject to 100 character limit", subject=subject
            )
            subject = subject[:100]
        self._logger.info(
            "publishing SNS message", subject=subject, sns_message=message
        )
        if self._client is None:
            return
        self._client.publish(
            TopicArn=self._topic_arn,
            Message=dedent(str(message)),
            Subject=str(subject),
        )
