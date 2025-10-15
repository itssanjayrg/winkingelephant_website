from __future__ import annotations

import dataclasses
import logging
import os
import sys
from threading import Lock
from time import sleep
from collections.abc import Generator

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars
from boto3.s3.transfer import (
    create_transfer_manager,
    TransferConfig,
)
from s3transfer.manager import TransferManager
from boto3.session import Session as AWSSession
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from s3transfer.subscribers import BaseSubscriber

from utilities.SNSClient import SNSClient
from utilities.oauth_utils import get_assumed_role_session
from utilities.logging_utils import get_structlog_logger

try:
    IS_GLUE_JOB = True
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
except ModuleNotFoundError as e:
    IS_GLUE_JOB = False

DELETE_BATCH_SIZE = 1_000

logging.basicConfig(
    format="%(asctime)s : %(filename)s - %(funcName)s : %(lineno)d : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)

_logger = logging.getLogger(__name__)


class MyProgressCallbackInvoker(BaseSubscriber):
    def __init__(self, logger: structlog.BoundLogger):
        self.logger = logger

    def on_done(self, future, **kwargs):
        self.logger.debug(
            f"copy successful",
            src_bucket=future.meta.call_args.copy_source["Bucket"],
            src_key=future.meta.call_args.copy_source["Key"],
            dest_bucket=future.meta.call_args.bucket,
            dest_key=future.meta.call_args.key,
            object_bytes="{:,}".format(future.meta.size),
        )


@dataclasses.dataclass(kw_only=True)
class Handler:
    logger: structlog.BoundLogger
    sns_client: SNSClient
    s3_client: BaseClient
    transfer_manager: TransferManager
    environment: str
    file_extension: str | None = None
    src_bucket: str
    src_prefix: str | None = None
    dest_bucket: str
    dest_prefix: str | None = None

    file_count_lock: Lock = Lock()
    file_count: int = 0

    total_object_bytes_lock: Lock = Lock()
    total_object_bytes: int = 0

    @classmethod
    def factory(
        cls, logger: structlog.BoundLogger, src_bucket: str, dest_bucket: str, **kwargs
    ) -> Handler:
        logger = logger.bind(
            src_bucket=src_bucket,
            dest_bucket=dest_bucket,
        )
        return cls(
            logger=logger, src_bucket=src_bucket, dest_bucket=dest_bucket, **kwargs
        )

    def increment_file_count(self):
        with self.file_count_lock:
            self.file_count += 1

    def append_total_object_bytes(self, object_bytes: int):
        with self.total_object_bytes_lock:
            self.total_object_bytes += object_bytes

    def copy_file(
        self,
        src_path: str,
        dest_path: str,
        subscriber: MyProgressCallbackInvoker,
    ) -> None:
        copy_source = {"Bucket": self.src_bucket, "Key": src_path}
        max_retries = 10
        delay = 2  # initial delay
        delay_incr = 2  # additional delay in each loop

        for retry_count in range(max_retries + 1):
            try:
                head_object = self.s3_client.head_object(
                    Bucket=self.src_bucket,
                    Key=src_path,
                )

                object_bytes = head_object["ContentLength"]

                bind_contextvars(
                    retry_count=retry_count,
                    src_path=src_path,
                    dest_path=dest_path,
                    delay=delay,
                    object_bytes="{:,}".format(object_bytes),
                )

                self.increment_file_count()
                self.append_total_object_bytes(object_bytes=object_bytes)

                self.logger.debug("attempting copy")
                self.transfer_manager.copy(
                    copy_source,
                    self.dest_bucket,
                    dest_path,
                    subscribers=[subscriber],
                )

                return None

            except ClientError as err:
                if retry_count == max_retries:
                    self.logger.exception(f"copy failed terminally")
                    raise err

                error = err.response.get("Error")
                if not error:
                    self.logger.exception(f"copy failed terminally")
                    raise err

                code = error.get("Code", "")
                if code != "SlowDown":
                    self.logger.exception("copy failed terminally")
                    raise err

                self.logger.exception("copy failed - retrying")
                sleep(delay)
                delay += delay_incr
            finally:
                unbind_contextvars(
                    "retry_count", "src_path", "dest_path", "delay", "object_bytes"
                )
        raise Exception("unreachable code")

    def copy_all_files(
        self, subscriber: MyProgressCallbackInvoker
    ) -> Generator[list[dict[str, str]], None, None]:
        prefix = self.src_prefix
        if prefix == "":
            prefix = None

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.src_bucket,
            Prefix=prefix,
        )

        search_param = f"Contents[][]"
        if self.file_extension is not None:
            search_param = f"Contents[?ends_with(Key, '.{self.file_extension}')][]"

        # Filter results for parquet files only.
        parquet_objects = page_iterator.search(search_param)

        delete_objects: list[dict[str, str]] = []

        for s3_object in parquet_objects:
            if s3_object is None:
                continue
            key = s3_object["Key"]

            if self.dest_prefix == "" or self.dest_prefix is None:
                dest_path = key
            else:
                dest_path = f"{self.dest_prefix}/{key}"

            self.copy_file(
                src_path=key,
                dest_path=dest_path,
                subscriber=subscriber,
            )
            delete_objects.append({"Key": key})

        # Wait for all s3 files to be transferred before continuing with the load
        self.transfer_manager.shutdown(cancel=False)
        object_bytes_sum = "{:,}".format(self.total_object_bytes)

        object_bytes_avg = "0"
        if self.file_count > 0:
            object_bytes_avg = "{:,.2f}".format(
                self.total_object_bytes / self.file_count
            )

        self.logger.info(
            f"all files copied",
            object_bytes_sum=object_bytes_sum,
            file_count=self.file_count,
            object_bytes_avg=object_bytes_avg,
        )

        for pos in range(0, len(delete_objects), DELETE_BATCH_SIZE):
            yield delete_objects[pos : pos + DELETE_BATCH_SIZE]

    def copy_and_delete_all_files(self, subscriber: MyProgressCallbackInvoker) -> None:
        if self.src_prefix == "" or self.src_prefix is None:
            src_path = self.src_bucket
        else:
            src_path = f"{self.src_bucket}/{self.src_prefix}"

        if self.dest_prefix == "" or self.dest_prefix is None:
            dest_path = self.dest_bucket
        else:
            dest_path = f"{self.dest_bucket}/{self.dest_prefix}"

        try:
            for delete_batch in self.copy_all_files(subscriber=subscriber):
                self.s3_client.delete_objects(
                    Bucket=self.src_bucket, Delete={"Objects": delete_batch}
                )

            object_bytes_sum = "{:,}".format(self.total_object_bytes)

            object_bytes_avg = "0"
            if self.file_count > 0:
                object_bytes_avg = "{:,.2f}".format(
                    self.total_object_bytes / self.file_count
                )

            self.logger.info("all files deleted")
            self.sns_client.send_notification(
                subject=f"{self.environment}: Transfer {src_path} -> {dest_path}",
                message=f"Hi team, \n\nS3 objects were transferred from {src_path} to {dest_path} "
                f"in the {self.environment} environment.\n\n"
                f"Total objects: {self.file_count}\n"
                f"Total object bytes: {object_bytes_sum}\n"
                f"Average object bytes: {object_bytes_avg}\n",
            )
        except Exception as exc:
            self.logger.exception("S3 migration failed")
            self.sns_client.send_notification(
                subject=f"EXCEPTION {self.environment}: Transfer {src_path} -> {dest_path}",
                message=f"Hi team, \n\nAn exception occurred when transferring S3 objects "
                f"from {src_path} to {dest_path}"
                f"in the {self.environment} environment.\n\n\n{str(exc)}",
            )


def main(
    logger: structlog.BoundLogger,
    # Hard-coded parameters
    job_name: str,
    sns_topic_name: str,
    env_name: str,
    aws_session: AWSSession,
    # Dynamic parameters
    spark_log_level: str,
    src_bucket: str,
    dest_bucket: str,
    file_extension: str | None = None,
    src_prefix: str | None = None,
    dest_prefix: str | None = None,
    max_s3_workers: str | None = None,
):
    if max_s3_workers is None:
        max_s3_workers = "25"
    max_s3_workers = int(max_s3_workers)

    logger.info(
        f"{job_name} has started",
        src_bucket=src_bucket,
        dest_bucket=dest_bucket,
    )

    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )

    boto_config = Config(max_pool_connections=max_s3_workers)
    s3_client = aws_session.client("s3", config=boto_config)
    transfer_config = TransferConfig(
        use_threads=True,
        max_concurrency=max_s3_workers,
    )
    transfer_manager = create_transfer_manager(client=s3_client, config=transfer_config)

    handler = Handler.factory(
        logger=logger,
        sns_client=sns_client,
        s3_client=s3_client,
        transfer_manager=transfer_manager,
        environment=env_name,
        file_extension=file_extension,
        src_bucket=src_bucket,
        src_prefix=src_prefix,
        dest_bucket=dest_bucket,
        dest_prefix=dest_prefix,
    )
    subscriber = MyProgressCallbackInvoker(logger=logger)
    handler.copy_and_delete_all_files(subscriber=subscriber)


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
            "sns_topic_name",
            "env_name",
            # Dynamic parameters
            "spark_log_level",
            "max_s3_workers",
            "file_extension",
            "src_bucket",
            "src_prefix",
            "dest_bucket",
            "dest_prefix",
        ],
    )
    sc = SparkContext()
    sc.setLogLevel(args["spark_log_level"])
    glue_context = GlueContext(sc)
    structlogger = get_structlog_logger(glue_context.get_logger())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["src_bucket"], args)

    session = AWSSession(region_name="us-east-1")

    main(
        logger=structlogger,
        # Hard-coded parameters
        job_name=args["JOB_NAME"],
        sns_topic_name=None if args["sns_topic_name"] == "" else args["sns_topic_name"],
        env_name=args["env_name"],
        # Dynamic parameters
        spark_log_level=args["spark_log_level"],
        max_s3_workers=args["max_s3_workers"],
        file_extension=(
            None if args["file_extension"] == "__null__" else args["file_extension"]
        ),
        src_bucket=args["src_bucket"],
        src_prefix=None if args["src_prefix"] == "__null__" else args["src_prefix"],
        dest_bucket=args["dest_bucket"],
        dest_prefix=None if args["dest_prefix"] == "__null__" else args["dest_prefix"],
        aws_session=session,
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    args = {
        ## Hard-coded parameters
        "JOB_NAME": "S3_Migration",
        "sns_topic_name": "",
        "env_name": "dev1",
        # Dynamic parameters
        "spark_log_level": "DEBUG",
        "file_extension": "snappy.parquet",
        "src_bucket": "encova-aws-use1-dev1-s3-cda-cc-0001",
        "src_prefix": "__copy_into__/__archive__/",
        "dest_bucket": "encova-aws-use1-dev1-archive-0001",
        "dest_prefix": "cm",
    }

    _logger.setLevel(args["spark_log_level"])
    structlogger = get_structlog_logger(logger=_logger)

    assumed_role_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )

    main(
        logger=structlogger,
        # Hard-coded parameters
        job_name=args["JOB_NAME"],
        sns_topic_name=None if args["sns_topic_name"] == "" else args["sns_topic_name"],
        env_name=args["env_name"],
        # Dynamic parameters
        spark_log_level=args["spark_log_level"],
        max_s3_workers=args.get("max_s3_workers"),
        file_extension=(
            None if args["file_extension"] == "__null__" else args["file_extension"]
        ),
        src_bucket=args["src_bucket"],
        src_prefix=None if args["src_prefix"] == "__null__" else args["src_prefix"],
        dest_bucket=args["dest_bucket"],
        dest_prefix=None if args["dest_prefix"] == "__null__" else args["dest_prefix"],
        aws_session=assumed_role_session,
    )
