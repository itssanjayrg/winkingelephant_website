from __future__ import annotations

import json

import structlog
import dataclasses
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep, perf_counter
from typing import ClassVar, Any, Generator

from boto3 import set_stream_logger
from boto3.session import Session as AWSSession
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator

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

logging.basicConfig(
    format="%(asctime)s : %(filename)s - %(funcName)s : %(lineno)d : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)

_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Table:
    logger: structlog.BoundLogger
    table_name: str
    core_center: str

    src_cda_schema_id: str
    src_cda_folder: int
    src_total_records_count: int
    src_bucket_name: str
    src_prefix: str

    dest_bucket_name: str
    dest_prefix: str

    def get_table_path(self, prefix: str) -> str:
        if prefix == "":
            return self.table_name
        return f"{prefix}/{self.table_name}"

    @property
    def src_table_path(self) -> str:
        return self.get_table_path(prefix=self.src_prefix)

    @property
    def dest_table_path(self) -> str:
        return self.get_table_path(prefix=self.dest_prefix)

    @property
    def src_full_path(self) -> str:
        return f"{self.src_bucket_name}/{self.src_table_path}"

    @property
    def dest_full_path(self) -> str:
        return f"{self.dest_bucket_name}/{self.dest_table_path}"

    def get_src_cda_timestamp_prefixes(
        self, paginator: Paginator
    ) -> Generator[str, Any, None]:
        schema_paginator = paginator.paginate(
            Bucket=self.src_bucket_name,
            Prefix=f"{self.src_table_path}/",
            Delimiter="/",
        )

        for schema in schema_paginator.search("CommonPrefixes"):
            cda_schema_prefix = schema.get("Prefix")
            cda_timestamp_paginator = paginator.paginate(
                Bucket=self.src_bucket_name,
                Prefix=cda_schema_prefix,
                Delimiter="/",
            )
            for cda_timestamp in cda_timestamp_paginator.search("CommonPrefixes"):
                yield cda_timestamp.get("Prefix")

    def get_loadable_cda_timestamp_prefixes(
        self, paginator: Paginator
    ) -> Generator[str, Any, None]:
        self.logger.info(f"{self.table_name}: getting CDA timestamp prefixes")
        cda_timestamp_prefixes = self.get_src_cda_timestamp_prefixes(
            paginator=paginator
        )
        low_bound_cda_timestamp = 0
        for cda_timestamp_prefix in cda_timestamp_prefixes:
            cda_timestamp = int(cda_timestamp_prefix.split("/")[-2])
            if low_bound_cda_timestamp < cda_timestamp <= self.src_cda_folder:
                yield cda_timestamp_prefix

    def load_to_s3(self, executor: ThreadPoolExecutor, handler: Handler) -> None:
        paginator = handler.s3_client.get_paginator("list_objects_v2")

        cda_timestamp_prefixes = self.get_loadable_cda_timestamp_prefixes(
            paginator=paginator
        )

        exceptions: list[Exception] = []

        futures_cda_timestamp_prefix = {
            executor.submit(
                handler.copy_files_for_folder,
                cda_timestamp_prefix=cda_timestamp_prefix,
            ): cda_timestamp_prefix
            for cda_timestamp_prefix in cda_timestamp_prefixes
        }

        # Wait for all tasks to complete
        for future in as_completed(futures_cda_timestamp_prefix):
            try:
                future.result()
            except Exception as exc:
                exceptions.append(exc)

        if exceptions:
            raise exceptions[0]

        handler.logger.info(
            f"The s3 copy test for {self.core_center} {self.table_name} completed"
        )

    def load_to_mrg(
        self,
        handler: Handler,
        s3_executor: ThreadPoolExecutor,
    ):
        self.load_to_s3(executor=s3_executor, handler=handler)


@dataclasses.dataclass
class Handler:
    logger: structlog.BoundLogger
    spark_log_level: str
    s3_client: BaseClient
    job_name: str
    env_name: str
    core_center: str
    src_bucket: str
    src_prefix: str
    dest_bucket: str
    dest_prefix: str

    def get_manifest_file(self) -> dict:
        if self.src_prefix == "":
            src_path = "manifest.json"
        else:
            src_path = f"{self.src_prefix}/manifest.json"

        manifest = self.s3_client.get_object(
            Bucket=self.src_bucket,
            Key=src_path,
        )
        file_content = manifest["Body"].read().decode("utf-8")
        json_content = json.loads(file_content)
        return json_content

    def copy_file(
        self, src_bucket: str, src_path: str, dest_bucket: str, dest_path: str
    ) -> tuple[float, int]:
        start = perf_counter()

        copy_source = {"Bucket": src_bucket, "Key": src_path}
        max_retries = 10
        delay = 2  # initial delay
        delay_incr = 2  # additional delay in each loop

        for retry_count in range(max_retries + 1):
            try:
                object_bytes = 0
                if self.spark_log_level.upper() == "DEBUG":
                    head_object = self.s3_client.head_object(
                        Bucket=src_bucket,
                        Key=src_path,
                    )
                    object_bytes = head_object["ContentLength"]

                self.logger.debug(
                    f"attempting copy",
                    src_bucket=src_bucket,
                    src_path=src_path,
                    dest_bucket=dest_bucket,
                    dest_path=dest_path,
                    retry_count=retry_count,
                    object_bytes="{:,}".format(object_bytes),
                    delay=delay,
                )
                self.s3_client.copy(copy_source, dest_bucket, dest_path)

                end = perf_counter()
                elapsed = end - start
                self.logger.debug(
                    f"copy successful",
                    src_bucket=src_bucket,
                    src_path=src_path,
                    dest_bucket=dest_bucket,
                    dest_path=dest_path,
                    retry_count=retry_count,
                    object_bytes="{:,}".format(object_bytes),
                    delay=delay,
                    elapsed="{:.2f}".format(elapsed),
                )

                return elapsed, object_bytes

            except ClientError as err:
                if retry_count == max_retries:
                    self.logger.exception(
                        f"copy failed terminally",
                        src_bucket=src_bucket,
                        src_path=src_path,
                        dest_bucket=dest_bucket,
                        dest_path=dest_path,
                        retry_count=retry_count,
                        delay=delay,
                    )
                    raise err

                error = err.response.get("Error")
                if not error:
                    self.logger.exception(
                        f"copy failed terminally",
                        src_bucket=src_bucket,
                        src_path=src_path,
                        dest_bucket=dest_bucket,
                        dest_path=dest_path,
                        retry_count=retry_count,
                        delay=delay,
                    )
                    raise err

                code = error.get("Code", "")
                if code != "SlowDown":
                    self.logger.exception(
                        f"copy failed terminally",
                        src_bucket=src_bucket,
                        src_path=src_path,
                        dest_bucket=dest_bucket,
                        dest_path=dest_path,
                        retry_count=retry_count,
                        delay=delay,
                    )
                    raise err

                self.logger.exception(
                    f"copy failed - retrying",
                    src_bucket=src_bucket,
                    src_path=src_path,
                    dest_bucket=dest_bucket,
                    dest_path=dest_path,
                    retry_count=retry_count,
                    delay=delay,
                )
                sleep(delay)
                delay += delay_incr

        raise Exception("Unreachable code")

    def copy_files_for_folder(self, cda_timestamp_prefix: str) -> None:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.src_bucket,
            Prefix=cda_timestamp_prefix,
        )

        # Filter results for parquet files only.
        parquet_objects = page_iterator.search(
            "Contents[?ends_with(Key, '.snappy.parquet')][]"
        )

        object_stats: list[tuple[float, int]] = [
            self.copy_file(
                src_bucket=self.src_bucket,
                src_path=key_data["Key"],
                dest_bucket=self.dest_bucket,
                dest_path=f'{self.dest_prefix}{key_data["Key"].replace(self.src_prefix, "")}',
            )
            for key_data in parquet_objects
        ]
        durations_sum = sum([duration for duration, _ in object_stats])
        object_bytes_sum = sum([object_bytes for _, object_bytes in object_stats])
        file_count = len(object_stats)
        durations_avg = durations_sum / file_count
        object_bytes_avg = object_bytes_sum / file_count
        self.logger.debug(
            f"cda timestamp folder copied",
            cda_timestamp_prefix=cda_timestamp_prefix,
            src_bucket=self.src_bucket,
            dest_bucket=self.dest_bucket,
            durations_sum="{:.2f}".format(durations_sum),
            object_bytes_sum="{:,}".format(object_bytes_sum),
            file_count=file_count,
            durations_avg="{:.2f}".format(durations_avg),
            object_bytes_avg="{:,.2f}".format(object_bytes_avg),
        )

    def load_tables_to_mrg(
        self,
        tables: list[Table],
        max_workers: int,
        s3_executor: ThreadPoolExecutor,
    ) -> list[Exception]:
        exceptions: list[Exception] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_table_dict = {
                executor.submit(
                    table.load_to_mrg,
                    handler=self,
                    s3_executor=s3_executor,
                ): table
                for table in tables
            }

            # Wait for all tasks to complete
            for future in as_completed(futures_table_dict):
                table = futures_table_dict[future]
                try:
                    future.result()
                    self.logger.info(
                        f"{table.table_name}: Successful s3 copy from guidewire to encova"
                    )
                except Exception as exc:
                    exceptions.append(exc)
        return exceptions

    def send_failure_notification(
        self, exc: Exception, exceptions: list[Exception]
    ) -> None:
        exception_message = (
            str(exc)
            if not exceptions
            else "\n\n\n".join([str(exc) for exc in exceptions])
        )
        if len(exception_message) > 1500:
            exception_message = exception_message[:1500]

        self.logger.exception(
            f"FAILURE: Ingestion - {self.job_name} Failed for '{self.core_center}' core center\n{exception_message}"
        )

    def send_success_notification(self) -> None:
        self.logger.info(
            f"SUCCESS: The glue job {self.job_name} has succeeded for the '{self.core_center}' core center."
        )


def main(
    logger: structlog.BoundLogger,
    # Hard-coded parameters
    job_name: str,
    env_name: str,
    gw_cda_bucket: str,
    gw_s3_dir: str,
    tgt_s3_bucket: str,
    tgt_s3_prefix: str,
    # Dynamic parameters
    core_center: str,
    use_interface_endpoint: bool,
    aws_session: AWSSession,
    spark_log_level: str,
):
    exceptions = []
    logger.info(f"The s3 copy test has started for {core_center}")

    max_s3_workers = 60
    max_sf_workers = 20
    boto_config = Config(max_pool_connections=max_s3_workers)
    if spark_log_level == "DEBUG":
        set_stream_logger("")
    s3_endpoint_url = (
        "https://bucket.vpce-0510fd7d9109f393d-iuhif6y9.s3.us-east-1.vpce.amazonaws.com"
        if use_interface_endpoint
        else None
    )
    s3_client = aws_session.client(
        "s3", config=boto_config, endpoint_url=s3_endpoint_url
    )
    logger.info(f"S3 Endpoint URL: {s3_client.meta.endpoint_url}")

    handler = Handler(
        logger=logger,
        spark_log_level=spark_log_level,
        s3_client=s3_client,
        job_name=job_name,
        env_name=env_name,
        core_center=core_center,
        src_bucket=gw_cda_bucket,
        src_prefix=gw_s3_dir,
        dest_bucket=tgt_s3_bucket,
        dest_prefix=tgt_s3_prefix,
    )

    try:
        manifest = handler.get_manifest_file()

        tables: list[Table] = []
        for table_name, table_details in manifest.items():
            src_cda_schema_id = sorted(
                table_details["schemaHistory"].keys(), key=lambda x: x[1], reverse=True
            )[0]
            table = Table(
                logger=handler.logger,
                table_name=table_name,
                core_center=handler.core_center,
                # src
                src_cda_schema_id=src_cda_schema_id,
                src_cda_folder=int(table_details["lastSuccessfulWriteTimestamp"]),
                src_total_records_count=table_details["totalProcessedRecordsCount"],
                src_bucket_name=handler.src_bucket,
                src_prefix=handler.src_prefix,
                # dest
                dest_bucket_name=handler.dest_bucket,
                dest_prefix=handler.dest_prefix,
            )
            tables.append(table)

        with ThreadPoolExecutor(max_workers=max_s3_workers) as s3_executor:
            exceptions = handler.load_tables_to_mrg(
                tables=tables, max_workers=max_sf_workers, s3_executor=s3_executor
            )

        if exceptions:
            raise exceptions[0]

        handler.send_success_notification()
        handler.logger.info(f"The s3 copy test for {core_center}")
    except Exception as exc:
        if len(exceptions) > 0:
            handler.send_failure_notification(exc=exc, exceptions=exceptions)
        raise exc


def get_core_center_config(in_args: dict[str, str]) -> dict[str, str]:
    core_center_configs = {
        "pc": {
            "gw_cda_bucket": in_args["gw_pc_s3_bucket"],
            "gw_s3_dir": in_args["gw_pc_dir"],
            "tgt_s3_bucket": in_args["tgt_s3_pc_bucket"],
        },
        "bc": {
            "gw_cda_bucket": in_args["gw_bc_s3_bucket"],
            "gw_s3_dir": in_args["gw_bc_dir"],
            "tgt_s3_bucket": in_args["tgt_s3_bc_bucket"],
        },
        "cc": {
            "gw_cda_bucket": in_args["gw_cc_s3_bucket"],
            "gw_s3_dir": in_args["gw_cc_dir"],
            "tgt_s3_bucket": in_args["tgt_s3_cc_bucket"],
        },
        "cm": {
            "gw_cda_bucket": in_args["gw_cm_s3_bucket"],
            "gw_s3_dir": in_args["gw_cm_dir"],
            "tgt_s3_bucket": in_args["tgt_s3_cm_bucket"],
        },
    }

    _core_center = in_args["core_center"]

    if _core_center not in core_center_configs:
        raise ValueError(
            f"Invalid arguments. Must provide one of {', '.join([key for key in core_center_configs.keys()])}."
        )

    _core_center_config = core_center_configs[_core_center]
    if _core_center_config["gw_s3_dir"] == "NOT AVAILABLE":
        _core_center_config["gw_s3_dir"] = ""

    _core_center_config["gw_s3_dir"] = _core_center_config["gw_s3_dir"].rstrip("/")

    return _core_center_config


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
            "env_name",
            "gw_bc_dir",
            "gw_bc_s3_bucket",
            "gw_cc_dir",
            "gw_cc_s3_bucket",
            "gw_cm_dir",
            "gw_cm_s3_bucket",
            "gw_pc_dir",
            "gw_pc_s3_bucket",
            "tgt_s3_bc_bucket",
            "tgt_s3_cc_bucket",
            "tgt_s3_cm_bucket",
            "tgt_s3_pc_bucket",
            "tgt_s3_prefix",
            # Dynamic parameters
            "core_center",
            "use_interface_endpoint",
            "spark_log_level",
        ],
    )
    sc = SparkContext()
    sc.setLogLevel(args["spark_log_level"])
    glue_context = GlueContext(sc)
    structlogger = get_structlog_logger(glue_context.get_logger())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["core_center"], args)

    core_center_config = get_core_center_config(args)

    session = AWSSession(region_name="us-east-1")

    main(
        logger=structlogger,
        # Hard-coded parameters
        job_name=args["JOB_NAME"],
        env_name=args["env_name"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        tgt_s3_bucket=core_center_config["tgt_s3_bucket"],
        tgt_s3_prefix=args["tgt_s3_prefix"],
        # Dynamic parameters
        core_center=args["core_center"],
        use_interface_endpoint=(
            True if args["use_interface_endpoint"].upper() == "TRUE" else False
        ),
        aws_session=session,
        spark_log_level=args["spark_log_level"],
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    args = {
        ## Hard-coded parameters
        "JOB_NAME": "GW_S3_TO_ENCOVA_S3",
        "env_name": "dev1",
        "gw_bc_dir": "dev1e2e/bc/1745172445388/",
        "gw_bc_s3_bucket": "beta-3-us-east-1-encova-encovadev-cda-56c9a",
        "gw_cc_dir": "dev1e2e/cc/1745174274718/",
        "gw_cc_s3_bucket": "beta-3-us-east-1-encova-encovadev-cda-56c9a",
        "gw_cm_dir": "dev1e2e/cm/1745172304241/",
        "gw_cm_s3_bucket": "beta-3-us-east-1-encova-encovadev-cda-56c9a",
        "gw_pc_dir": "dev1e2e/pc/1745174051807/",
        "gw_pc_s3_bucket": "beta-3-us-east-1-encova-encovadev-cda-56c9a",
        "tgt_s3_bc_bucket": "encova-aws-use1-dev1-s3-cda-bc-0001",
        "tgt_s3_cc_bucket": "encova-aws-use1-dev1-s3-cda-cc-0001",
        "tgt_s3_cm_bucket": "encova-aws-use1-dev1-s3-cda-cm-0001",
        "tgt_s3_pc_bucket": "encova-aws-use1-dev1-s3-cda-pc-0001",
        "tgt_s3_prefix": "__copy_into__",
        # Dynamic parameters
        "core_center": "cm",
        "use_interface_endpoint": "false",
        "spark_log_level": "INFO",
    }

    _logger.setLevel(args["spark_log_level"])
    structlogger = get_structlog_logger(logger=_logger)

    core_center_config = get_core_center_config(args)

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
        env_name=args["env_name"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        tgt_s3_bucket=core_center_config["tgt_s3_bucket"],
        tgt_s3_prefix=args["tgt_s3_prefix"],
        # Dynamic parameters
        core_center=args["core_center"],
        use_interface_endpoint=(
            True if args["use_interface_endpoint"].upper() == "TRUE" else False
        ),
        aws_session=assumed_role_session,
        spark_log_level=args["spark_log_level"],
    )
