from __future__ import annotations

import structlog
import dataclasses
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from textwrap import dedent
from time import sleep, perf_counter
from typing import ClassVar, Any, Generator

from boto3 import set_stream_logger
from boto3.session import Session as AWSSession
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from utilities.snowflake_utils import get_snowflake_connection
from utilities.oauth_utils import get_assumed_role_session, get_token
from utilities.SNSClient import SNSClient
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


def get_next_batch_id(connection: SnowflakeConnection, logger: structlog.BoundLogger):
    logger.info(f"fetching batch_id")
    with connection.cursor() as cursor:
        (batch_id) = cursor.execute(
            "SELECT ETL_CTRL.GET_BATCHID() AS BATCHID;"
        ).fetchone()[0]
        return batch_id


def drop_table(
    connection: SnowflakeConnection,
    logger: structlog.BoundLogger,
    schema_name: str,
    table_name: str,
):
    logger.info(f"Dropping table: {schema_name}.{table_name}")
    query = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
    with connection.cursor() as cursor:
        cursor.execute(query)


class LoadException(Exception):
    table: Layer
    procedure: str
    cause: Exception | None

    def __init__(self, message, table: Layer, procedure: str, cause: Exception | None):
        self.message = message
        self.table = table
        self.procedure = procedure
        self.cause = cause
        super().__init__(message, *args)


def insert_into_jc_process_log(
    cursor: SnowflakeCursor,
    row_count: int,
    schema_name: str,
    table_name: str,
    batch_id: str,
    process_name: str,
):
    cursor.execute(
        dedent(
            f"""
                INSERT INTO ETL_CTRL.JC_{schema_name}_PROCESS_LOG (
                    BATCHID,
                    TABLE_NAME,
                    JOB_NAME,
                    PROCESS_NAME,
                    ROW_COUNT
                )
                VALUES (
                    {batch_id},
                    '{table_name}',
                    'GW_S3_to_Snowflake_MRG',
                    $${process_name}$$,
                    {row_count}
                );
            """
        )
    ).fetchone()


def update_jc_load_status(
    schema_name: str,
    table_name: str,
    batch_id: str,
    status: str,
    cursor: SnowflakeCursor,
    logger: structlog.BoundLogger,
):
    logger.info(
        f"{schema_name}.{table_name}: setting ETL_CTRL.JC_{schema_name}_LOAD_STATUS to {status}"
    )
    (rows_inserted, rows_updated) = cursor.execute(
        dedent(
            f"""
                MERGE INTO ETL_CTRL.JC_{schema_name}_LOAD_STATUS AS TGT
                USING (
                    SELECT
                        TABLE_NAME,
                        STATUS
                    FROM (
                        VALUES (
                            '{table_name}',
                            '{status}'
                        )
                    )
                    AS SUB (
                        TABLE_NAME,
                        STATUS
                    )
                ) AS SRC
                    ON LOWER(TGT.TABLE_NAME) = LOWER(SRC.TABLE_NAME)
                WHEN MATCHED THEN
                    UPDATE SET
                        TGT.BATCH_ID = {batch_id},
                        TGT.STATUS = SRC.STATUS,
                        TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                        TGT.ROW_INSERT_TMS = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                    TABLE_NAME,
                    STATUS
                )
                VALUES (
                    SRC.TABLE_NAME,
                    SRC.STATUS
                );
            """
        )
    ).fetchone()
    insert_into_jc_process_log(
        cursor=cursor,
        row_count=rows_inserted + rows_updated,
        schema_name=schema_name,
        table_name=table_name,
        batch_id=batch_id,
        process_name=f"merge into load status",
    )


def set_jc_manifest_post_s3_load(
    core_center: str,
    schema_name: str,
    table_name: str,
    cda_schema_id: str,
    cda_folder: int,
    cda_folder_tms: datetime,
    total_records_count: int,
    batch_id: str,
    cursor: SnowflakeCursor,
    logger: structlog.BoundLogger,
):
    logger.info(
        f"{schema_name}.{table_name}: merging into ETL_CTRL.JC_{core_center.upper()}_MANIFEST_DETAILS"
    )

    (rows_inserted, rows_updated) = cursor.execute(
        dedent(
            f"""
                MERGE INTO ETL_CTRL.JC_{core_center.upper()}_MANIFEST_DETAILS AS TGT
                    USING (
                        SELECT *
                        FROM (
                            VALUES (
                                '{table_name}',
                                '{cda_schema_id}',
                                '{cda_folder}',
                                '{cda_folder_tms.strftime('%Y-%m-%d %H:%M:%S.%f')}'::TIMESTAMP_NTZ,
                                {total_records_count}
                            )
                        )
                            AS SUB (
                                TABLE_NAME,
                                CDA_SCHEMA_ID,
                                CDA_FOLDER,
                                CDA_FOLDER_TMS,
                                TOTAL_RECORDS_COUNT
                            )
                    ) AS SRC
                    ON TGT.TABLE_NAME = SRC.TABLE_NAME
                    WHEN MATCHED AND TGT.CDA_FOLDER_TMS < SRC.CDA_FOLDER_TMS THEN
                        UPDATE SET
                            TGT.CDA_SCHEMA_ID = SRC.CDA_SCHEMA_ID,
                            TGT.CDA_FOLDER = SRC.CDA_FOLDER ,
                            TGT.CDA_FOLDER_TMS = SRC.CDA_FOLDER_TMS ,
                            TGT.TOTAL_RECORDS_COUNT = SRC.TOTAL_RECORDS_COUNT,
                            TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (
                                TABLE_NAME,
                                CDA_SCHEMA_ID,
                                CDA_FOLDER,
                                CDA_FOLDER_TMS,
                                TOTAL_RECORDS_COUNT,
                                ROW_INSERT_TMS,
                                ROW_UPDATE_TMS
                            )
                            VALUES (SRC.TABLE_NAME,
                                    SRC.CDA_SCHEMA_ID,
                                    SRC.CDA_FOLDER,
                                    SRC.CDA_FOLDER_TMS,
                                    TOTAL_RECORDS_COUNT,
                                    CURRENT_TIMESTAMP,
                                    CURRENT_TIMESTAMP);
            """
        ),
    ).fetchone()
    insert_into_jc_process_log(
        cursor=cursor,
        row_count=rows_inserted + rows_updated,
        schema_name=schema_name,
        table_name=table_name,
        batch_id=batch_id,
        process_name=f"merge into manifest details",
    )


@dataclasses.dataclass
class Layer:
    logger: structlog.BoundLogger
    batch_id: str
    core_center: str
    table_name: str
    schema_name: str
    status: str
    load: bool
    proc_name: ClassVar[str]

    def update_jc_load_status(self, status: str, cursor: SnowflakeCursor):
        cursor.execute(
            dedent(
                f"""
                    UPDATE ETL_CTRL.JC_{self.schema_name}_LOAD_STATUS
                    SET BATCH_ID = {self.batch_id},
                        ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                        STATUS = $${status}$$
                    WHERE TABLE_NAME = '{self.table_name}';
                """
            )
        )

    def insert_into_jc_error_log(
        self, error_message: str, error_sql: str, cursor: SnowflakeCursor
    ):
        query = dedent(
            f"""
                INSERT INTO ETL_CTRL.JC_ERROR_LOG(
                    BATCHID,
                    DAY_ID,
                    ERROR_MESSAGE,
                    ERROR_SQL,
                    JOB_NAME,
                    ROW_INSERT_TMS,
                    SCHEMA_NAME,
                    TABLE_NAME
                )
                VALUES (
                    {self.batch_id},
                    CURRENT_DATE,
                    $${error_message}$$,
                    $${error_sql}$$,
                    $${self.proc_name}$$,
                    current_timestamp,
                    $${self.schema_name}$$,
                    $${self.table_name}$$
                )
            """
        )
        cursor.execute(query)

    def call_stored_procedure(
        self,
        query: str,
        cursor: SnowflakeCursor,
    ) -> list[tuple] | list[dict]:
        try:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: Running query: {query}"
            )
            self.update_jc_load_status(status="started", cursor=cursor)
            response = cursor.execute(dedent(query)).fetchall()
            self.update_jc_load_status(status="completed", cursor=cursor)
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: Successful {self.proc_name}"
            )
            return response
        except Exception as exc:
            error_sql = getattr(exc, "query", None)
            if error_sql is not None:
                error_sql = error_sql.replace("$$", r"\$\$")
            error_message = str(getattr(exc, "msg", None))

            self.insert_into_jc_error_log(
                error_message=error_message, error_sql=error_sql, cursor=cursor
            )

            self.update_jc_load_status(status="failed", cursor=cursor)

            self.logger.exception(
                f"{self.schema_name}.{self.table_name}: exception occurred while running query: {query}"
            )
            raise LoadException(
                message=f"{self.schema_name}.{self.table_name}: exception occurred while running",
                table=self,
                procedure=self.proc_name,
                cause=exc,
            ) from exc


@dataclasses.dataclass
class RawLayer(Layer):
    proc_name = "SP_PROCESS_RAW_TABLE"

    def execute(self, cursor: SnowflakeCursor) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return
        self.call_stored_procedure(
            query=f"""
                    CALL ETL_CTRL.{self.proc_name}(
                        {self.batch_id},
                        {self.status.lower() == 'initial'},
                        '{self.core_center}',
                        '{self.table_name}',
                        '{self.schema_name}'
                    );
                """,
            cursor=cursor,
        )

        (
            manifest_total_records_count,
            raw_count,
            total_records_count_diff,
            manifest_cda_folder,
            raw_cda_folder,
        ) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        MANIFEST_TOTAL_RECORDS_COUNT,
                        RAW_COUNT,
                        TOTAL_RECORDS_COUNT_DIFF,
                        MANIFEST_CDA_FOLDER,
                        RAW_CDA_FOLDER
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        # CDA Behavior: When all the records in a micro-batch are duplicates, the manifest.json file is updated with
        # the lastSuccessfulWriteTimestamp using the micro-batch ID (MANIFEST_CDA_FOLDER). The folder for the
        # corresponding time (RAW_CDA_FOLDER) does not exist because duplicate records are not written. In such
        # cases, the RAW_CDA_FOLDER is expected to be smaller than the MANIFEST_CDA_FOLDER.
        if (
            total_records_count_diff != 0
            or manifest_cda_folder is None
            or raw_cda_folder is None
            or manifest_cda_folder < raw_cda_folder
        ):
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"TOTAL_RECORDS_COUNT: table {raw_count}, "
                f"manifest: {manifest_total_records_count} | "
                f"CDA_FOLDER: table {raw_cda_folder}, "
                f"manifest: {manifest_cda_folder}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class StageLayer(Layer):
    proc_name = "SP_PROCESS_STG_TABLE"

    def execute(
        self,
        cursor: SnowflakeCursor,
        raw_schema_name: str,
        cda_schema_id: str,
        cda_folder: int,
    ) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return

        self.call_stored_procedure(
            query=f"""
                        CALL ETL_CTRL.{self.proc_name}(
                            {self.batch_id},
                            {self.status.lower() == 'initial'},
                            '{self.core_center}',
                            '{self.table_name}',
                            '{self.schema_name}',
                            '{raw_schema_name}',
                            '{cda_schema_id}',
                            '{cda_folder}'
                        );
                    """,
            cursor=cursor,
        )

        (raw_count, stg_count, stg_count_diff) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        RAW_COUNT,
                        STG_COUNT,
                        STG_COUNT_DIFF
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        if stg_count_diff != 0:
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"RAW count: {raw_count}, "
                f"STG count: {stg_count}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class MergeLayer(Layer):
    proc_name = "SP_PROCESS_MRG_TABLE"

    def execute(self, cursor: SnowflakeCursor, stg_schema_name: str) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return

        self.call_stored_procedure(
            query=f"""
                    CALL ETL_CTRL.{self.proc_name}(
                        {self.batch_id},
                        {self.status.lower() == 'initial'},
                        '{self.core_center}',
                        '{self.table_name}',
                        '{self.schema_name}',
                        '{stg_schema_name}'
                     );
                    """,
            cursor=cursor,
        )

        (mrg_count, expected_mrg_count, expected_mrg_count_diff) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        MRG_COUNT,
                        EXPECTED_MRG_COUNT,
                        EXPECTED_MRG_COUNT_DIFF
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        if expected_mrg_count_diff != 0:
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"MRG count: {mrg_count}, "
                f"Expected MRG count: {expected_mrg_count}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class Table:
    logger: structlog.BoundLogger
    table_name: str
    core_center: str
    batch_id: str
    raw_layer: RawLayer
    stg_layer: StageLayer
    mrg_layer: MergeLayer

    load_s3: bool
    src_cda_schema_id: str
    src_cda_folder: int
    src_cda_folder_tms: datetime
    src_total_records_count: int
    src_bucket_name: str
    src_prefix: str

    dest_cda_folder: int | None
    dest_cda_folder_tms: datetime
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
        low_bound_cda_timestamp = (
            0 if self.dest_cda_folder is None else self.dest_cda_folder
        )
        for cda_timestamp_prefix in cda_timestamp_prefixes:
            cda_timestamp = int(cda_timestamp_prefix.split("/")[-2])
            if low_bound_cda_timestamp < cda_timestamp <= self.src_cda_folder:
                yield cda_timestamp_prefix

    def load_to_s3(
        self, executor: ThreadPoolExecutor, handler: Handler, cursor: SnowflakeCursor
    ) -> None:
        if not self.load_s3:
            self.logger.info(f"{self.table_name}: skipping S3 load")
            return

        self.raw_layer.load = True
        update_jc_load_status(
            schema_name=self.raw_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            status="pending s3 transfer",
            cursor=cursor,
            logger=self.logger,
        )

        self.stg_layer.load = True
        update_jc_load_status(
            schema_name=self.stg_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            status="pending raw load",
            cursor=cursor,
            logger=self.logger,
        )

        self.mrg_layer.load = True
        update_jc_load_status(
            schema_name=self.mrg_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            status="pending stg load",
            cursor=cursor,
            logger=self.logger,
        )

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
            f"{'incremental' if handler.is_incremental else 'initial'} "
            f"file transfer for all {self.core_center} {self.table_name} completed"
        )

        set_jc_manifest_post_s3_load(
            core_center=self.core_center,
            schema_name=self.raw_layer.schema_name,
            table_name=self.table_name,
            cda_schema_id=self.src_cda_schema_id,
            cda_folder=self.src_cda_folder,
            cda_folder_tms=self.src_cda_folder_tms,
            total_records_count=self.src_total_records_count,
            batch_id=self.batch_id,
            cursor=cursor,
            logger=self.logger,
        )

    def load_to_mrg(
        self,
        handler: Handler,
        s3_executor: ThreadPoolExecutor,
        connection: SnowflakeConnection,
    ):
        with connection.cursor() as cursor:
            if handler.s3_load_enabled:
                self.load_to_s3(executor=s3_executor, handler=handler, cursor=cursor)

            if not handler.mrg_load_enabled:
                return

            self.raw_layer.execute(cursor=cursor)

            self.stg_layer.execute(
                cursor=cursor,
                raw_schema_name=self.raw_layer.schema_name,
                cda_schema_id=self.src_cda_schema_id,
                cda_folder=self.src_cda_folder,
            )

            self.mrg_layer.execute(
                cursor=cursor,
                stg_schema_name=self.stg_layer.schema_name,
            )


def get_core_center_details(
    is_incremental: bool,
    core_center: str,
    logger: structlog.BoundLogger,
    connection: SnowflakeConnection,
) -> tuple[bool, bool, str, str, str]:
    logger.info(f"fetching {core_center} core center details")
    with connection.cursor() as cursor:
        (
            s3_load_enabled,
            mrg_load_enabled,
            raw_schema_name,
            stg_schema_name,
            mrg_schema_name,
        ) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        IFF(
                            {is_incremental},
                            S3_INCREMENTAL_LOAD_FLAG = TRUE,
                            S3_INITIAL_LOAD_FLAG = TRUE
                        )                        AS S3_LOAD_ENABLED,
                        IFF(
                            {is_incremental},
                            MRG_INCREMENTAL_LOAD_FLAG = TRUE,
                            MRG_INITIAL_LOAD_FLAG = TRUE
                        )                        AS MRG_LOAD_ENABLED,
                        UPPER(RAW_SCHEMA_NAME)   AS RAW_SCHEMA_NAME,
                        UPPER(STG_SCHEMA_NAME)   AS STG_SCHEMA_NAME,
                        UPPER(MERGE_SCHEMA_NAME) AS MRG_SCHEMA_NAME
                    FROM ETL_CTRL.JC_CORE_CENTER_DETAILS
                    WHERE UPPER(CORE_CENTER) = UPPER('{core_center}')
                """
            )
        ).fetchone()
        return (
            s3_load_enabled,
            mrg_load_enabled,
            raw_schema_name,
            stg_schema_name,
            mrg_schema_name,
        )


@dataclasses.dataclass
class Handler:
    logger: structlog.BoundLogger
    spark_log_level: str
    sns_client: SNSClient
    s3_client: BaseClient
    connection: SnowflakeConnection
    batch_id: str
    job_name: str
    env_name: str
    is_incremental: bool
    core_center: str
    src_bucket: str
    src_prefix: str
    dest_bucket: str
    raw_schema_name: str
    stg_schema_name: str
    mrg_schema_name: str
    s3_load_enabled: bool
    mrg_load_enabled: bool
    dest_prefix: ClassVar[str] = "__copy_into__"
    is_frequent: bool

    def copy_manifest_file(self):
        """
        Copy manifest file from source to temporary path in dest bucket
        """
        if self.src_prefix == "":
            src_path = "manifest.json"
        else:
            src_path = f"{self.src_prefix}/manifest.json"

        if self.dest_prefix == "":
            dest_path = "manifest.json"
        else:
            dest_path = f"{self.dest_prefix}/manifest.json"

        self.copy_file(
            src_bucket=self.src_bucket,
            src_path=src_path,
            dest_bucket=self.dest_bucket,
            dest_path=dest_path,
        )
        self.logger.info(
            f"manifest file copied from {self.src_bucket}/{src_path} to {self.dest_bucket}/{dest_path}"
        )

    def truncate_jc_manifest_details(self):
        self.logger.info(
            f"truncating ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS"
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                        TRUNCATE TABLE ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS;
                    """
                )
            ).fetchone()

    def truncate_jc_load_status(self, schema_name: str):
        self.logger.info(f"truncating ETL_CTRL.JC_{schema_name}_LOAD_STATUS")
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                        TRUNCATE TABLE ETL_CTRL.JC_{schema_name}_LOAD_STATUS;
                    """
                )
            ).fetchone()

    def drop_all_tables_in_schema(self, schema_name: str):
        self.logger.info(f"Dropping all tables in schema: {schema_name}")

        query = f"""
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
            FROM (SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_CATALOG = CURRENT_DATABASE()
                    AND UPPER(TABLE_SCHEMA) = '{schema_name.upper()}')
        """
        tables: list[tuple[str, str]] = []
        exceptions: list[Exception] = []
        with self.connection.cursor() as cursor:
            tables.extend(
                [
                    (schema_name, table_name)
                    for (schema_name, table_name) in cursor.execute(query)
                ]
            )

        with ThreadPoolExecutor() as executor:
            futures_table_dict = {
                executor.submit(
                    drop_table,
                    connection=self.connection,
                    logger=self.logger,
                    schema_name=table[0],
                    table_name=table[1],
                ): table
                for table in tables
            }

            for future in as_completed(futures_table_dict):
                (schema_name, table_name) = futures_table_dict[future]
                try:
                    future.result()
                    self.logger.info(f"Dropped table: {schema_name}.{table_name}")
                except Exception as exc:
                    self.logger.error(
                        f"Exception raised when dropping table: {schema_name}.{table_name}:\n\n{str(exc)}"
                    )
                    exceptions.append(exc)
        if len(exceptions) != 0:
            self.logger.error(exceptions)
            raise exceptions[0]

        self.logger.info(f"Dropped all tables in schema: {schema_name}")

    def truncate_manifest_and_load_status(self):
        self.truncate_jc_manifest_details()
        self.truncate_jc_load_status(schema_name=self.raw_schema_name)
        self.truncate_jc_load_status(schema_name=self.stg_schema_name)
        self.truncate_jc_load_status(schema_name=self.mrg_schema_name)
        self.drop_all_tables_in_schema(schema_name=self.raw_schema_name)
        self.drop_all_tables_in_schema(schema_name=self.stg_schema_name)
        self.drop_all_tables_in_schema(schema_name=self.mrg_schema_name)

    def get_altered_tables(self) -> list[Table]:
        """
        Compare the current manifest (from Snowflake) to the new manifest (from GW) and return tables
        where on of the following is true:
        - The lastSuccessfulWriteTimestamp has changed
        - The table is missing from either the current or new manifest

        :return:
        """
        self.logger.info(f"fetching altered tables")
        query = dedent(
            f"""
                    SELECT
                        LOWER(SRC_MFST.TABLE_NAME)                            AS TABLE_NAME,
                        SRC_MFST.CDA_SCHEMA_ID                                AS SRC_CDA_SCHEMA_ID,
                        SRC_MFST.CDA_FOLDER::NUMBER                           AS SRC_CDA_FOLDER,
                        SRC_MFST.CDA_FOLDER_TMS                               AS SRC_CDA_FOLDER_TMS,
                        SRC_MFST.TOTAL_RECORDS_COUNT                          AS SRC_TOTAL_RECORDS_COUNT,
                        DEST_MFST.CDA_FOLDER::NUMBER                          AS DEST_CDA_FOLDER,
                        DEST_MFST.CDA_FOLDER_TMS                              AS DEST_CDA_FOLDER_TMS,
                        DEST_MFST.CDA_FOLDER_TMS IS NULL
                        OR SRC_MFST.CDA_FOLDER_TMS > DEST_MFST.CDA_FOLDER_TMS AS LOAD_S3,
                        COALESCE(RAW.STATUS, 'initial')                       AS RAW_STATUS,
                        LOAD_S3 OR NOT EQUAL_NULL(RAW.STATUS, 'completed')    AS LOAD_RAW,
                        COALESCE(STG.STATUS, 'initial')                       AS STG_STATUS,
                        LOAD_RAW OR NOT EQUAL_NULL(STG.STATUS, 'completed')   AS LOAD_STG,
                        COALESCE(MRG.STATUS, 'initial')                       AS MRG_STATUS,
                        LOAD_STG OR NOT EQUAL_NULL(MRG.STATUS, 'completed')   AS LOAD_MRG
                    FROM ETL_CTRL.VW_{self.core_center.upper()}_MANIFEST_DETAILS AS SRC_MFST
                        LEFT JOIN ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS AS DEST_MFST
                            ON SRC_MFST.TABLE_NAME = DEST_MFST.TABLE_NAME
                        LEFT JOIN ETL_CTRL.JC_{self.raw_schema_name.upper()}_LOAD_STATUS AS RAW
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(RAW.TABLE_NAME)
                        LEFT JOIN ETL_CTRL.JC_{self.stg_schema_name.upper()}_LOAD_STATUS AS STG
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(STG.TABLE_NAME)
                        LEFT JOIN ETL_CTRL.JC_{self.mrg_schema_name.upper()}_LOAD_STATUS AS MRG
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(MRG.TABLE_NAME)
                        {"INNER JOIN ETL_CTRL.JC_FREQ_LOAD_TABLE_LIST AS FREQ ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(FREQ.TABLE_NAME) " if self.is_frequent else ""}

                    WHERE LOWER(SRC_MFST.TABLE_NAME) != 'heartbeat'
                        AND (
                            ({self.s3_load_enabled} AND LOAD_S3) OR
                            ({self.mrg_load_enabled} AND (LOAD_RAW OR LOAD_STG OR LOAD_MRG))
                        )
                """
        )
        self.logger.info(
            f"Generated SQL query for get_altered_tables method :\n{query}"
        )
        tables: list[Table] = []

        with self.connection.cursor() as cursor:
            for (
                table_name,
                src_cda_schema_id,
                src_cda_folder,
                src_cda_folder_tms,
                src_total_records_count,
                dest_cda_folder,
                dest_cda_folder_tms,
                load_s3,
                raw_status,
                load_raw,
                stg_status,
                load_stg,
                mrg_status,
                load_mrg,
            ) in cursor.execute(query).fetchall():
                self.logger.info(
                    f"{table_name}: load_s3: {load_s3} load_raw: {load_raw} load_stg: {load_stg} load_mrg: {load_mrg}"
                )
                tables.append(
                    Table(
                        logger=self.logger,
                        table_name=table_name,
                        core_center=self.core_center,
                        batch_id=self.batch_id,
                        raw_layer=RawLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.raw_schema_name,
                            status=raw_status,
                            load=load_raw,
                        ),
                        stg_layer=StageLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.stg_schema_name,
                            status=stg_status,
                            load=load_stg,
                        ),
                        mrg_layer=MergeLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.mrg_schema_name,
                            status=mrg_status,
                            load=load_mrg,
                        ),
                        load_s3=load_s3,
                        # src
                        src_cda_schema_id=src_cda_schema_id,
                        src_cda_folder=src_cda_folder,
                        src_cda_folder_tms=src_cda_folder_tms,
                        src_total_records_count=src_total_records_count,
                        src_bucket_name=self.src_bucket,
                        src_prefix=self.src_prefix,
                        # dest
                        dest_cda_folder=dest_cda_folder,
                        dest_cda_folder_tms=dest_cda_folder_tms,
                        dest_bucket_name=self.dest_bucket,
                        dest_prefix=self.dest_prefix,
                    )
                )

        return tables

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

    def send_no_files_or_tables_notification(self) -> None:
        if not self.s3_load_enabled and not self.mrg_load_enabled:
            self.sns_client.send_notification(
                subject=(
                    f"{self.env_name} Ingestion - {self.core_center} - S3 and Merge load are disabled"
                ),
                message=(
                    "Hi team,\n\n"
                    f"S3 and Merge load are disabled for the '{self.core_center}' core center\n\n"
                    "Thanks"
                ),
            )
        else:
            self.sns_client.send_notification(
                subject=(
                    f"{self.env_name} Ingestion - {self.core_center} - No "
                    f"{'incremental' if self.is_incremental else 'initial'} "
                    f"files to copy or tables to load through the MRG layer"
                ),
                message=(
                    "Hi team,\n\n"
                    f"The {self.core_center} Glue job {self.job_name} processed 0 "
                    f"{'incremental' if self.is_incremental else 'initial'} files in its latest run. "
                    "This is because there were no new files available for ingestion and no Snowflake "
                    "tables to load through the MRG layer\n\n"
                    "Thanks"
                ),
            )

    def load_tables_to_mrg(
        self,
        tables: list[Table],
        max_workers: int,
        s3_executor: ThreadPoolExecutor,
    ) -> list[LoadException]:
        exceptions: list[LoadException] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_table_dict = {
                executor.submit(
                    table.load_to_mrg,
                    handler=self,
                    s3_executor=s3_executor,
                    connection=self.connection,
                ): table
                for table in tables
            }

            # Wait for all tasks to complete
            for future in as_completed(futures_table_dict):
                table = futures_table_dict[future]
                try:
                    future.result()
                    self.logger.info(f"{table.table_name}: Successful load to merge")
                except LoadException as exc:
                    exceptions.append(exc)
        return exceptions

    def send_failure_notification(
        self, exc: Exception, exceptions: list[LoadException]
    ) -> None:
        exception_message = (
            str(exc)
            if not exceptions
            else "\n\n\n".join(
                [
                    (
                        f"Table Name: {exc.table.table_name}\n"
                        f"Procedure Name: {exc.procedure}\n"
                        f"Exception: {str(exc.cause).splitlines()[-1]}\n\n"
                        f"********************************************************"
                    )
                    for exc in exceptions
                ]
            )
        )
        if len(exception_message) > 1500:
            exception_message = exception_message[:1500]

        message = dedent(
            "Hi team,\n\n"
            f"FAILURE: The {'incremental' if self.is_incremental else 'initial'} glue job {self.job_name} "
            f"for the '{self.core_center}' core center failed to copy the files from GW CDA to Snowflake Mrg layer. "
            f"The errors are listed below: \n\n{exception_message}\n\n"
            "For more information please check Cloudwatch latest log streams.\n\n"
            "Thanks"
        )

        self.sns_client.send_notification(
            subject=f"FAILURE: Ingestion - GW CDA to Snowflake Mrg layer Failed for '{self.core_center}' core center",
            message=message,
        )

    def send_success_notification(self) -> None:
        self.sns_client.send_notification(
            subject=f"SUCCESS: Ingestion - GW CDA to Snowflake Mrg layer "
            f"Succeeded for '{self.core_center}' core center",
            message=dedent(
                "Hi team,\n\n"
                f"SUCCESS: The glue job {self.job_name} has succeeded for the '{self.core_center}' core center."
            ),
        )

    def insert_into_jc_batch_status(self, status):
        batch_name = f"{'Incremental' if self.is_incremental else 'Initial'} load for {self.core_center}"
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                    MERGE INTO ETL_CTRL.JC_BATCH_STATUS AS TARGET
                        USING (SELECT $${self.batch_id}$$ AS BATCHID,
                                      UPPER($${status}$$) AS STATUS,
                                      $${batch_name}$$    AS BATCH_NAME,
                                      $${self.job_name}$$ AS JOB_NAME) AS SOURCE
                        ON TARGET.BATCHID = SOURCE.BATCHID
                        WHEN MATCHED AND UPPER(SOURCE.STATUS) IN ('COMPLETED', 'FAILED') THEN
                            UPDATE SET
                                TARGET.END_TIME = CURRENT_TIMESTAMP,
                                TARGET.ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                                TARGET.STATUS = SOURCE.STATUS
                        WHEN MATCHED AND UPPER(SOURCE.STATUS) NOT IN ('COMPLETED', 'FAILED') THEN
                            UPDATE SET
                                TARGET.ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                                TARGET.STATUS = SOURCE.STATUS
                        WHEN NOT MATCHED THEN
                            INSERT (
                                    BATCHID,
                                    BATCH_NAME,
                                    DAY_ID,
                                    START_TIME,
                                    END_TIME,
                                    JOB_NAME,
                                    ROW_INSERT_TMS,
                                    ROW_UPDATE_TMS,
                                    STATUS
                                )
                                VALUES (SOURCE.BATCHID,
                                        SOURCE.BATCH_NAME,
                                        CURRENT_DATE,
                                        CURRENT_TIMESTAMP,
                                        NULL,
                                        SOURCE.JOB_NAME,
                                        CURRENT_TIMESTAMP,
                                        CURRENT_TIMESTAMP,
                                        SOURCE.STATUS)
                """
                )
            )


def main(
    logger: structlog.BoundLogger,
    # Hard-coded parameters
    job_name: str,
    sns_topic_name: str,
    env_name: str,
    secret_name: str,
    token_endpoint: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    gw_cda_bucket: str,
    gw_s3_dir: str,
    tgt_s3_bucket: str,
    # Dynamic parameters
    core_center: str,
    is_incremental: bool,
    is_frequent: bool,
    use_interface_endpoint: bool,
    aws_session: AWSSession,
    spark_log_level: str,
):
    exceptions = []
    logger.info(
        f"The {'incremental' if is_incremental else 'initial'} job has started for {core_center}"
    )

    authorization_token = get_token(
        session=aws_session,
        secret_name=secret_name,
        token_endpoint=token_endpoint,
        role_name=snowflake_role,
    )

    sf_connection = get_snowflake_connection(
        logger=logger,
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

    max_s3_workers = 60
    max_sf_workers = 20
    boto_config = Config(max_pool_connections=max_s3_workers)
    if spark_log_level == "DEBUG":
        set_stream_logger("")
    s3_endpoint_url = (
        "https://bucket.vpce-0510fd7d9109f393d-iuhif6y9.s3.us-east-1.vpce.amazonaws.com"
        if use_interface_endpoint and env_name == "runway1e2e"
        else None
    )
    s3_client = aws_session.client(
        "s3", config=boto_config, endpoint_url=s3_endpoint_url
    )
    logger.info(f"S3 Endpoint URL: {s3_client.meta.endpoint_url}")

    (
        s3_load_enabled,
        mrg_load_enabled,
        raw_schema_name,
        stg_schema_name,
        mrg_schema_name,
    ) = get_core_center_details(
        is_incremental=is_incremental,
        core_center=core_center,
        connection=sf_connection,
        logger=logger,
    )

    batch_id = get_next_batch_id(connection=sf_connection, logger=logger)

    handler = Handler(
        logger=logger,
        spark_log_level=spark_log_level,
        sns_client=sns_client,
        s3_client=s3_client,
        connection=sf_connection,
        batch_id=batch_id,
        job_name=job_name,
        env_name=env_name,
        is_incremental=is_incremental,
        core_center=core_center,
        src_bucket=gw_cda_bucket,
        src_prefix=gw_s3_dir,
        dest_bucket=tgt_s3_bucket,
        s3_load_enabled=s3_load_enabled,
        mrg_load_enabled=mrg_load_enabled,
        raw_schema_name=raw_schema_name,
        stg_schema_name=stg_schema_name,
        mrg_schema_name=mrg_schema_name,
        is_frequent=is_frequent,
    )

    try:
        handler.insert_into_jc_batch_status(status="STARTED")
        if handler.s3_load_enabled and not handler.is_incremental:
            handler.truncate_manifest_and_load_status()

        if handler.s3_load_enabled:
            handler.copy_manifest_file()

        tables = handler.get_altered_tables()

        if len(tables) == 0:
            handler.send_no_files_or_tables_notification()

        with ThreadPoolExecutor(max_workers=max_s3_workers) as s3_executor:
            exceptions = handler.load_tables_to_mrg(
                tables=tables, max_workers=max_sf_workers, s3_executor=s3_executor
            )

        if exceptions:
            raise exceptions[0]

        handler.send_success_notification()
        handler.logger.info(
            f"{'incremental' if is_incremental else 'initial'} file transfer for all {core_center} tables completed"
        )
        handler.insert_into_jc_batch_status(status="COMPLETED")
    except Exception as exc:
        if len(exceptions) > 0:
            handler.send_failure_notification(exc=exc, exceptions=exceptions)
        handler.insert_into_jc_batch_status(status="FAILED")
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
            "sns_topic_name",
            "env_name",
            "secret_name",
            "token_endpoint",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "snowflake_role",
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
            # Dynamic parameters
            "core_center",
            "is_incremental",
            "is_frequent",
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
        sns_topic_name=None if args["sns_topic_name"] == "" else args["sns_topic_name"],
        env_name=args["env_name"],
        secret_name=args["secret_name"],
        token_endpoint=args["token_endpoint"],
        snowflake_account=args["snowflake_account"],
        snowflake_user=args["snowflake_user"],
        snowflake_database=args["snowflake_database"],
        snowflake_warehouse=args["snowflake_warehouse"],
        snowflake_role=args["snowflake_role"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        tgt_s3_bucket=core_center_config["tgt_s3_bucket"],
        # Dynamic parameters
        core_center=args["core_center"],
        is_incremental=True if args["is_incremental"].upper() == "TRUE" else False,
        is_frequent=True if args["is_frequent"].upper() == "TRUE" else False,
        aws_session=session,
        use_interface_endpoint=(
            True if args["use_interface_endpoint"].upper() == "TRUE" else False
        ),
        spark_log_level=args["spark_log_level"],
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    args = {
        ## Hard-coded parameters
        "JOB_NAME": "GW_S3_TO_SNOWFLAKE_MRG",
        "sns_topic_name": "",
        "env_name": "dev1",
        "secret_name": "dev-aws-glue",
        "token_endpoint": "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token",
        "snowflake_account": "encova-dev.privatelink",
        "snowflake_user": "0oa25sh1ca4BsKnGC0h8",
        "snowflake_database": "DEV1E2E",
        "snowflake_warehouse": "WH_GLUE",
        "snowflake_role": "DEV1E2E__GLUE__SVC_ROLE",
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
        # Dynamic parameters
        "core_center": "cm",
        "is_incremental": "true",
        "is_frequent": "false",
        "use_interface_endpoint": "false",
        "spark_log_level": "DEBUG",
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
        sns_topic_name=None if args["sns_topic_name"] == "" else args["sns_topic_name"],
        env_name=args["env_name"],
        secret_name=args["secret_name"],
        token_endpoint=args["token_endpoint"],
        snowflake_account=args["snowflake_account"],
        snowflake_user=args["snowflake_user"],
        snowflake_database=args["snowflake_database"],
        snowflake_warehouse=args["snowflake_warehouse"],
        snowflake_role=args["snowflake_role"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        tgt_s3_bucket=core_center_config["tgt_s3_bucket"],
        # Dynamic parameters
        core_center=args["core_center"],
        is_incremental=True if args["is_incremental"].upper() == "TRUE" else False,
        is_frequent=True if args["is_frequent"].upper() == "TRUE" else False,
        aws_session=assumed_role_session,
        use_interface_endpoint=(
            True if args["use_interface_endpoint"].upper() == "TRUE" else False
        ),
        spark_log_level=args["spark_log_level"],
    )
