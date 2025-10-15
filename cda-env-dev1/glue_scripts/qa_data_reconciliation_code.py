from __future__ import annotations

import dataclasses
import logging
from collections import defaultdict
import pandas as pd

from boto3.session import Session as AWSSession
from utilities.snowflake_utils import get_snowflake_connection
from utilities.oauth_utils import get_assumed_role_session, get_token
from utilities.logging_utils import get_structlog_logger
from typing import List, Dict
from textwrap import dedent
import sys
import structlog
from botocore.config import Config
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from snowflake.connector import SnowflakeConnection
from pyspark.context import SparkContext

from botocore.client import BaseClient

try:
    IS_GLUE_JOB = True

    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job

    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions
except ModuleNotFoundError as e:
    IS_GLUE_JOB = False

logging.basicConfig(
    format="%(asctime)s : %(filename)s - %(funcName)s : %(lineno)d : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)

for noisy_logger in [
    "snowflake",
    "boto3",
    "botocore",
    "urllib3",
    "concurrent.futures",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


@dataclasses.dataclass
class Table:
    batch_id: int
    core_center: str
    table_name: str
    stage_name: str
    raw_schema_name: str
    stg_schema_name: str
    mrg_schema_name: str
    s3_file_count: int
    encova_S3_file_count: int
    logger: structlog.BoundLogger

    stage_file_count: int = 0
    raw_schema_record_count: int = 0
    stg_schema_record_count: int = 0
    stg_schema_record_distinct_count: int = 0
    mrg_schema_record_count: int = 0

    @classmethod
    def factory(
        cls,
        batch_id: int,
        core_center: str,
        table_name: str,
        stage_name: str,
        raw_schema_name: str,
        stg_schema_name: str,
        mrg_schema_name: str,
        s3_file_count: int,
        encova_S3_file_count: int,
        logger: structlog.BoundLogger,
    ) -> Table:
        logger = logger.bind(
            table_name=table_name,
            batch_id=batch_id,
        )
        return cls(
            batch_id=batch_id,
            core_center=core_center,
            table_name=table_name,
            stage_name=stage_name,
            raw_schema_name=raw_schema_name,
            stg_schema_name=stg_schema_name,
            mrg_schema_name=mrg_schema_name,
            s3_file_count=s3_file_count,
            encova_S3_file_count=encova_S3_file_count,
            logger=logger,
        )

    def get_stage_file_count(
        self,
        connection: SnowflakeConnection,
    ) -> int:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(metadata$filename) from {self.stage_name}/{self.table_name.lower()}/"
                )
                result = cursor.fetchone()
                count = result[0] if result else 0
                return count
        except Exception as exc:
            error_desc = f"Error fetching file count for table"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def get_record_count(
        self,
        schema_name: str,
        connection: SnowflakeConnection,
        distinct: bool,
    ) -> int:
        try:
            with connection.cursor() as cursor:
                if distinct:
                    query = f"SELECT count(distinct id) from {schema_name}.{self.table_name}"
                else:
                    query = f"SELECT count(*) from {schema_name}.{self.table_name}"
                cursor.execute(query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                return count
        except Exception as exc:
            error_desc = f"Error fetching count for {schema_name} schema"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def write_to_snowflake(
        self,
        connection: SnowflakeConnection,
        max_batchid: int,
    ):
        try:
            with connection.cursor() as cursor:
                query = f"""
                INSERT INTO ETL_CTRL.JC_{self.core_center.upper()}_E2E_COUNT_VALIDATION(BATCH_ID,
                                                         SF_BATCHID,
                                                         TABLE_NAME,
                                                         GW_S3_COUNT,
                                                         ENCOVA_S3_COUNT,
                                                         ENCOVA_STAGE_COUNT,
                                                         RAW_LAYER_COUNT,
                                                         STG_LAYER_COUNT,
                                                         STG_LAYER_DISTINCT_COUNT,
                                                         MRG_LAYER_COUNT,
                                                         RECONCILIATION_STATUS)
                VALUES ({self.batch_id},
                        {max_batchid},
                        '{self.table_name}',
                        {self.s3_file_count},
                        {self.encova_S3_file_count},
                        {self.stage_file_count},
                        {self.raw_schema_record_count},
                        {self.stg_schema_record_count},
                        {self.stg_schema_record_distinct_count},
                        {self.mrg_schema_record_count},
                        '{"Success" if self.reconciled else "Failed"}');
                """
                cursor.execute(query)
        except Exception as exc:
            error_desc = f"Error writing counts to Table ETL_CTRL.JC_{self.core_center.upper()}_E2E_COUNT_VALIDATION"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def write_counts_to_snowflake(
        self,
        connection: SnowflakeConnection,
        max_batchid: int,
    ):
        self.logger.info("Getting counts")

        self.stage_file_count = self.get_stage_file_count(connection=connection)

        self.raw_schema_record_count = self.get_record_count(
            schema_name=self.raw_schema_name,
            connection=connection,
            distinct=False,
        )

        self.stg_schema_record_count = self.get_record_count(
            schema_name=self.stg_schema_name,
            connection=connection,
            distinct=False,
        )
        # getting distinct id counts from STG Layer to compare with MRG Layer because MRG Layer is SCD Type 1
        self.stg_schema_record_distinct_count = self.get_record_count(
            schema_name=self.stg_schema_name,
            connection=connection,
            distinct=True,
        )
        self.mrg_schema_record_count = self.get_record_count(
            schema_name=self.mrg_schema_name,
            connection=connection,
            distinct=False,
        )
        self.logger.info("Counts retrieved")

        self.write_to_snowflake(
            connection=connection,
            max_batchid=max_batchid,
        )
        self.logger.info("Counts written to Snowflake")

    @property
    def reconciled(self) -> bool:
        return (
            self.s3_file_count == self.encova_S3_file_count
            and self.stage_file_count >= self.raw_schema_record_count
            and self.raw_schema_record_count == self.stg_schema_record_count
            and self.stg_schema_record_distinct_count == self.mrg_schema_record_count
        )


@dataclasses.dataclass
class Handler:
    logger: structlog.BoundLogger
    s3_client: BaseClient
    connection: SnowflakeConnection
    batch_id: int

    @classmethod
    def factory(
        cls,
        logger: structlog.BoundLogger,
        s3_client: BaseClient,
        connection: SnowflakeConnection,
    ) -> Handler:
        batch_id = cls.get_next_batch_id(logger=logger, connection=connection)
        bound_logger = logger.bind(batch_id=batch_id)
        return cls(
            logger=bound_logger,
            s3_client=s3_client,
            connection=connection,
            batch_id=batch_id,
        )

    def truncate_table(self, snowflake_table: str):
        self.logger.info(f"truncating ETL_CTRL.{snowflake_table}")

        # cursor.execute(query)
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                                TRUNCATE TABLE ETL_CTRL.{snowflake_table};
                            """
                )
            ).fetchone()

    def get_parquet_file_counts(
        self,
        src_bucket: str,
        src_prefix: str,
        incremental_tables: list[Table],
        tgt_table_info,
    ) -> dict[str, int]:
        self.logger.info("Starting  Getting File counts from GW S3 Bucket.")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=src_bucket,
            Prefix=src_prefix,
        )
        # Filter results for parquet files only.
        parquet_objects = page_iterator.search(
            "Contents[?ends_with(Key, '.snappy.parquet')][]"
        )

        src_table_counts = defaultdict(int)
        for parquet in parquet_objects:
            key = parquet["Key"]
            parts = key.split("/")
            if "/heartbeat/" in key.lower():
                continue
            try:
                if src_bucket.startswith("beta-3"):
                    table_name = parts[3]
                    timestamp = parts[5]
                else:
                    table_name = parts[2]
                    timestamp = parts[4]
            except IndexError:
                table_name = "unknown"

            if table_name in incremental_tables and table_name in tgt_table_info:
                if timestamp <= tgt_table_info[table_name]["max_timestamp"]:
                    src_table_counts[table_name.upper()] += 1
        self.logger.info("File counts fetched successfully from GW S3 Bucket.")
        return src_table_counts

    def fetch_table_metadata(
        self,
        stage_name,
        table_name,
    ):
        """Fetch file metadata from Snowflake stage for a specific table."""
        cursor = self.connection.cursor()
        query = f"LIST {stage_name}/{table_name.lower()}/"
        self.logger.info(query)
        cursor.execute(query)

        table_info = {"count": 0, "max_timestamp": None}

        for row in cursor.fetchall():
            file_path = row[0]  # Full file path
            parts = file_path.split("/")

            if len(parts) >= 5:
                timestamp = parts[7]  # Extract timestamp

                # Increment file count
                table_info["count"] += 1

                # Update max timestamp
                if (
                    table_info["max_timestamp"] is None
                    or timestamp > table_info["max_timestamp"]
                ):
                    table_info["max_timestamp"] = timestamp

        cursor.close()
        print(table_info)
        return table_name, table_info

    def get_encova_s3_files_metadata(
        self,
        stage_name: str,
        incremental_tables: list[Table],
        max_workers: int,
    ):
        """Fetch metadata for all tables using ThreadPoolExecutor."""
        self.logger.info("Getting File metadata from encova s3 Bucket.")
        tgt_table_info = defaultdict(lambda: {"count": 0, "max_timestamp": None})

        with ThreadPoolExecutor(max_workers) as executor:
            future_to_table = {
                executor.submit(
                    self.fetch_table_metadata,
                    stage_name=stage_name,
                    table_name=table,
                ): table
                for table in incremental_tables
            }

            for future in as_completed(future_to_table):
                table_name, table_info = future.result()
                tgt_table_info[table_name] = table_info
        return tgt_table_info

    @classmethod
    def get_next_batch_id(
        cls,
        logger: structlog.BoundLogger,
        connection: SnowflakeConnection,
    ):
        logger.info(f"fetching batch_id")
        with connection.cursor() as cursor:
            (batch_id) = cursor.execute(
                "SELECT ETL_CTRL.GET_BATCHID() AS BATCHID;"
            ).fetchone()[0]
            return batch_id

    def write_all_table_counts_to_snowflake(
        self,
        tables: list[Table],
        max_workers: int,
        max_batchid: int,
    ) -> list[tuple[Table, Exception]]:
        exceptions: list[tuple[Table, Exception]] = []
        self.logger.info("Getting File counts from snowflake stage.")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for parallel execution
            future_to_table = {
                executor.submit(
                    table.write_counts_to_snowflake,
                    connection=self.connection,
                    max_batchid=max_batchid,
                ): table
                for table in tables
            }

            # Collect results as they complete
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    future.result()
                except Exception as exc:
                    exceptions.append((table, exc))

        return exceptions


@dataclasses.dataclass
class RecordValidator:
    raw_schema: str
    stg_schema: str
    mrg_schema: str
    sf_stage_name: str
    logger: structlog.BoundLogger
    connection: SnowflakeConnection
    snowflake_table: str
    core_center: str
    batch_id: int

    @classmethod
    def factory(
        cls,
        logger: structlog.BoundLogger,
        connection: SnowflakeConnection,
        raw_schema: str,
        stg_schema: str,
        mrg_schema: str,
        sf_stage_name: str,
        snowflake_table: str,
        core_center: str,
    ) -> RecordValidator:
        """Factory method to initialize the ValidationHandler."""
        batch_id = Handler.get_next_batch_id(logger=logger, connection=connection)
        return cls(
            logger=logger,
            connection=connection,
            raw_schema=raw_schema,
            stg_schema=stg_schema,
            mrg_schema=mrg_schema,
            sf_stage_name=sf_stage_name,
            snowflake_table=snowflake_table,
            core_center=core_center,
            batch_id=batch_id,
        )

    def validate_s3_to_raw(self, table_name, max_sf_batchid, min_end_time) -> str:
        """Validate S3 to RAW record comparison."""
        try:
            self.logger.info(f"Validating S3 to RAW for table: {table_name}")
            cursor = self.connection.cursor()

            query = f"""
                SELECT $1 AS data,
                    SUBSTR(metadata$filename,
                    CHARINDEX('part-',
                    metadata$filename)) AS filename
                FROM {self.sf_stage_name}/{table_name.lower()}/
                where METADATA$FILE_LAST_MODIFIED >= '{min_end_time}'
                MINUS
                SELECT CDA_RAW_DATA, CDA_FILE_NAME
                FROM {self.raw_schema}.{table_name.lower()}
                where batchid > ({max_sf_batchid})
            """

            cursor.execute(query)
            result = cursor.fetchall()
            return "Matching" if not result else "Error! NOT Matching"
        except Exception as exc:
            error_desc = f"Error during S3 to RAW validation for table: {table_name}"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def validate_raw_to_stg(self, table_name, min_batchid, binary_table_names) -> str:
        """Validate RAW to STG record comparison."""
        try:
            self.logger.info(f"Validating RAW to STG for table: {table_name}")
            cursor = self.connection.cursor()

            v_batchid = min_batchid

            query = f"""
                CREATE OR REPLACE TEMPORARY TABLE {self.raw_schema}.{table_name}_temp AS
                SELECT
                    LISTAGG(DISTINCT key, ', ') WITHIN GROUP (ORDER BY key) AS col_list,
                    LISTAGG(DISTINCT in_col,',') WITHIN GROUP (ORDER BY in_col) AS piv_col_list
                FROM (
                    SELECT seq, key, value,CONCAT('''', key, '''') as in_col
                    FROM {self.raw_schema}.{table_name} raw,
                         LATERAL FLATTEN(input => CDA_RAW_DATA, recursive => TRUE) t
                    WHERE UPPER(key) NOT IN ('GWCBI___SEQVAL','SRID','WKB')
                )f
            """

            cursor.execute(query)

            # Fetch column lists
            cursor.execute(
                f"SELECT col_list, piv_col_list FROM {self.raw_schema}.{table_name}_temp"
            )
            v_col_list, v_piv_col_list = cursor.fetchone()
            if table_name.upper() in binary_table_names:
                ls = v_col_list.split(",")
                V_modified_col_list = ", ".join(
                    [f"CAST({col} AS VARCHAR) AS {col}" for col in ls]
                )
            else:
                V_modified_col_list = v_col_list

            # Construct SQL queries

            sql1 = f"""
                SELECT {v_col_list}, cda_schema_id, cda_folder
                FROM (
                    SELECT raw.cda_file_name, cda_schema_id, cda_folder,
                           batchid, f.seq, f.key AS pivot_column, f.value::VARCHAR AS pivot_value
                    FROM {self.raw_schema}.{table_name} raw,
                        LATERAL FLATTEN(CDA_RAW_DATA, recursive => TRUE) f
                        WHERE batchid >= {v_batchid}
                ) PIVOT(MAX(pivot_value) FOR pivot_column IN ({v_piv_col_list})) AS
                p(file_name,cda_schema_id, cda_folder, batchid,seq,{v_col_list})
            """
            sql2 = f"""
                SELECT {V_modified_col_list}, cda_schema_id, cda_folder
                FROM {self.stg_schema}.{table_name}
                WHERE batchid >= {v_batchid}
            """
            final_query = f"{sql1} EXCEPT {sql2}"
            cursor.execute(final_query)
            result = cursor.fetchall()
            return "Matching" if not result else "Error! NOT Matching"
        except Exception as exc:
            error_desc = f"Error during RAW to STG validation for table: {table_name}"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def validate_stg_to_mrg(self, table_name, min_batchid) -> str:
        """Validate STG to MRG record comparison."""
        try:
            self.logger.info(f"Validating STG to MRG for table: {table_name}")
            cursor = self.connection.cursor()
            v_batchid = min_batchid
            sql = f"""
                    CREATE OR REPLACE TEMPORARY TABLE {self.stg_schema}.{table_name}_temp AS
                    SELECT LISTAGG(DISTINCT column_name, ',') WITHIN GROUP (ORDER BY column_name) AS stage_col
                    FROM information_schema.columns
                    WHERE table_name = UPPER('{table_name}')
                    AND table_schema = UPPER('{self.stg_schema}')

                    AND column_name NOT IN ('CDA_SCHEMA_ID', 'CDA_FOLDER', 'CDA_FILE_NAME', 'ROW_INSERT_TMS', 'PUBLICID',
                                            'UPDATETIME', 'ID', 'GWCBI___SEQVAL_HEX', 'GWCBI___LSN')
                    """
            cursor.execute(sql)

            # Get stage column list
            cursor.execute(f"SELECT stage_col FROM {self.stg_schema}.{table_name}_temp")
            v_stage_col = cursor.fetchone()[0]

            # Create temporary table for merge columns
            sql = f"""
                CREATE OR REPLACE TEMPORARY TABLE {self.stg_schema}.{table_name}_temp2 AS
                SELECT LISTAGG(DISTINCT column_name, ',') WITHIN GROUP (ORDER BY column_name) AS merge_col
                FROM information_schema.columns
                WHERE table_name = UPPER('{table_name}')
                AND table_schema = UPPER('{self.mrg_schema}')

                AND column_name NOT IN ('GW_DELETE_FLAG', 'ROW_INSERT_TMS', 'ROW_UPDATE_TMS', 'PUBLICID',
                                        'UPDATETIME', 'ID', 'GWCBI___SEQVAL_HEX', 'GWCBI___LSN')
            """
            cursor.execute(sql)

            # Get merge column list
            cursor.execute(
                f"SELECT merge_col FROM {self.stg_schema}.{table_name}_temp2"
            )
            v_merge_col = cursor.fetchone()[0]

            if table_name[:3].lower() in [
                "pc_",
                "bc_",
                "pcx",
                "bcx",
                "ccx",
                "abx",
                "cc_",
                "ab_",
            ]:
                sql1 = f"""
                    SELECT {v_stage_col}, t4.publicid, t4.id, t4.GWCBI___SEQVAL_HEX, t4.GWCBI___LSN
                    FROM (
                        SELECT MAX(cda_folder) AS maxcdatms, id
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id
                    ) t1
                    JOIN (
                        SELECT id, cda_folder, MAX(gwcbi___seqval_hex) AS maxhexval
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id, cda_folder
                    ) t2
                    ON t1.id = t2.id AND t1.maxcdatms = t2.cda_folder
                    JOIN (
                        SELECT id, cda_folder, gwcbi___seqval_hex, MAX(gwcbi___lsn) AS maxlsn
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id, cda_folder, gwcbi___seqval_hex
                    ) t3
                    ON t3.id = t2.id AND t3.cda_folder = t2.cda_folder
                    AND t3.gwcbi___seqval_hex = t2.maxhexval
                    JOIN {self.stg_schema}.{table_name} t4
                    ON t4.id = t3.id AND t4.gwcbi___seqval_hex = t3.gwcbi___seqval_hex
                    AND t4.cda_folder = t3.cda_folder AND t4.gwcbi___lsn = t3.maxlsn
                    WHERE gwcbi___operation <> 1 AND batchid >= {v_batchid}
                """

                sql2 = f"""
                    SELECT {v_merge_col}, publicid,id, GWCBI___SEQVAL_HEX, GWCBI___LSN
                    FROM {self.mrg_schema}.{table_name}
                    WHERE batchid >= {v_batchid}
                """
            elif table_name[:4].lower() in ["pctl", "bctl", "cctl", "abtl"]:
                sql1 = f"""
                    SELECT {v_stage_col}, t4.id, t4.GWCBI___SEQVAL_HEX, t4.GWCBI___LSN
                    FROM (
                        SELECT MAX(cda_folder) AS maxcdatms, id
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id
                    ) t1
                    JOIN (
                        SELECT id, cda_folder, MAX(gwcbi___seqval_hex) AS maxhexval
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id, cda_folder
                    ) t2
                    ON t1.id = t2.id AND t1.maxcdatms = t2.cda_folder
                    JOIN (
                        SELECT id, cda_folder, gwcbi___seqval_hex, MAX(gwcbi___lsn) AS maxlsn
                        FROM {self.stg_schema}.{table_name}
                        GROUP BY id, cda_folder, gwcbi___seqval_hex
                    ) t3
                    ON t3.id = t2.id AND t3.cda_folder = t2.cda_folder
                    AND t3.gwcbi___seqval_hex = t2.maxhexval
                    JOIN {self.stg_schema}.{table_name} t4
                    ON t4.id = t3.id AND t4.gwcbi___seqval_hex = t3.gwcbi___seqval_hex
                    AND t4.cda_folder = t3.cda_folder AND t4.gwcbi___lsn = t3.maxlsn
                    WHERE gwcbi___operation <> 1 AND batchid >= {v_batchid}
                """

                sql2 = f"""
                    SELECT {v_merge_col},  id, GWCBI___SEQVAL_HEX, GWCBI___LSN
                    FROM {self.mrg_schema}.{table_name}
                    WHERE batchid >= {v_batchid}
                """

            # Combine queries with EXCEPT
            final_sql = f"{sql1} EXCEPT {sql2}"
            cursor.execute(final_sql)

            results = cursor.fetchall()
            return "Matching" if not results else "Error! NOT Matching"
        except Exception as exc:
            error_desc = f"Error during STG to MRG validation for table: {table_name}"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def write_results_to_snowflake(
        self,
        table_name: str,
        sf_batchid: int,
        s3_to_raw_result: str,
        raw_to_stg_result: str,
        stg_to_mrg_result: str,
    ):
        """Write validation results to Snowflake."""
        try:
            self.logger.info(
                f"Writing validation results to Snowflake for table: {table_name}"
            )

            with self.connection.cursor() as cursor:
                # Insert results
                insert_query = f"""
                INSERT INTO {self.snowflake_table} (BATCH_ID,SF_BATCHID,Table_Name,S3_TO_RAW, RAW_to_STG, STG_to_MRG,ROW_INSERT_TMS)
                VALUES ({self.batch_id},{sf_batchid},'{table_name}','{s3_to_raw_result}', '{raw_to_stg_result}', '{stg_to_mrg_result}',CURRENT_TIMESTAMP);
                """
                cursor.execute(insert_query)

            self.logger.info(f"Results written to Snowflake for table: {table_name}")
        except Exception as exc:
            error_desc = f"Error writing results to Snowflake for table: {table_name}"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def validate_and_write(
        self,
        table_name: str,
        max_sf_batchid: int,
        max_batchid: int,
        min_end_time: str,
        min_batchid: int,
        binary_table_names: List[str],
    ):
        """Master function to validate and write results for a table."""

        # Perform validations
        s3_to_raw_result = self.validate_s3_to_raw(
            table_name, max_sf_batchid, min_end_time
        )
        raw_to_stg_result = self.validate_raw_to_stg(
            table_name, min_batchid, binary_table_names
        )
        stg_to_mrg_result = self.validate_stg_to_mrg(table_name, min_batchid)

        # Prepare results
        self.write_results_to_snowflake(
            table_name=table_name,
            sf_batchid=max_batchid,
            s3_to_raw_result=s3_to_raw_result,
            raw_to_stg_result=raw_to_stg_result,
            stg_to_mrg_result=stg_to_mrg_result,
        )

    def truncate_table(self, snowflake_table: str):
        self.logger.info(f"truncating ETL_CTRL.{snowflake_table}")

        # cursor.execute(query)
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                            TRUNCATE TABLE ETL_CTRL.{snowflake_table};
                        """
                )
            ).fetchone()

    def parallel_master_validation(
        self,
        tables: List[str],
        max_workers: int,
        max_sf_batchid: int,
        max_batchid: int,
        min_end_time: str,
        min_batchid: int,
        binary_table_names: List[str],
    ) -> None:
        exceptions: list[tuple[Table, Exception]] = []
        """Run master validation for multiple tables in parallel."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(
                    self.validate_and_write,
                    table_name,
                    max_sf_batchid,
                    max_batchid,
                    min_end_time,
                    min_batchid,
                    binary_table_names,
                ): table_name
                for table_name in tables
            }

            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    future.result()
                    self.logger.info(f"Validation completed for table: {table_name}")
                except Exception as exc:
                    self.logger.exception(
                        f"Error during validation for table: {table_name}"
                    )
                    exceptions.append((table_name, exc))
        return exceptions


def main(
    logger: structlog.BoundLogger,
    secret_name: str,
    token_endpoint: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    gw_cda_bucket: str,
    gw_s3_dir: str,
    sf_stage_name: str,
    core_center: str,
    aws_session: AWSSession,
    raw_schema_name: str,
    stg_schema_name: str,
    mrg_schema_name: str,
    record_validation_flag: bool,
    is_incremental: bool,
) -> None:
    logger.info(
        f"QA Data Reconciliation from GW-S3 to MRG Layer job has started for {core_center}"
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

    max_s3_workers = 60
    max_sf_workers = 20
    boto_config = Config(max_pool_connections=max_s3_workers)
    s3_client = aws_session.client("s3", config=boto_config)

    if record_validation_flag:
        handler2 = RecordValidator.factory(
            logger=logger,
            connection=sf_connection,
            raw_schema=raw_schema_name,
            stg_schema=stg_schema_name,
            mrg_schema=mrg_schema_name,
            sf_stage_name=sf_stage_name,
            snowflake_table=f"JC_{core_center.lower()}_E2E_RECORD_VALIDATION",
            core_center=core_center,
        )

        if not is_incremental:
            handler2.truncate_table(
                snowflake_table=f"JC_{core_center.lower()}_E2E_RECORD_VALIDATION"
            )

        fetch_batchid = f"SELECT coalesce(max(SF_BATCHID),0) FROM ETL_CTRL.JC_{core_center.upper()}_E2E_RECORD_VALIDATION"
        cursor = sf_connection.cursor()
        cursor.execute(fetch_batchid)
        max_sf_batchid = cursor.fetchone()[0]
        fetch_table_query = f"""select TABLE_NAME
                                FROM ETL_CTRL.JC_MRG_{core_center.upper()}_LOAD_STATUS
                                where Batch_id > ({max_sf_batchid}) """
        # order by 1 limit 10

        cursor.execute(fetch_table_query)
        sf_tables = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            f"""
                SELECT
                    MAX(batchid) AS max_batchid,
                    MIN(end_time) AS min_end_time,
                    MIN(batchid) AS min_batchid,
                FROM etl_ctrl.jc_batch_status
                where (SUBSTRING(batch_name, 22, 2) = lower('{core_center}')
                or SUBSTRING(batch_name, 18, 2) = lower('{core_center}')
                ) and
                batchid >({max_sf_batchid} )
                and end_time is not null
            """
        )
        max_batchid, min_end_time, min_batchid = cursor.fetchone()

        binary_table_names = [
            "BC_INSTRUMENTEDMESSAGE",
            "BCX_REVIEWCASE_EXT",
            "BC_USERUIPREFERENCES",
            "BCX_REVIEWCASE_EXT",
            "CC_USERUIPREFERENCES",
            "CC_INSTRUMENTEDMESSAGE",
            "AB_USERUIPREFERENCES",
            "PCX_REVIEWCASE",
            "PC_RATEBOOKEXPORTRESULT",
            "PCX_BUILDINGGEODATA_EXT",
            "PCX_RATINGSWHISTORY_EXT",
            "PCX_PPBIGCNTNTCONTAINER_EXT",
            "PC_USERUIPREFERENCES",
            "PCX_MUCINTERIMRESULT_EXT",
            "PC_INSTRUMENTEDMESSAGE",
            "PC_REVIEWCASE",
            "PC_WORKSHEETDATA",
        ]

        exceptions = handler2.parallel_master_validation(
            tables=sf_tables,
            max_workers=max_sf_workers,
            max_sf_batchid=max_sf_batchid,
            max_batchid=max_batchid,
            min_end_time=min_end_time,
            min_batchid=min_batchid,
            binary_table_names=binary_table_names,
        )

        if exceptions:
            for table, exc in exceptions:
                logger.exception(f"Exception: {table.table_name}")
            raise exceptions[0][1]

    else:
        logger.info("Skipping record validation as record_validation_flag is False.")

        handler = Handler.factory(
            logger=logger,
            s3_client=s3_client,
            connection=sf_connection,
        )

        if not is_incremental:
            handler.truncate_table(
                snowflake_table=f"JC_{core_center.lower()}_E2E_COUNT_VALIDATION"
            )

        fetch_batchid = f"SELECT coalesce(max(SF_BATCHID),0) FROM ETL_CTRL.JC_{core_center.upper()}_E2E_COUNT_VALIDATION"
        cursor = sf_connection.cursor()
        cursor.execute(fetch_batchid)
        max_sf_batchid = cursor.fetchone()[0]
        query = f"SELECT TABLE_NAME FROM ETL_CTRL.JC_MRG_{core_center.upper()}_LOAD_STATUS where Batch_id > ({max_sf_batchid}) "

        cursor.execute(query)

        # Fetch table names
        incremental_tables = {row[0] for row in cursor.fetchall()}

        cursor.execute(
            f"""
                SELECT
                    MAX(batchid) AS max_batchid,
                FROM etl_ctrl.jc_batch_status
                where (SUBSTRING(batch_name, 22, 2) = lower('{core_center}')
                or SUBSTRING(batch_name, 18, 2) = lower('{core_center}') ) and
                batchid >({max_sf_batchid} )
                and end_time is not null
            """
        )
        max_batchid = cursor.fetchone()[0]

        metadata = handler.get_encova_s3_files_metadata(
            sf_stage_name,
            incremental_tables,
            max_sf_workers,
        )

        parquet_file_counts = handler.get_parquet_file_counts(
            src_bucket=gw_cda_bucket,
            src_prefix=gw_s3_dir,
            incremental_tables=incremental_tables,
            tgt_table_info=metadata,
        )

        tables: list[Table] = [
            Table.factory(
                batch_id=handler.batch_id,
                core_center=core_center,
                table_name=table_name,
                stage_name=sf_stage_name,
                raw_schema_name=raw_schema_name,
                stg_schema_name=stg_schema_name,
                mrg_schema_name=mrg_schema_name,
                s3_file_count=s3_file_count,
                encova_S3_file_count=metadata.get(table_name.lower(), {}).get(
                    "count", 0
                ),
                logger=logger,
            )
            for table_name, s3_file_count in parquet_file_counts.items()
        ]

        exceptions = handler.write_all_table_counts_to_snowflake(
            tables=tables, max_workers=max_sf_workers, max_batchid=max_batchid
        )

        if exceptions:
            for table, exc in exceptions:
                logger.exception(f"Exception: {table.table_name}")
            raise exceptions[0][1]

        handler.logger.info("Success")


def get_core_center_config(in_args: dict[str, str]) -> dict[str, str]:
    core_center_configs = {
        "pc": {
            "gw_cda_bucket": in_args["gw_pc_s3_bucket"],
            "gw_s3_dir": in_args["gw_pc_dir"],
            "sf_stage_name": "@pc_stage/__copy_into__/__archive__",
            "raw_schema_name": "RAW_PC",
            "stg_schema_name": "STG_PC",
            "mrg_schema_name": "MRG_PC",
        },
        "bc": {
            "gw_cda_bucket": in_args["gw_bc_s3_bucket"],
            "gw_s3_dir": in_args["gw_bc_dir"],
            "sf_stage_name": "@bc_stage/__copy_into__/__archive__",
            "raw_schema_name": "RAW_BC",
            "stg_schema_name": "STG_BC",
            "mrg_schema_name": "MRG_BC",
        },
        "cc": {
            "gw_cda_bucket": in_args["gw_cc_s3_bucket"],
            "gw_s3_dir": in_args["gw_cc_dir"],
            "sf_stage_name": "@cc_stage/__copy_into__/__archive__",
            "raw_schema_name": "RAW_CC",
            "stg_schema_name": "STG_CC",
            "mrg_schema_name": "MRG_CC",
        },
        "cm": {
            "gw_cda_bucket": in_args["gw_cm_s3_bucket"],
            "gw_s3_dir": in_args["gw_cm_dir"],
            "sf_stage_name": "@cm_stage/__copy_into__/__archive__",
            "raw_schema_name": "RAW_CM",
            "stg_schema_name": "STG_CM",
            "mrg_schema_name": "MRG_CM",
        },
    }

    _core_center = in_args["core_center"]

    if _core_center not in core_center_configs:
        raise ValueError(
            f"Invalid arguments. Must provide one of {', '.join([key for key in core_center_configs.keys()])}."
        )

    core_center_config = core_center_configs[_core_center]
    if core_center_config["gw_s3_dir"] == "NOT AVAILABLE":
        core_center_config["gw_s3_dir"] = ""

    return core_center_config


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    _args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
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
            # Dynamic parameters
            "core_center",
            "record_validation_flag",
            "is_incremental",
        ],
    )

    _sc = SparkContext()
    _glue_context = GlueContext(_sc)
    _structlogger = get_structlog_logger(_glue_context.get_logger())
    _job = Job(_glue_context)
    _job.init(_args["JOB_NAME"] + _args["core_center"], _args)

    _core_center_config = get_core_center_config(_args)

    session = AWSSession(region_name="us-east-1")

    # noinspection DuplicatedCode
    main(
        logger=_structlogger,
        secret_name=_args["secret_name"],
        token_endpoint=_args["token_endpoint"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        snowflake_role=_args["snowflake_role"],
        gw_cda_bucket=_core_center_config["gw_cda_bucket"],
        gw_s3_dir=_core_center_config["gw_s3_dir"],
        sf_stage_name=_core_center_config["sf_stage_name"],
        core_center=_args["core_center"],
        aws_session=session,
        raw_schema_name=_core_center_config["raw_schema_name"],
        stg_schema_name=_core_center_config["stg_schema_name"],
        mrg_schema_name=_core_center_config["mrg_schema_name"],
        record_validation_flag=(
            True if _args["record_validation_flag"].upper() == "TRUE" else False
        ),
        is_incremental=(True if _args["is_incremental"].upper() == "TRUE" else False),
    )

    _job.commit()

elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    _structlogger = get_structlog_logger(logger=_logger)

    _args = {
        ## Hard-coded parameters
        "JOB_NAME": "QA_DATA_RECONCILIATION",
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
        # Dynamic parameters
        "core_center": "cm",
        "record_validation_flag": "True",
        "is_incremental": "True",
    }

    _core_center_config = get_core_center_config(_args)

    _assumed_role_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )

    # noinspection DuplicatedCode
    main(
        logger=_structlogger,
        secret_name=_args["secret_name"],
        token_endpoint=_args["token_endpoint"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        snowflake_role=_args["snowflake_role"],
        gw_cda_bucket=_core_center_config["gw_cda_bucket"],
        gw_s3_dir=_core_center_config["gw_s3_dir"],
        sf_stage_name=_core_center_config["sf_stage_name"],
        core_center=_args["core_center"],
        aws_session=_assumed_role_session,
        raw_schema_name=_core_center_config["raw_schema_name"],
        stg_schema_name=_core_center_config["stg_schema_name"],
        mrg_schema_name=_core_center_config["mrg_schema_name"],
        record_validation_flag=(
            True if _args["record_validation_flag"].upper() == "TRUE" else False
        ),
        is_incremental=(True if _args["is_incremental"].upper() == "TRUE" else False),
    )
