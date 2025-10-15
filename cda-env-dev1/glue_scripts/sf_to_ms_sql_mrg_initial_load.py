from __future__ import annotations

import dataclasses
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Generator

import structlog
from boto3.session import Session as AWSSession
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    expr,
    max as spark_max,
    from_json,
    lit,
    concat,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from utilities.JDBCConnection import JDBCConnection, JDBCResultSet
from utilities.get_local_spark_session import get_local_spark_session
from utilities.logging_utils import get_structlog_logger
from utilities.oauth_utils import get_assumed_role_session, get_token, get_secret
from utilities.snowflake_utils import get_snowflake_connection

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
    logging.getLogger(noisy_logger).setLevel(logging.ERROR)


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# Thread-local storage for JDBC connections
thread_local = threading.local()

# Define the schema for the geography JSON field to extract 'srid' and 'wkb' values
GEOGRAPHY_SCHEMA = StructType(
    [
        StructField("srid", IntegerType(), True),
        StructField("wkb", StringType(), True),
    ]
)


def get_next_batch_id(connection: SnowflakeConnection, logger: structlog.BoundLogger):
    logger.info(f"fetching batch_id")
    with connection.cursor() as cursor:
        (batch_id) = cursor.execute(
            "SELECT ETL_CTRL.SEQ_MSSQL_BATCHID.NEXTVAL AS BATCHID;"
        ).fetchone()[0]
        return batch_id


@dataclasses.dataclass(kw_only=True)
class MsSqlClient:
    ms_sql_jdbc_url: str
    ms_sql_jdbc_properties: dict[str, str | int]
    logger: structlog.BoundLogger

    def get_connection(self) -> JDBCConnection:
        try:
            self.logger.info(
                "Establishing MS SQL Server connection using JDBC Connection ."
            )
            # TODO this may not work if it cannot accept batch_limit to this type of connection
            spark_session = SparkSession.builder.getOrCreate()

            # Load the driver class
            # noinspection PyProtectedMember,PyUnresolvedReferences
            spark_session._jvm.Class.forName(self.ms_sql_jdbc_properties["driver"])

            # noinspection PyProtectedMember,PyUnresolvedReferences
            connection = spark_session._jvm.java.sql.DriverManager.getConnection(
                self.ms_sql_jdbc_url,
                self.ms_sql_jdbc_properties["user"],
                self.ms_sql_jdbc_properties["password"],
            )
            self.logger.info("Connection established to the On-prem server.")
            return connection
        except Exception as exc:
            error_desc = f"Failed to establish MS SQL server connection"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    @property
    def connection(self) -> JDBCConnection:
        if not hasattr(thread_local, "jdbc_connection"):
            thread_local.jdbc_connection = self.get_connection()
        return thread_local.jdbc_connection

    @contextmanager
    def execute_query(self, query: str) -> Generator[JDBCResultSet, None, None]:
        stmt = self.connection.createStatement()
        try:
            yield stmt.executeQuery(query)
        finally:
            stmt.close()

    def execute_update(self, query: str) -> int:
        stmt = self.connection.createStatement()
        try:
            return stmt.executeUpdate(query)
        finally:
            stmt.close()

    def write_to_error_log(
        self,
        table_name: str,
        current_mssql_job_batch_id: int,
        error_msg: str,
        error_sql: str,
        core_center: str,
        sf_batch_id: int = 0,
    ) -> None:
        error_log_table_name = f"JC_MSSQL_{core_center.upper()}_ERROR_LOG"
        try:
            # Truncate error message if it exceeds 8000 characters
            if len(str(error_msg)) > 8000:
                error_msg = str(error_msg)[:7997] + "..."

            # Replace problematic characters in error message and SQL
            error_msg = str(error_msg).replace("'", " ")
            error_sql = str(error_sql).replace("'", " ")

            # Insert error log into the database

            insert_query = f"""
                INSERT INTO {error_log_table_name} (
                    MSSQL_BATCH_ID,
                    SF_BATCH_ID,
                    ERROR_MESSAGE,
                    ERROR_SQL,
                    ROW_INSERT_TIMESTAMP,
                    TABLE_NAME
                ) VALUES (
                    {current_mssql_job_batch_id},
                    {sf_batch_id},
                    '{error_msg}',
                    '{error_sql}',
                    CURRENT_TIMESTAMP,
                    '{table_name}'
                );
            """
            self.execute_update(insert_query)

            self.logger.info(
                f"Wrote '{table_name}' error message to {error_log_table_name} table. "
                f"BATCH_ID: {sf_batch_id if sf_batch_id != 0 else current_mssql_job_batch_id}",
            )

        except Exception as exc:
            error_desc = (
                f"Error writing '{table_name}' error message to {error_log_table_name} table. "
                f"BATCH_ID: {sf_batch_id if sf_batch_id != 0 else current_mssql_job_batch_id}"
            )
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc


@dataclasses.dataclass(kw_only=True)
class Table:
    logger: structlog.BoundLogger
    base_table_name: str
    table_suffix: str
    schema_name: str
    core_center: str
    current_sf_table_batch_id: int
    last_successful_sf_table_batch_id: int
    current_mssql_job_batch_id: int
    extracts_stage_s3_bucket_name: str
    mssql_client: MsSqlClient
    sf_incremental_record_count: int = 0
    sf_soft_delete_record_count: int = 0
    deleted_incremental_record_count: int = 0
    deleted_soft_delete_record_count: int = 0
    s3_unload_record_count: int = 0

    @property
    def table_name(self):
        if self.table_suffix is None:
            return self.base_table_name
        else:
            return f"{self.base_table_name}_{self.table_suffix}"

    def set_sf_incremental_and_soft_delete_record_counts(
        self, cursor: SnowflakeCursor
    ) -> None:
        export_count_query = f"""
                    SELECT NVL(COUNT_IF(GW_DELETE_FLAG <> 'Y'), 0) AS COUNT,
                           NVL(COUNT_IF(GW_DELETE_FLAG = 'Y'), 0)  AS SOFT_DELETE_COUNT
                    FROM {self.schema_name}.{self.base_table_name}
                    WHERE BATCHID > {self.last_successful_sf_table_batch_id}

                """
        cursor.execute(export_count_query)
        result = cursor.fetchone()
        if result:
            self.sf_incremental_record_count = result[0]
            self.sf_soft_delete_record_count = result[1]

    def copy_into_s3(self, cursor: SnowflakeCursor) -> None:
        unload_query = f"""
                    COPY INTO @ETL_CTRL.EXTRACTS/{self.stage_path}
                    FROM (
                        SELECT *
                        FROM {self.schema_name}.{self.base_table_name}
                        WHERE BATCHID > {self.last_successful_sf_table_batch_id})
                        FILE_FORMAT = (TYPE = 'PARQUET'
                    )
                    OVERWRITE = TRUE
                    HEADER = TRUE;
                """
        cursor.execute(unload_query)
        result = cursor.fetchall()

        self.logger.info(
            f"Successfully unloaded table {self.base_table_name} to {self.s3_path}"
        )
        if result:
            self.s3_unload_record_count = result[0][0]

    @property
    def stage_path(self) -> str:
        return f"sf_to_onprem_mrg_extract/{self.core_center}/{self.base_table_name}/"

    @property
    def s3_path(self) -> str:
        if IS_GLUE_JOB:
            return f"s3://{self.extracts_stage_s3_bucket_name.rstrip('/')}/{self.stage_path}"
        else:
            # Local Hadoop prefix has to be s3a
            return f"s3a://{self.extracts_stage_s3_bucket_name.rstrip('/')}/{self.stage_path}"

    def unload_sf_table_to_s3(self, snowflake_connection: SnowflakeConnection) -> None:
        self.logger.info(f"Starting unload for table: {self.base_table_name}")

        with snowflake_connection.cursor() as cursor:
            self.set_sf_incremental_and_soft_delete_record_counts(cursor=cursor)

            total_count = (
                self.sf_incremental_record_count + self.sf_soft_delete_record_count
            )
            self.copy_into_s3(cursor=cursor)

            if self.s3_unload_record_count != total_count:
                error_desc = (
                    f"Snowflake Table {self.base_table_name} Record Count {total_count} for Batchid "
                    f"{self.last_successful_sf_table_batch_id} is not matching with Records "
                    f"unloaded into S3 {self.s3_unload_record_count}"
                )
                self.logger.error(error_desc)
                raise Exception(error_desc)

    def load_and_validate_data(self, spark: SparkSession) -> DataFrame:
        df = spark.read.parquet(self.s3_path)
        if df.rdd.isEmpty():
            self.logger.info(f"No data to load for table {self.base_table_name}")
            raise Exception(
                f"Error while reading data from Spark for '{self.base_table_name}', there is no data for this table"
            )

        if "batchid" in [column.lower() for column in df.columns]:
            self.current_sf_table_batch_id = df.agg(
                spark_max("batchid").alias("max_batch_id")
            ).collect()[0]["max_batch_id"]
            self.logger.info(
                f"Extracted latest BATCH_ID for table '{self.base_table_name}' from spark dataframe is: {self.current_sf_table_batch_id}"
            )
            return df
        else:
            self.logger.warning(
                f"BATCHID column not found in data for table '{self.base_table_name}'"
            )
            raise Exception(
                f"BATCHID column not found in data for table '{self.base_table_name}'"
            )

    def merge_jc_ms_sql_load_status(
        self,
        current_sf_table_batch_id: int,
        status: str,
    ) -> None:
        status = status.lower()
        if status not in ["started", "completed", "failed"]:
            raise Exception(
                f"unhandled status: {status}. Must be one of 'started', 'completed', or 'failed'."
            )

        # TODO: Refactor onprem to ms_sql
        merge_query = f"""
                    MERGE INTO JC_MSSQL_{self.core_center.upper()}_LOAD_STATUS AS TGT
                    USING (
                        VALUES (
                            {self.current_mssql_job_batch_id},
                            {current_sf_table_batch_id},
                            '{self.table_name}',
                            '{status}',
                            GETDATE(),
                            GETDATE()
                        )
                    ) AS SRC (
                        MSSQL_BATCH_ID,
                        SF_BATCH_ID,
                        TABLE_NAME,
                        STATUS,
                        ROW_INSERT_TIMESTAMP,
                        ROW_UPDATE_TIMESTAMP
                    )
                    ON TGT.SF_BATCH_ID = SRC.SF_BATCH_ID
                        AND TGT.MSSQL_BATCH_ID = SRC.MSSQL_BATCH_ID
                        AND TGT.TABLE_NAME = SRC.TABLE_NAME
                    WHEN MATCHED THEN
                        UPDATE SET TGT.STATUS = SRC.STATUS,
                            TGT.ROW_UPDATE_TIMESTAMP = SRC.ROW_UPDATE_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (
                            MSSQL_BATCH_ID,
                            SF_BATCH_ID,
                            TABLE_NAME,
                            STATUS,
                            ROW_INSERT_TIMESTAMP,
                            ROW_UPDATE_TIMESTAMP
                        )
                        VALUES (
                            SRC.MSSQL_BATCH_ID,
                            SRC.SF_BATCH_ID,
                            SRC.TABLE_NAME,
                            SRC.STATUS,
                            SRC.ROW_INSERT_TIMESTAMP,
                            SRC.ROW_UPDATE_TIMESTAMP
                        );
                    ;
                """

        try:
            self.mssql_client.execute_update(merge_query)
        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=merge_query,
                core_center=self.core_center,
            )
            error_desc = f"Failed to update load status for table '{self.table_name}'"
            self.logger.error(f"{error_desc}: {exc}")
            raise Exception(error_desc) from exc

    def process_records_for_deletion(
        self, df: DataFrame
    ) -> Tuple[DataFrame | None, DataFrame | None, DataFrame]:
        index_col = "ID"

        if index_col.upper() not in [column.upper() for column in df.columns]:
            error_desc = f"ID column not found in table {self.base_table_name}"
            self.logger.error(error_desc)
            raise Exception(error_desc)

        df_incremental = df.filter(df["GW_DELETE_FLAG"] != "Y")
        df_soft_delete = df.filter(df["GW_DELETE_FLAG"] == "Y")

        df_del_incremental_ids = df_incremental.select(index_col).distinct()
        df_del_soft_delete_ids = df_soft_delete.select(index_col).distinct()

        return df_del_incremental_ids, df_del_soft_delete_ids, df_incremental

    def delete_ms_sql_records(
        self,
        df_del_ids: DataFrame | None,
        delete_type: str,  # "incremental_delete" or "soft_delete"
    ) -> int:
        if df_del_ids is None or df_del_ids.rdd.isEmpty():
            self.logger.info(
                f"No {delete_type} records to delete from table: {self.table_name}"
            )
            return 0

        else:
            to_delete_ids_count = df_del_ids.count()
            self.logger.info(
                f"Starting to delete {to_delete_ids_count} {delete_type} records from On-prem {self.table_name} table"
            )

        del_stg_table_name = f"JC_MSSQL_{self.core_center.upper()}_DELETE_STG"

        self.load_process_log(
            current_mssql_job_batch_id=self.current_mssql_job_batch_id,
            current_sf_table_batch_id=self.current_sf_table_batch_id,
            process_name=f"{delete_type} records Identified to delete",
            table_name=self.table_name,
            row_count=to_delete_ids_count,
        )

        # Add columns to DataFrame before writing
        df_del_ids = (
            df_del_ids.withColumn("TABLE_NAME", lit(self.table_name))
            .withColumn("MSSQL_BATCH_ID", lit(self.current_mssql_job_batch_id))
            .withColumn("DELETE_TYPE", lit(delete_type))
        )

        # Insert into staging table
        df_del_ids.write.jdbc(
            url=self.mssql_client.ms_sql_jdbc_url,
            table=del_stg_table_name,
            mode="append",
            properties=self.mssql_client.ms_sql_jdbc_properties,
        )

        self.logger.info(
            f"Successfully inserted {to_delete_ids_count} {delete_type} records into {del_stg_table_name} table"
        )

        self.load_process_log(
            current_mssql_job_batch_id=self.current_mssql_job_batch_id,
            current_sf_table_batch_id=self.current_sf_table_batch_id,
            process_name=f"Inserted {delete_type} IDs into Delete Staging Table",
            table_name=self.table_name,
            row_count=to_delete_ids_count,
        )

        # Delete records from the actual table
        delete_query = f"""
            DELETE FROM {self.table_name}
            WHERE ID IN (
                SELECT ID
                FROM {del_stg_table_name}
                WHERE TABLE_NAME = '{self.table_name}'
                AND MSSQL_BATCH_ID = {self.current_mssql_job_batch_id}
                AND DELETE_TYPE = '{delete_type}'
            )
        """

        try:
            deleted_count = self.mssql_client.execute_update(delete_query)
            self.logger.info(
                f"Deleted {deleted_count} {delete_type} records from table '{self.table_name}' using staging table."
            )

            self.load_process_log(
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                current_sf_table_batch_id=self.current_sf_table_batch_id,
                process_name=f"Deleted {delete_type} records from the table",
                table_name=self.table_name,
                row_count=deleted_count,
            )

            # delete records from Delete Staging table once done
            del_stg_query = f"""
                DELETE FROM {del_stg_table_name}
                WHERE TABLE_NAME = '{self.table_name}'
                AND MSSQL_BATCH_ID = {self.current_mssql_job_batch_id}
                AND DELETE_TYPE = '{delete_type}'
            """
            self.mssql_client.execute_update(del_stg_query)
            self.logger.info(
                f"Successfully deleted records from '{del_stg_table_name}' table after inserting records into Delete Stage table and then deleting in '{self.table_name}'."
            )

            # return to delete count for validation
            return deleted_count

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=delete_query,
                core_center=self.core_center,
            )
            raise Exception(
                f"Error deleting {delete_type} records from table {self.table_name} using staging table."
            ) from exc

    def get_ms_sql_column_map(
        self,
        spark: SparkSession,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str],
    ) -> dict[str, Row]:
        # Fetch SQL Server schema information
        query = f"""
                (SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{self.table_name}') AS SCHEMA_INFO
            """
        df = spark.read.jdbc(
            url=ms_sql_jdbc_url,
            table=query,
            properties=ms_sql_jdbc_properties,
        )

        # Map SQL Server schema
        return {row["COLUMN_NAME"].lower(): row for row in df.collect()}

    def handle_geography_column(self, geo_df: DataFrame, col_name: str) -> DataFrame:
        # Parse the JSON to get SRID and WKB
        geo_df = geo_df.withColumn(col_name, from_json(col(col_name), GEOGRAPHY_SCHEMA))

        # Check SRID is 4326
        invalid_srid_count = geo_df.filter(
            geo_df[col_name]["srid"] != lit(4326)
        ).count()
        if invalid_srid_count > 0:
            self.logger.error(
                f"Job failed: Code is designed to handle SRID of only 4326 in geography column '{col_name}' for table {self.table_name}"
            )
            raise ValueError(
                f"Invalid SRID in column '{col_name}'. Expected SRID 4326, but found other values."
            )

        # Replace JSON with only the 'wkb' field and add '0x' prefix
        geo_df = geo_df.withColumn(
            col_name,
            concat(lit("0x"), geo_df[col_name].getField("wkb")),
        )
        return geo_df

    def handle_all_geography_columns(
        self, df: DataFrame, geo_columns: list[str]
    ) -> tuple[DataFrame | None, DataFrame | None]:
        if not geo_columns:
            self.logger.info(
                f"No geography columns identified in the schema for table {self.table_name}."
            )
            return df, None

        self.logger.info(f"Geography columns identified: {geo_columns}")

        # Select ID, BATCHID, and geography columns for staging
        geo_df = df.select("ID", *geo_columns)

        # Drop rows where all geography columns are null
        geo_df = geo_df.dropna(subset=geo_columns)

        # Parse JSON for each geography column and validate SRID
        for col_name in geo_columns:
            geo_df = self.handle_geography_column(geo_df=geo_df, col_name=col_name)

        self.logger.info(
            f"Geography columns prepared for table: {self.table_name} are prepared to load into staging table"
        )

        # Drop geography columns from the main DataFrame for loading into the main table
        df = df.drop(*geo_columns)

        return df, geo_df

    def get_column_expression(
        self,
        col_name: str,
        data_type: str,
        numeric_precision: int,
        numeric_scale: int,
    ) -> Column | None:
        # Define casting expressions based on SQL Server column type
        if data_type in [
            "varchar",
            "nvarchar",
            "char",
            "nchar",
            "text",
            "ntext",
        ]:
            return expr(f"CAST(`{col_name}` AS STRING) AS `{col_name}`")
        elif data_type in ["int", "bigint", "smallint", "tinyint"]:
            return expr(f"CAST(`{col_name}` AS LONG) AS `{col_name}`")
        elif data_type in ["decimal", "numeric"]:
            return expr(
                f"CAST(`{col_name}` AS DECIMAL({numeric_precision},{numeric_scale})) AS `{col_name}`"
            )
        elif data_type in ["float", "real"]:
            return expr(f"CAST(`{col_name}` AS DOUBLE) AS `{col_name}`")
        elif data_type == "date":
            return expr(f"CAST(`{col_name}` AS DATE) AS `{col_name}`")
        elif data_type in [
            "datetime",
            "smalldatetime",
            "datetime2",
            "timestamp",
        ]:
            return expr(f"CAST(`{col_name}` AS TIMESTAMP) AS `{col_name}`")
        elif data_type in ["varbinary", "binary", "image"]:
            return expr(f"CAST(`{col_name}` AS BINARY) AS `{col_name}`")
        elif data_type == "bit":
            return expr(
                f"""CAST(
                        CASE
                            WHEN `{col_name}` = TRUE THEN 1
                            WHEN `{col_name}` = FALSE THEN 0
                            WHEN `{col_name}` IS NULL THEN NULL
                        END AS INT
                    ) AS `{col_name}`"""
            )
        elif data_type == "geography":
            return None
        else:
            raise ValueError(
                f"Unsupported data type '{data_type}' for column '{col_name}' in table '{self.table_name}'"
            )

    def adjust_data_types(
        self,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str | int],
        df: DataFrame,
        spark: SparkSession,
    ) -> Tuple[DataFrame, Optional[DataFrame]]:
        self.logger.info(
            f"Identifying schema for '{self.table_name}' from On-prem Server"
        )

        ms_sql_column_map = self.get_ms_sql_column_map(
            spark=spark,
            ms_sql_jdbc_url=ms_sql_jdbc_url,
            ms_sql_jdbc_properties=ms_sql_jdbc_properties,
        )

        # Identify common columns
        df_columns = set([column.lower() for column in df.columns])
        ms_sql_columns = set(ms_sql_column_map.keys())
        common_columns = df_columns.intersection(ms_sql_columns)

        # Load only the common columns (those present in both MS SQL Server and Snowflake)
        if not common_columns:
            raise ValueError(
                f"No matching columns found between Snowflake and SQL Server for table '{self.table_name}'."
            )

        # Identify geography columns
        geography_columns = [
            column
            for column in common_columns
            if ms_sql_column_map[column]["DATA_TYPE"].lower() == "geography"
        ]
        df, df_incremental_stage = self.handle_all_geography_columns(
            df=df, geo_columns=geography_columns
        )

        # Construct expressions to cast and select columns based on SQL Server schema
        columns_expr = []
        for col_name in common_columns:
            sql_col_info = ms_sql_column_map[col_name]
            sql_data_type = sql_col_info["DATA_TYPE"].lower()
            numeric_precision = sql_col_info[
                "NUMERIC_PRECISION"
            ]  # Added this line to get the numeric precision value from on prem.
            numeric_scale = sql_col_info[
                "NUMERIC_SCALE"
            ]  # Added this line to get the numeric scale from on prem.
            columns_expr.append(
                self.get_column_expression(
                    col_name=col_name,
                    data_type=sql_data_type,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                )  # Passing numeric precision and numeric scale as Paramters.
            )

        # Select only the transformed columns for the main DataFrame
        df = df.select(*[col_exp for col_exp in columns_expr if col_exp is not None])
        self.logger.info(
            f"Schema validation and data type adjustments completed for '{self.table_name}'."
        )

        # Return main DataFrame and staging DataFrame for geography columns
        return df, df_incremental_stage

    def convert_wkb_to_geography(
        self,
        geo_column: str,
        stg_table_name: str,
    ) -> None:
        self.logger.info(
            "Starting convert_wkb_to_geography function with new query structure using ID matching"
        )

        # Updated query to handle WKB data as described
        conversion_query = f"""
            DECLARE @id INT;
            DECLARE @spatialpoint NVARCHAR(MAX);
            DECLARE @geography GEOGRAPHY;
            DECLARE @wkbOriginal VARBINARY(MAX);
            DECLARE @wkbNoSRID VARBINARY(MAX);

            DECLARE cur CURSOR FOR
            SELECT ID, GEO_COLUMN
            FROM {stg_table_name}
            WHERE TABLE_NAME = '{self.table_name}';

            OPEN cur;

            FETCH NEXT FROM cur INTO @id, @spatialpoint;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Convert the hex string to varbinary directly
                SET @wkbOriginal = CONVERT(VARBINARY(MAX), @spatialpoint, 1);

                -- Extract and concatenate the relevant parts:
                -- First 4 bytes (prefix)
                DECLARE @prefix VARBINARY(4) = SUBSTRING(@wkbOriginal, 1, 4);

                -- Remaining bytes after SRID (starting from byte 9)
                DECLARE @geometryData VARBINARY(MAX) = SUBSTRING(@wkbOriginal, 9, LEN(@wkbOriginal) - 8);

                -- Combine prefix and geometry data without SRID
                SET @wkbNoSRID = @prefix + @geometryData;
                --select @wkbNoSRID
                -- Convert to geography using STGeomFromWKB with the desired SRID (e.g., 4326)
                SET @geography = geography::STGeomFromWKB(@wkbNoSRID, 4326); -- Replace 4326 with your actual SRID if different

                -- Insert the id and geography value into the target table
            UPDATE {self.table_name}
            SET {geo_column} = @geography
            WHERE ID = @id;

                -- Fetch the next row
                FETCH NEXT FROM cur INTO @id, @spatialpoint;
            END;

            CLOSE cur;
            DEALLOCATE cur;
        """

        try:
            self.logger.info(f"{conversion_query}")
            self.mssql_client.execute_update(conversion_query)
            self.logger.info(
                f"Successfully converted WKB to geography in '{self.table_name}' using ID matching."
            )

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=conversion_query,
                core_center=self.table_name.split("_")[1],
            )
            self.logger.error(f"Error converting WKB to geography: {exc}")
            raise

    def load_process_log(
        self,
        current_mssql_job_batch_id: int,
        current_sf_table_batch_id: int,
        process_name: str,
        table_name: str,
        row_count: int = 0,
    ) -> None:
        process_log_table_name = f"JC_MSSQL_{self.core_center.upper()}_PROCESS_LOG"

        query = f"""

        INSERT INTO {process_log_table_name} (
            MSSQL_BATCH_ID,
            SF_BATCH_ID,
            TABLE_NAME,
            PROCESS_NAME,
            ROW_COUNT
        )
        VALUES (
            {current_mssql_job_batch_id},
            {current_sf_table_batch_id},
            '{table_name}',
            '{process_name}',
            {row_count}
        )

        """
        # TODO: Duplicated code fragment. Consider making a function.
        try:
            # Execute the query
            self.mssql_client.execute_update(query)

        except Exception as exc:
            # Log and handle exceptions
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=self.core_center,
            )
            error_desc = f"Error Loading {process_log_table_name}"
            self.logger.error(f"{error_desc}: {exc}")
            raise Exception(error_desc) from exc

    def wkb_to_geography(
        self,
        geo_df: DataFrame,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str | int],
    ) -> None:
        geo_column = [column for column in geo_df.columns if column != "ID"]

        # Check if exactly one geography column was identified
        if len(geo_column) != 1:
            raise Exception(
                f"Expected one geography column, but found {len(geo_column)}: {geo_column}"
            )

        # Rename the identified column to 'GEO_COLUMN'
        geo_df = geo_df.withColumnRenamed(geo_column[0], "GEO_COLUMN")

        # Add TABLE_NAME column with a constant value for table_name
        geo_df = geo_df.withColumn("TABLE_NAME", lit(self.table_name))

        stg_table_name = f"JC_MSSQL_{self.core_center.upper()}_STG"

        # Write geography-related data to the staging table
        geo_df.write.jdbc(
            url=ms_sql_jdbc_url,
            table=stg_table_name,
            mode="append",
            properties=ms_sql_jdbc_properties,
        )
        inserted_count = geo_df.count()

        self.load_process_log(
            current_mssql_job_batch_id=self.current_mssql_job_batch_id,
            current_sf_table_batch_id=self.current_sf_table_batch_id,
            process_name=f"Inserted Incremental records into Stage table",
            table_name=self.table_name,
            row_count=inserted_count,
        )

        self.logger.info(
            f"Inserted {inserted_count} records having geography data into staging table '{stg_table_name}' for table {self.table_name}"
        )

        # Call the conversion function to update the main table's geography column
        self.convert_wkb_to_geography(
            geo_column=geo_column[0],
            stg_table_name=stg_table_name,
        )

        self.load_process_log(
            current_mssql_job_batch_id=self.current_mssql_job_batch_id,
            current_sf_table_batch_id=self.current_sf_table_batch_id,
            process_name=f"Inserted Incremental records from Stage table to table",
            table_name=self.table_name,
            row_count=inserted_count,
        )

        self.logger.info(
            f"Successfully converted WKB to geography & inserted records into main table '{self.table_name}'"
        )

    def process_and_load_incremental_records(
        self,
        df_incremental: DataFrame,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str | int],
        spark: SparkSession,
    ) -> None:
        try:
            df_incremental, df_incremental_stage = self.adjust_data_types(
                ms_sql_jdbc_url=ms_sql_jdbc_url,
                ms_sql_jdbc_properties=ms_sql_jdbc_properties,
                df=df_incremental,
                spark=spark,
            )
        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql="N/A",
                core_center=self.core_center,
            )
            self.logger.error(
                f"Error in schema validation or data type adjustment for table '{self.table_name}': {exc}"
            )
            raise

        # Drop duplicate rows based on PublicID if the column exists
        if "PublicID" in df_incremental.columns:
            self.logger.info(
                f"Dropping duplicate rows based on PublicID for table {self.table_name}"
            )
            df_incremental = df_incremental.dropDuplicates(["PublicID"])

        self.logger.info(
            f"Inserting records into MS SQL for : {self.table_name} table has started"
        )

        df_incremental.write.jdbc(
            url=ms_sql_jdbc_url,
            table=self.table_name,
            mode="append",
            properties=ms_sql_jdbc_properties,
        )

        inserted_count = df_incremental.count()

        self.load_process_log(
            current_mssql_job_batch_id=self.current_mssql_job_batch_id,
            current_sf_table_batch_id=self.current_sf_table_batch_id,
            process_name=f"Inserted Incremental records to the table",
            table_name=self.table_name,
            row_count=inserted_count,
        )

        self.logger.info(
            f"Successfully Inserted Incremental records into MS SQL for : {self.table_name} table"
        )

        if not df_incremental_stage:
            return

        self.logger.info(
            f"Processing table: {self.table_name} having geography data type columns"
        )
        try:
            self.wkb_to_geography(
                geo_df=df_incremental_stage,
                ms_sql_jdbc_url=ms_sql_jdbc_url,
                ms_sql_jdbc_properties=ms_sql_jdbc_properties,
            )
        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql="",
                core_center=self.table_name.split("_")[1],
            )
            self.logger.error(
                f"Error inserting data into staging table for '{self.table_name}' or "
                f"converting WKB to geography for table '{self.table_name}': {exc}"
            )
            raise Exception(
                f"Failed to process WKB to geography conversion for table '{self.table_name}'"
            ) from exc

    def fetch_ms_sql_inserted_count(self) -> int:
        query = f"""
                SELECT
                    COUNT(*) AS count
                FROM {self.table_name}
                WHERE BATCHID > (
                        SELECT
                            COALESCE(MAX(SF_BATCH_ID), 0)
                        FROM JC_MSSQL_{self.core_center.upper()}_LOAD_STATUS
                        WHERE table_name = '{self.table_name}'
                            AND status = 'completed'
                    );
            """
        try:
            with self.mssql_client.execute_query(query) as result_set:
                total_records_count_from_sf = 0
                if result_set.next():
                    total_records_count_from_sf = result_set.getInt("count")

                self.logger.info(
                    f"Successfully Retrieved  record count for latest insert from '{self.table_name}': "
                    f"{total_records_count_from_sf}"
                )
                return total_records_count_from_sf

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=self.core_center,
            )
            error_desc = f"Error fetching record count from table '{self.table_name}'"
            self.logger.exception(error_desc)
            raise Exception(error_desc) from exc

    def load_audit_validation(
        self,
        df_incremental_rec_count: int,
        df_soft_delete_rec_count: int,
        inserted_incremental_rec_count: int,
        reconciliation_status: str,
    ) -> None:
        validation_table_name = (
            f"JC_MSSQL_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION"
        )

        insert_query = f"""
            INSERT INTO {validation_table_name} (
                TABLE_NAME,
                MSSQL_BATCH_ID,
                SF_BATCH_ID,
                SF_INCREMENTAL_REC_COUNT,
                SF_SOFT_DELETE_REC_COUNT,
                DF_INCREMENTAL_REC_COUNT,
                DF_SOFT_DELETE_REC_COUNT,
                INSERTED_INCREMENTAL_REC_COUNT,
                DELETED_INCREMENTAL_REC_COUNT,
                DELETED_SOFT_DELETE_REC_COUNT,
                DIFF_SF_VS_DF_INCREMENTAL,
                DIFF_DF_VS_MSSQL_INCREMENTAL,
                DIFF_SF_VS_DF_SOFT_DELETE,
                DIFF_DF_VS_MSSQL_SOFT_DELETE,
                RECONCILIATION_STATUS,
                ROW_INSERT_TIMESTAMP
            ) VALUES (
                '{self.table_name}',
                {self.current_mssql_job_batch_id},
                {self.current_sf_table_batch_id},
                {self.sf_incremental_record_count},
                {self.sf_soft_delete_record_count},
                {df_incremental_rec_count},
                {df_soft_delete_rec_count},
                {inserted_incremental_rec_count},
                {self.deleted_incremental_record_count},
                {self.deleted_soft_delete_record_count},
                {self.sf_incremental_record_count - df_incremental_rec_count},
                {df_incremental_rec_count - inserted_incremental_rec_count},
                {self.sf_soft_delete_record_count - df_soft_delete_rec_count},
                {df_soft_delete_rec_count - self.deleted_soft_delete_record_count},
                '{reconciliation_status}',
                CURRENT_TIMESTAMP
            );
        """

        # TODO: Duplicated code fragment. Consider making a function.
        try:
            # Execute the query
            self.mssql_client.execute_update(insert_query)
            self.logger.info(
                f"Loaded {validation_table_name} table for '{self.table_name}'."
            )

        except Exception as exc:
            # Log and handle errors
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=insert_query,
                core_center=self.core_center,
            )
            error_desc = f"Failed to load {validation_table_name}"
            self.logger.error(f"{error_desc}: {exc}")
            raise Exception(error_desc) from exc

    def validate_and_reconcile_data(
        self,
        deleted_incremental_count: int,
        deleted_soft_delete_count: int,
    ) -> None:
        # Fetch inserted incremental record count from on-prem database
        inserted_incremental_rec_count = self.fetch_ms_sql_inserted_count()

        # Compare record counts for discrepancies
        sf_vs_df_incremental_rec_count = (
            self.sf_incremental_record_count != deleted_incremental_count
        )
        df_vs_ms_sql_incremental_rec_count = (
            deleted_incremental_count != inserted_incremental_rec_count
        )
        sf_vs_df_soft_delete_record_count = (
            self.sf_soft_delete_record_count != deleted_soft_delete_count
        )

        # Determine reconciliation status and log errors if discrepancies exist
        reconciliation_status = "Passed"
        if (
            sf_vs_df_incremental_rec_count
            or df_vs_ms_sql_incremental_rec_count
            or sf_vs_df_soft_delete_record_count
        ):
            reconciliation_status = "Failed"
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg="Record Count Mismatch",
                error_sql="",
                core_center=self.core_center,
            )

        # Log audit validation metrics
        self.load_audit_validation(
            df_incremental_rec_count=deleted_incremental_count,
            df_soft_delete_rec_count=deleted_soft_delete_count,
            inserted_incremental_rec_count=inserted_incremental_rec_count,
            reconciliation_status=reconciliation_status,
        )

    def load_ms_sql_table(
        self,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str | int],
        spark: SparkSession,
        sf_ms_sql_data_sync_job: bool,
    ) -> Dict[str, Any]:
        try:
            self.logger.info(
                f"Loading data from S3 to On-prem for table: {self.table_name}"
            )

            # Load and validate the data
            df = self.load_and_validate_data(spark=spark)

            # Skip actual writes and deletions if sf_ms_sql_data_sync_job is True
            if sf_ms_sql_data_sync_job:
                self.logger.info(
                    f"Skipping writes and deletions for table: {self.table_name} as sf_ms_sql_data_sync_job is True"
                )

                # Update only control tables
                self.merge_jc_ms_sql_load_status(
                    current_sf_table_batch_id=self.current_sf_table_batch_id,
                    status="completed",
                )

                # Log the skipped process
                self.load_process_log(
                    current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                    current_sf_table_batch_id=self.current_sf_table_batch_id,
                    process_name="Skipped Loading Table as sf_ms_sql_data_sync_job is set to TRUE",
                    table_name=self.table_name,
                    row_count=0,
                )

                return {"table_name": self.table_name, "status": "loaded"}

            self.merge_jc_ms_sql_load_status(
                current_sf_table_batch_id=self.current_sf_table_batch_id,
                status="started",
            )
            # Process records for incremental and soft delete
            (
                df_del_incremental_ids,
                df_del_soft_delete_ids,
                df_incremental,
            ) = self.process_records_for_deletion(df=df)

            # Delete records in MS SQL server to maintain the Type 1 SCD
            # deleted_incremental_count = self.delete_ms_sql_records(
            #     df_del_ids=df_del_incremental_ids,
            #     delete_type = "incremental_delete"
            # )

            self.mssql_client.execute_update(f"TRUNCATE TABLE {self.table_name}")
            time.sleep(30)

            # Adjust and write incremental records to On-prem
            self.process_and_load_incremental_records(
                df_incremental=df_incremental,
                ms_sql_jdbc_url=ms_sql_jdbc_url,
                ms_sql_jdbc_properties=ms_sql_jdbc_properties,
                spark=spark,
            )

            # Hard Delete records that are marked "Y" for GW_DELETE_FLAG Column
            # deleted_soft_delete_count = self.delete_ms_sql_records(
            #     df_del_ids=df_del_soft_delete_ids,
            #     delete_type = "soft_delete"
            # )

            # Perform validation and reconciliation
            # self.validate_and_reconcile_data(
            #     deleted_incremental_count=deleted_incremental_count,
            #     deleted_soft_delete_count=deleted_soft_delete_count,
            # )

            # Update load status to "completed"
            self.merge_jc_ms_sql_load_status(
                current_sf_table_batch_id=self.current_sf_table_batch_id,
                status="completed",
            )

        except Exception as exc:
            self.logger.error(f"Error loading table {self.table_name}: {exc}")
            self.merge_jc_ms_sql_load_status(
                current_sf_table_batch_id=self.current_sf_table_batch_id,
                status="failed",
            )
            self.mssql_client.write_to_error_log(
                table_name=self.table_name,
                sf_batch_id=self.current_sf_table_batch_id,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql="N/A",
                core_center=self.core_center,
            )
            raise


@dataclasses.dataclass
class Handler:
    logger: structlog.BoundLogger
    spark: SparkSession
    snowflake_connection: SnowflakeConnection
    max_spark_workers: int
    max_sf_workers: int
    current_mssql_job_batch_id: int
    mssql_client: MsSqlClient

    def fetch_last_successful_sf_table_batch_id(
        self,
        table_name: str,
        core_center: str,
    ) -> int:
        load_status_table_name = f"JC_MSSQL_{core_center.upper()}_LOAD_STATUS"
        query = f"""
                SELECT ISNULL(MAX(SF_BATCH_ID), 0) AS last_successful_sf_table_batch_id
                FROM {load_status_table_name}
                WHERE STATUS = 'completed'
                AND TABLE_NAME = '{table_name}'
            """
        try:
            with self.mssql_client.execute_query(query) as result_set:
                if result_set.next():
                    last_successful_sf_table_batch_id = result_set.getInt(
                        "last_successful_sf_table_batch_id"
                    )
                else:
                    last_successful_sf_table_batch_id = 0

                self.logger.info(
                    f"Latest BATCH_ID extracted from MS SQL {load_status_table_name} for table '{table_name}' is {last_successful_sf_table_batch_id}"
                )
                return last_successful_sf_table_batch_id

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=table_name,
                sf_batch_id=0,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=core_center,
            )
            error_desc = f"Error fetching latest BATCH_ID extracted from MS SQL {load_status_table_name} for table '{table_name}'"
            self.logger.error(f"{error_desc}: {exc}")
            raise Exception(error_desc) from exc

    def get_extracts_stage_s3_bucket_name(self) -> str:
        # Execute the query
        cursor = self.snowflake_connection.cursor()
        cursor.execute("DESCRIBE STAGE ETL_CTRL.EXTRACTS;")
        # noinspection SqlResolve
        cursor.execute(
            """
            SELECT "property_value"
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "parent_property" = 'STAGE_LOCATION';
            """
        )
        (stage_locations_str,) = cursor.fetchone()
        stage_locations = json.loads(stage_locations_str)
        if len(stage_locations) != 1:
            raise Exception(
                f"Multiple stage locations reported for ETL_CTRL.EXTRACTS stage. Only one expected: {stage_locations}"
            )
        stage_location = stage_locations[0]
        if not stage_location.startswith("s3://"):
            raise Exception(f"S3 Bucket expected: {stage_location}")
        # Strip the s3:// to return only the bucket name
        return stage_location[5:]

    def fetch_sf_tables(
        self,
        extracts_stage_s3_bucket_name: str,
        core_center: str,
        table_suffix: str,
    ) -> List[Table]:
        # Updated query to fetch distinct table names and max batch ID
        query = f"""
                SELECT CTL.TABLE_NAME,
                       MAX(LD_STS.BATCH_ID) AS LATEST_BATCH_ID
                FROM ETL_CTRL.JC_SF_TO_MSSQL_TABLES AS CTL
                         INNER JOIN ETL_CTRL.JC_MRG_{core_center.upper()}_LOAD_STATUS AS LD_STS
                              ON LOWER(CTL.TABLE_NAME) = LOWER(LD_STS.TABLE_NAME)
                WHERE LD_STS.STATUS = 'completed'
                GROUP BY CTL.TABLE_NAME
            """
        try:
            self.logger.info(f"Executing query to fetch tables from Snowflake")

            cursor = self.snowflake_connection.cursor()
            cursor.execute(query)

            tables: list[Table] = []
            for table_name, latest_batch_id in cursor.fetchall():
                last_successful_sf_table_batch_id = (
                    self.fetch_last_successful_sf_table_batch_id(
                        table_name=table_name, core_center=core_center
                    )
                )

                if latest_batch_id <= last_successful_sf_table_batch_id:
                    self.logger.info(
                        f"Skipping {table_name}: latest_batch_id <= last_successful_sf_table_batch_id.",
                        latest_batch_id=latest_batch_id,
                        last_successful_sf_table_batch_id=last_successful_sf_table_batch_id,
                    )
                    continue
                tables.append(
                    Table(
                        base_table_name=table_name,
                        table_suffix=table_suffix,
                        schema_name=f"MRG_{core_center.upper()}",
                        core_center=core_center,
                        current_sf_table_batch_id=latest_batch_id,
                        last_successful_sf_table_batch_id=last_successful_sf_table_batch_id,
                        current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                        extracts_stage_s3_bucket_name=extracts_stage_s3_bucket_name,
                        logger=self.logger,
                        mssql_client=self.mssql_client,
                    )
                )

            self.logger.info(f"Total tables to process: {len(tables)}")

            return tables

        except Exception as exc:
            # TODO: This batch_id = 0 is a good example of why we need an MSSQL-specific batch id.
            #  Every such error will be lumped into a pile of 0 batchid failures.
            self.mssql_client.write_to_error_log(
                table_name="NA",
                sf_batch_id=0,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=core_center,
            )
            raise Exception(f"Error fetching table lists") from exc

    def unload_sf_tables_to_s3(
        self, tables: List[Table]
    ) -> list[tuple[Table, Exception]]:
        failed_tables: list[tuple[Table, Exception]] = []

        # Multithreading to process tables
        with ThreadPoolExecutor(max_workers=self.max_sf_workers) as executor:
            futures = {
                executor.submit(
                    table.unload_sf_table_to_s3,
                    snowflake_connection=self.snowflake_connection,
                ): table
                for table in tables
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    failed_tables.append((table, exc))
        return failed_tables

    def insert_jc_ms_sql_batch_status(
        self,
        job_name: str,
        core_center: str,
    ) -> None:
        insert_query = f"""
                    INSERT INTO JC_MSSQL_{core_center.upper()}_BATCH_STATUS (
                        MSSQL_BATCH_ID,
                        JOB_NAME,
                        ROW_INSERT_TIMESTAMP,
                        ROW_UPDATE_TIMESTAMP,
                        STATUS
                    ) VALUES (
                        {self.current_mssql_job_batch_id},
                        '{job_name}',
                        GETDATE(),
                        GETDATE(),
                        'started'
                    )
                """
        self.mssql_client.execute_update(insert_query)

        self.logger.info(
            f"Inserted new batch status record with MSSQL Job BATCHID {self.current_mssql_job_batch_id} and STATUS as 'started' in JC_MSSQL_{core_center.upper()}_BATCH_STATUS table"
        )

    def update_ms_sql_batch_status(
        self,
        core_center: str,
        status: str,
    ) -> None:
        status_table_name = f"JC_MSSQL_{core_center.upper()}_BATCH_STATUS"

        status = status.lower()
        if status not in ["completed", "failed"]:
            error_desc = f"Invalid status '{status}' provided."
            self.logger.error(error_desc)
            raise ValueError(error_desc)

        query = f"""
            UPDATE {status_table_name}
            SET ROW_UPDATE_TIMESTAMP = GETDATE(),
                STATUS = '{status}'
            WHERE MSSQL_BATCH_ID = {self.current_mssql_job_batch_id};
        """

        try:
            self.mssql_client.execute_update(query)
            self.logger.info(
                f"Updated batch status for {self.current_mssql_job_batch_id} to STATUS '{status}' in {status_table_name}"
            )

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name="NA",
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=core_center,
            )
            self.logger.exception(f"Error updating {status_table_name}")
            raise

    def load_into_ms_sql(
        self,
        tables: List[Table],
        core_center: str,
        job_name: str,
        ms_sql_jdbc_url: str,
        ms_sql_jdbc_properties: dict[str, str | int],
        sf_ms_sql_data_sync_job: bool,
    ) -> list[tuple[Table, Exception]]:
        loaded_tables: list[Table] = []
        failed_tables: list[tuple[Table, Exception]] = []

        self.logger.info("Starting Phase 2: Loading into On-Prem Server...")

        # Update JC_MSSQL_BATCH_STATUS table as started
        self.insert_jc_ms_sql_batch_status(
            job_name=job_name,
            core_center=core_center,
        )

        with ThreadPoolExecutor(max_workers=self.max_spark_workers) as executor:
            futures = {
                executor.submit(
                    table.load_ms_sql_table,
                    ms_sql_jdbc_url=ms_sql_jdbc_url,
                    ms_sql_jdbc_properties=ms_sql_jdbc_properties,
                    spark=self.spark,
                    sf_ms_sql_data_sync_job=sf_ms_sql_data_sync_job,
                ): table
                for table in [
                    table for table in tables if table.s3_unload_record_count > 0
                ]
            }

            for future in as_completed(futures):
                table = futures[future]
                try:
                    result = future.result()
                    loaded_tables.append(result)
                except Exception as exc:
                    failed_tables.append((table, exc))

        # Log the number of successfully loaded tables and failed tables
        self.logger.info(f"Number of tables successfully loaded: {len(loaded_tables)}")
        return failed_tables

    def truncate_staging_table(self, stg_table_name: str) -> None:
        """
        Truncates the specified staging table in SQL Server.
        """
        query = f"TRUNCATE TABLE {stg_table_name}"
        try:
            self.mssql_client.execute_update(query)
            self.logger.info(f"Truncated staging table: {stg_table_name}")

        except Exception as exc:
            self.mssql_client.write_to_error_log(
                table_name=stg_table_name,
                sf_batch_id=0,
                current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                error_msg=str(exc),
                error_sql=query,
                core_center=stg_table_name.split("_")[1],
            )
            self.logger.error(f"Error truncating staging table {stg_table_name}: {exc}")
            raise Exception(
                f"Failed to truncate staging table {stg_table_name}"
            ) from exc

    def delete_parquet_files_after_load(
        self,
        s3_client: BaseClient,
        extracts_stage_s3_bucket_name: str,
        core_center: str,
    ) -> None:
        """
        Deletes all Parquet files under the specified S3 path:
        {s3_bucket.rstrip('/')}/sf_to_onprem_mrg_extract/{core_center}
        """
        s3_prefix = f"sf_to_onprem_mrg_extract/{core_center}"

        self.logger.info(
            f"Deleting all parquet extract files at: s3://{extracts_stage_s3_bucket_name}/{s3_prefix}/*"
        )

        try:
            # Initialize a paginator to handle large number of objects
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=extracts_stage_s3_bucket_name, Prefix=s3_prefix
            )

            objects_to_delete = []

            # Collect all object keys to delete
            for page in pages:
                if "Contents" in page:
                    objects_to_delete.extend(
                        [{"Key": obj["Key"]} for obj in page["Contents"]]
                    )

            if not objects_to_delete:
                self.logger.info(f"No files found for deletion under: {s3_prefix}")
                return

            self.logger.info(
                f"Total parquet objects to delete: {len(objects_to_delete)}"
            )

            # Delete objects in batches of 1000 (S3 limit per batch delete)
            chunk_size = 1000
            for i in range(0, len(objects_to_delete), chunk_size):
                delete_batch = objects_to_delete[i : i + chunk_size]
                retries = 3
                for attempt in range(retries):
                    try:
                        response = s3_client.delete_objects(
                            Bucket=extracts_stage_s3_bucket_name,
                            Delete={"Objects": delete_batch},
                        )
                        deleted = response.get("Deleted", [])
                        self.logger.info(
                            f"Deleted {len(deleted)} parquet objects in batch {i // chunk_size + 1}"
                        )
                        break  # Break out of retry loop on success
                    except ClientError as exc:
                        if attempt < retries - 1:
                            self.logger.warning(
                                f"Retry {attempt + 1} for batch {i // chunk_size + 1} due to error: {exc}"
                            )
                            time.sleep(2)  # Wait before retrying
                        else:
                            self.logger.error(
                                f"Failed to delete parquet objects in batch {i // chunk_size + 1} after {retries} retries"
                            )
                            raise  # Re-raise exception after final attempt

            self.logger.info(
                f"Successfully deleted all Parquet files under {extracts_stage_s3_bucket_name}"
            )

        except Exception:
            self.logger.exception(
                f"Error deleting Parquet files under {extracts_stage_s3_bucket_name}/sf_to_onprem_mrg_extract/{core_center}/"
            )
            # Depending on requirements, you may choose to raise an exception here
            raise


def main(
    logger: structlog.BoundLogger,
    job_name: str,
    core_center_db: str,
    snowflake_database: str,
    spark: SparkSession,
    max_sf_workers: int,
    max_spark_workers: int,
    core_center: str,
    max_s3_workers: int,
    aws_session: AWSSession,
    snowflake_secret_name: str,
    snowflake_token_endpoint: str,
    snowflake_role: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_warehouse: str,
    table_suffix: str,
    ms_sql_port: str,
    ms_sql_batch_load_limit: str,
    ms_sql_secret_name: str,
    sf_ms_sql_data_sync_job: bool,
) -> None:
    logger.info(f"Job : {job_name} starting")
    boto_config = Config(max_pool_connections=max_s3_workers)
    s3_client = aws_session.client("s3", config=boto_config)

    ms_sql_secret_string = get_secret(
        session=aws_session, secret_name=ms_sql_secret_name
    )

    ms_sql_jdbc_url = f"jdbc:sqlserver://{ms_sql_secret_string['server']}:{ms_sql_port};databaseName={core_center_db};{'' if IS_GLUE_JOB else 'encrypt=false;'}"
    ms_sql_jdbc_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": ms_sql_secret_string["username"],
        "password": ms_sql_secret_string["password"],
        "batchsize": ms_sql_batch_load_limit,
    }

    authorization_token = get_token(
        session=aws_session,
        secret_name=snowflake_secret_name,
        token_endpoint=snowflake_token_endpoint,
        role_name=snowflake_role,
    )

    snowflake_connection = get_snowflake_connection(
        logger=logger,
        snowflake_account=snowflake_account,
        snowflake_user=snowflake_user,
        snowflake_database=snowflake_database,
        snowflake_warehouse=snowflake_warehouse,
        snowflake_role=snowflake_role,
        authorization_token=authorization_token,
    )

    mssql_client = MsSqlClient(
        ms_sql_jdbc_url=ms_sql_jdbc_url,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
        logger=logger,
    )

    current_mssql_job_batch_id = get_next_batch_id(
        connection=snowflake_connection,
        logger=logger,
    )

    handler = Handler(
        mssql_client=mssql_client,
        logger=logger,
        spark=spark,
        snowflake_connection=snowflake_connection,
        max_spark_workers=max_spark_workers,
        max_sf_workers=max_sf_workers,
        current_mssql_job_batch_id=current_mssql_job_batch_id,
    )

    # Phase 1: Unloading from Snowflake
    extracts_stage_s3_bucket_name = handler.get_extracts_stage_s3_bucket_name()

    # 1.2 Fetch tables to process from Snowflake based on the last processed batch ID
    tables = handler.fetch_sf_tables(
        extracts_stage_s3_bucket_name=extracts_stage_s3_bucket_name,
        core_center=core_center,
        table_suffix=table_suffix,
    )

    # Check if there are any tables to process
    if not tables:
        handler.logger.info(
            f"Job : {job_name} Completed Successfully. No tables to process."
        )
        return

    handler.logger.info(
        "Starting Phase 1: Unloading records from Snowflake into Parquet Files in S3 Path"
    )

    # 1.3 Unload incremental records from snowflake
    failed_tables = handler.unload_sf_tables_to_s3(tables=tables)
    for table, exc in failed_tables:
        handler.mssql_client.write_to_error_log(
            table_name=table.table_name,
            sf_batch_id=table.current_sf_table_batch_id,
            current_mssql_job_batch_id=current_mssql_job_batch_id,
            error_msg=str(exc),
            error_sql="",
            core_center=core_center,
        )
        error_desc = f"Failed to unload tables: {[table.table_name for table, _ in failed_tables]}"
        handler.logger.error(error_desc)
        raise Exception(error_desc) from exc

    handler.logger.info("Phase 1 : Unloading from Snowflake is completed.")

    # Phase 2: Load

    # Truncating Staging table
    handler.truncate_staging_table(stg_table_name=f"JC_MSSQL_{core_center.upper()}_STG")

    # 2.1 Using Spark read and load the parquet files into respective tables in ms_sql
    failed_tables = handler.load_into_ms_sql(
        tables=tables,
        core_center=core_center,
        job_name=job_name,
        ms_sql_jdbc_url=ms_sql_jdbc_url,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
        sf_ms_sql_data_sync_job=sf_ms_sql_data_sync_job,
    )
    # Truncating Staging table
    handler.truncate_staging_table(stg_table_name=f"JC_MSSQL_{core_center.upper()}_STG")

    # 2.2 Delete the Parquet files post loading into MS SQL,
    handler.delete_parquet_files_after_load(
        s3_client=s3_client,
        extracts_stage_s3_bucket_name=extracts_stage_s3_bucket_name,
        core_center=core_center,
    )

    handler.logger.info(f"Number of tables failed to load: {len(failed_tables)}")
    for table, exc in failed_tables:
        handler.update_ms_sql_batch_status(
            core_center=core_center,
            status="failed",
        )
        handler.mssql_client.write_to_error_log(
            table_name=table.table_name,
            sf_batch_id=table.current_sf_table_batch_id,
            current_mssql_job_batch_id=current_mssql_job_batch_id,
            error_msg=str(exc),
            error_sql="",
            core_center=core_center,
        )
        error_desc = f"Failed to unload tables: {[table.table_name for table, exc in failed_tables]}"
        handler.logger.error(error_desc)
        raise Exception(error_desc) from exc

    handler.update_ms_sql_batch_status(
        core_center=core_center,
        status="completed",
    )

    handler.logger.info("Phase 2 completed: Loading into On-Prem Server.")


if __name__ == "__main__" and IS_GLUE_JOB:
    _args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
            "snowflake_secret_name",
            "snowflake_token_endpoint",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "snowflake_role",
            "sf_ms_sql_data_sync_job",
            "ms_sql_db_pc",
            "ms_sql_db_bc",
            "ms_sql_db_cc",
            "ms_sql_db_cm",
            "ms_sql_port",
            "ms_sql_secret_name",
            # Dynamic parameters
            "core_center",
            "ms_sql_batch_load_limit",
            "table_suffix",
        ],
    )
    _sc = SparkContext()
    _glue_context = GlueContext(_sc)
    _spark = _glue_context.spark_session
    _job = Job(_glue_context)
    _job.init(_args["JOB_NAME"] + _args["core_center"], _args)

    # noinspection PyProtectedMember,PyUnresolvedReferences
    _executor_cores = _sc._conf.get("spark.executor.cores")
    _max_spark_workers = int(_executor_cores) * 2 if _executor_cores else 4

    _structlogger = get_structlog_logger(_glue_context.get_logger())

    main(
        sf_ms_sql_data_sync_job=(
            True if _args["sf_ms_sql_data_sync_job"].upper() == "TRUE" else False
        ),
        logger=_structlogger,
        job_name=_args["JOB_NAME"],
        snowflake_database=_args["snowflake_database"],
        spark=_spark,
        max_sf_workers=20,
        max_spark_workers=_max_spark_workers,
        core_center=_args["core_center"],
        max_s3_workers=60,
        aws_session=AWSSession(region_name="us-east-1"),
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        table_suffix=_args["table_suffix"] if _args["table_suffix"] != "NONE" else None,
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_batch_load_limit=_args["ms_sql_batch_load_limit"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        core_center_db=_args[f"ms_sql_db_{_args['core_center']}"],
    )

    _job.commit()

elif __name__ == "__main__" and not IS_GLUE_JOB:
    # Hard-coded parameters
    _args = {
        "JOB_NAME": "Snowflake_to_mssql_merge_local",
        "snowflake_secret_name": "dev-aws-glue",
        "snowflake_token_endpoint": "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token",
        "snowflake_account": "encova-dev.privatelink",
        "snowflake_user": "0oa25sh1ca4BsKnGC0h8",
        "snowflake_database": "DEV1E2E",
        "snowflake_warehouse": "WH_GLUE",
        "snowflake_role": "DEV1E2E__GLUE__SVC_ROLE",
        "sf_ms_sql_data_sync_job": "False",
        "ms_sql_db_pc": "PolicyCenterR1",
        "ms_sql_db_bc": "BillingCenterR1",
        "ms_sql_db_cc": "ClaimsManagerR1",
        "ms_sql_db_cm": "ContactManagerR1",
        "ms_sql_port": 59796,
        "ms_sql_secret_name": "arn:aws:secretsmanager:us-east-1:058264312281:secret:aws-use1-dev-smr-ohdwdbs0024va-0001-mbDya0",
        # Dynamic parameters
        "core_center": "bc",
        "ms_sql_batch_load_limit": "10000",
        "table_suffix": "",
    }

    _max_spark_workers = 1

    _aws_profile_name = "dev"

    # Assume role session
    _aws_session = get_assumed_role_session(
        profile_name=_aws_profile_name,
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )

    # Replace this with your own structlog implementation if required
    _structlogger = get_structlog_logger(logger=_logger)

    # Initialize Spark and MS SQL connection
    _spark = get_local_spark_session(aws_session=_aws_session)

    main(
        sf_ms_sql_data_sync_job=(
            True if _args["sf_ms_sql_data_sync_job"].upper() == "TRUE" else False
        ),
        logger=_structlogger,
        job_name=_args["JOB_NAME"],
        snowflake_database=_args["snowflake_database"],
        spark=_spark,
        max_sf_workers=20,
        max_spark_workers=_max_spark_workers,
        core_center=_args["core_center"],
        max_s3_workers=10,
        aws_session=_aws_session,
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        table_suffix=_args["table_suffix"] if _args["table_suffix"] != "" else None,
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_batch_load_limit=_args["ms_sql_batch_load_limit"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        core_center_db=_args[f"ms_sql_db_{_args['core_center']}"],
    )
