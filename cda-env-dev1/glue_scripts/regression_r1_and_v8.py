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
            self.logger.error(error_desc)
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


@dataclasses.dataclass
class Handler:
    logger: structlog.BoundLogger
    spark: SparkSession
    snowflake_connection: SnowflakeConnection
    max_spark_workers: int
    max_sf_workers: int
    current_mssql_job_batch_id: int
    mssql_client_v8: MsSqlClient
    mssql_client_r1: MsSqlClient
    core_center_db: str
    core_center_db_v8: str
    ms_sql_jdbc_url_r1: str
    ms_sql_jdbc_url_v8: str
    ms_sql_jdbc_properties: dict[str, str | int]

    def fetch_sf_tables(
        self,
        core_center: str,
    ):
        # TODO: Refactor onprem to ms_sql
        # Updated query to fetch distinct table names and max batch ID
        query = f"""
                SELECT TABLE_NAME FROM ETL_CTRL.JC_SF_TO_MSSQL_TABLES where core_center ='{core_center.lower()}'
            """
        cursor = self.snowflake_connection.cursor()
        cursor.execute(query)
        onprem_tables = [row[0] for row in cursor.fetchall()]

        return onprem_tables

    def compare_data(
        self,
        table_name,
    ):
        self.logger.info(f"Record Validation for table: {table_name}")

        try:
            query = f"DECLARE @Result NVARCHAR(20); EXEC CompareSingleTable '{table_name}',{self.current_mssql_job_batch_id}, @Result OUTPUT"

            statement = self.mssql_client_r1.connection.createStatement()
            results = []
            has_more_results = statement.execute(query)
            rs_count = 0  # Count the number of result sets
            while has_more_results:  # Loop through multiple result sets
                result_set = statement.getResultSet()
                rs_count += 1

                # Extract column metadata
                metadata = result_set.getMetaData()
                column_count = metadata.getColumnCount()
                column_names = [
                    metadata.getColumnName(i + 1) for i in range(column_count)
                ]

                # Read the result set
                while result_set.next():
                    row_data = {
                        column_names[i]: result_set.getString(i + 1)
                        for i in range(column_count)
                    }

                    results.append(row_data)

                # Check if more results exist

                has_more_results = statement.getMoreResults()

            final_status = (
                results[-1]["Status"].strip() if "Status" in results[-1] else "Failed"
            )
            return (
                table_name,
                final_status,
            )
        except Exception as e:
            self.logger.info(e)
            return table_name, "Failed", str(e)

    def compare_counts(self, table_name):
        self.logger.info(f"Count Validation for table: {table_name}")

        V8_query = f"""
                SELECT COUNT(*) AS count from {self.core_center_db_v8}.dbo.{table_name}
            """

        with self.mssql_client_v8.execute_query(V8_query) as result_set:
            on_prem_record_count_v8 = 0
            if result_set.next():
                on_prem_record_count_v8 = result_set.getInt("count")
        R1_query = f"""
                SELECT COUNT(*) AS count from {self.core_center_db}.dbo.{table_name}
            """

        with self.mssql_client_r1.execute_query(R1_query) as result_set:
            on_prem_record_count_r1 = 0
            if result_set.next():
                on_prem_record_count_r1 = result_set.getInt("count")

        status = (
            "Counts Match"
            if on_prem_record_count_v8 == on_prem_record_count_r1
            else "Counts Differ"
        )
        return (table_name, on_prem_record_count_r1, on_prem_record_count_v8, status)

    # Function to store count comparison results in SQL Server
    def store_counts_in_sql(self, results):
        try:
            # Insert count comparison results into SQL table
            values_list = []
            for table_name, counts in results.items():
                values_list.append(
                    f"({self.current_mssql_job_batch_id},'{table_name}', {counts['DB_R1_Count']}, {counts['DB_V8_Count']}, '{counts['Status']}')"
                )

            if values_list:
                insert_query = f"""
                    INSERT INTO dbo.Regression_count_results (BATCHID,TableName, DB_R1_Count, DB_V8_Count, Status)
                    VALUES {", ".join(values_list)}
                """
                self.mssql_client_r1.execute_update(insert_query)

            self.logger.info(
                f"Successfully Inserted records into Regression count table"
            )

        except Exception as e:
            self.logger.error(f"⚠️ Error storing count results in SQL Server: {e}")

    def store_status_in_sql(
        self,
        results,
    ):
        try:
            # Construct a single INSERT query dynamically
            values_list = [
                f"({self.current_mssql_job_batch_id},'{table}','{result[1]}', '{result[2][:7997]}', '{result[3][:7997]}', '{result[4][:7997]}'  )"
                for table, result in results.items()
            ]

            if values_list:  # Insert only if there are records
                insert_query = f"""
                    INSERT INTO dbo.Regression_record_status_results (BatchID,TableName, Status,MISSING_ID_IN_R1,MISSING_ID_IN_V8,DATA_MISMATCH_ID )
                    VALUES {", ".join(values_list)}
                """
                self.mssql_client_r1.execute_update(insert_query)
            self.logger.info(
                f"Successfully Inserted records into Regression Record Status table"
            )

        except Exception as e:
            self.logger.error(f"⚠️ Error storing comparison status in SQL Server: {e}")


def main(
    logger: structlog.BoundLogger,
    job_name: str,
    core_center_db: str,
    core_center_db_v8: str,
    snowflake_database: str,
    spark: SparkSession,
    max_sf_workers: int,
    max_spark_workers: int,
    aws_session: AWSSession,
    snowflake_secret_name: str,
    snowflake_token_endpoint: str,
    snowflake_role: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_warehouse: str,
    ms_sql_port: str,
    ms_sql_batch_load_limit: str,
    ms_sql_secret_name: str,
    record_validation_flag: bool,
    core_center: str,
) -> None:
    logger.info(f"Job : {job_name} starting")

    ms_sql_secret_string = get_secret(
        session=aws_session, secret_name=ms_sql_secret_name
    )

    ms_sql_jdbc_url_v8 = f"jdbc:sqlserver://{ms_sql_secret_string['server']}:{ms_sql_port};databaseName={core_center_db_v8};{'' if IS_GLUE_JOB else 'encrypt=false;'}"
    ms_sql_jdbc_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": ms_sql_secret_string["username"],
        "password": ms_sql_secret_string["password"],
        "batchsize": ms_sql_batch_load_limit,
    }

    mssql_client_v8 = MsSqlClient(
        ms_sql_jdbc_url=ms_sql_jdbc_url_v8,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
        logger=logger,
    )

    ms_sql_jdbc_url_r1 = f"jdbc:sqlserver://{ms_sql_secret_string['server']}:{ms_sql_port};databaseName={core_center_db};{'' if IS_GLUE_JOB else 'encrypt=false;'}"
    mssql_client_r1 = MsSqlClient(
        ms_sql_jdbc_url=ms_sql_jdbc_url_r1,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
        logger=logger,
    )
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

    current_mssql_job_batch_id = get_next_batch_id(
        connection=snowflake_connection,
        logger=logger,
    )

    handler = Handler(
        mssql_client_v8=mssql_client_v8,
        mssql_client_r1=mssql_client_r1,
        core_center_db=core_center_db,
        core_center_db_v8=core_center_db_v8,
        logger=logger,
        spark=spark,
        snowflake_connection=snowflake_connection,
        max_spark_workers=max_spark_workers,
        max_sf_workers=max_sf_workers,
        current_mssql_job_batch_id=current_mssql_job_batch_id,
        ms_sql_jdbc_url_r1=ms_sql_jdbc_url_r1,
        ms_sql_jdbc_url_v8=ms_sql_jdbc_url_v8,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
    )

    # 1.2 Fetch tables to process from Snowflake based on the last processed batch ID
    onprem_tables = handler.fetch_sf_tables(core_center)

    # Check if there are any tables to process
    if not onprem_tables:
        handler.logger.info(
            f"Job : {job_name} Completed Successfully. No tables to process."
        )
        return

    if record_validation_flag:
        handler.logger.info("Starting Record Validation for onprem tables")

        exceptions = []
        failed_tables = []
        with ThreadPoolExecutor(max_workers=max_spark_workers) as executor:
            futures = {
                executor.submit(handler.compare_data, table): table
                for table in onprem_tables
            }

            for future in as_completed(futures):
                table_name = futures[future]

                try:
                    result = future.result()  # Get result from compare_dataframes()

                    if result[1] == "Matching":
                        logger.info(
                            f"Record Validation completed for table: {table_name} with {result[1]} Status"
                        )
                    else:
                        logger.info(
                            f"Record validation completed for table: {table_name} with {result[1]} Status"
                        )
                        failed_tables.append(table_name)
                except Exception as exc:
                    logger.error(
                        f"Error during Record validation for table: {table_name} Error: {str(exc)}"
                    )

                    failed_tables.append(table_name)

        if failed_tables:
            logger.error(f"Failed to validate below list of tables")
            logger.error(", ".join(failed_tables))

    else:
        handler.logger.info(
            "Skipping record validation as record_validation_flag is False."
        )

        exceptions = []
        count_results = {}
        with ThreadPoolExecutor(max_workers=max_spark_workers) as executor:
            futures = {
                executor.submit(handler.compare_counts, table): table
                for table in onprem_tables
            }

            for future in as_completed(futures):
                # table_name = futures[future]
                try:
                    table_name, count1, count2, status = future.result()
                    count_results[table_name] = {
                        "DB_R1_Count": count1,
                        "DB_V8_Count": count2,
                        "Status": status,
                    }

                    future.result()
                    logger.info(f"Count Validation completed for table: {table_name}")
                except Exception as exc:
                    logger.error(
                        f"Error during Count validation for table: {table_name}"
                    )
                    exceptions.append((table_name, exc))

        handler.store_counts_in_sql(count_results)

        if exceptions:
            for table, exc in exceptions:
                logger.error(f"Exception: {table.table_name}")
            raise exceptions[0][1]


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
            "ms_sql_db_pc",
            "ms_sql_db_bc",
            "ms_sql_db_cc",
            "ms_sql_db_cm",
            "ms_sql_db_pc_v8",
            "ms_sql_db_bc_v8",
            "ms_sql_db_cc_v8",
            "ms_sql_db_cm_v8",
            "ms_sql_port",
            "ms_sql_secret_name",
            # Dynamic parameters
            "core_center",
            "ms_sql_batch_load_limit",
            "record_validation_flag",
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

    session = AWSSession(region_name="us-east-1")

    main(
        logger=_structlogger,
        job_name=_args["JOB_NAME"],
        core_center=_args["core_center"],
        core_center_db=_args[f"ms_sql_db_{_args['core_center']}"],
        core_center_db_v8=_args[f"ms_sql_db_{_args['core_center']}_v8"],
        snowflake_database=_args["snowflake_database"],
        spark=_spark,
        max_sf_workers=20,
        max_spark_workers=_max_spark_workers,
        aws_session=session,
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_batch_load_limit=_args["ms_sql_batch_load_limit"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        record_validation_flag=True
        if _args["record_validation_flag"].upper() == "TRUE"
        else False,
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
        "ms_sql_db_pc": "PolicyCenterR1",
        "ms_sql_db_bc": "BillingCenterR1",
        "ms_sql_db_cc": "ClaimCenterR1",
        "ms_sql_db_cm": "ContactManagerR1",
        "ms_sql_db_pc_v8": "PolicyCenterR1_ST",
        "ms_sql_db_bc_v8": "BillingCenterR1_ST",
        "ms_sql_db_cc_v8": "ClaimCenterR1_ST",
        "ms_sql_db_cm_v8": "ContactManagerR1_ST",
        "ms_sql_port": 59796,
        "ms_sql_secret_name": "arn:aws:secretsmanager:us-east-1:058264312281:secret:aws-use1-dev-smr-ohdwdbs0024va-0001-mbDya0",
        # Dynamic parameters
        "ms_sql_batch_load_limit": "10000",
        "record_validation_flag": "True",
        "core_center": "cm",
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
        logger=_structlogger,
        job_name=_args["JOB_NAME"],
        core_center=_args["core_center"],
        core_center_db=_args[f"ms_sql_db_{_args['core_center']}"],
        core_center_db_v8=_args[f"ms_sql_db_{_args['core_center']}_v8"],
        snowflake_database=_args["snowflake_database"],
        spark=_spark,
        max_sf_workers=20,
        max_spark_workers=_max_spark_workers,
        aws_session=_aws_session,
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_batch_load_limit=_args["ms_sql_batch_load_limit"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        record_validation_flag=True
        if _args["record_validation_flag"].upper() == "TRUE"
        else False,
    )
