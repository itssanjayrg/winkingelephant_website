from __future__ import annotations

import abc
import dataclasses
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from textwrap import dedent
import psycopg2
import json
import structlog
from psycopg2.extensions import connection as pg_connection
from botocore.client import BaseClient
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict
from pyspark.context import SparkContext

try:
    IS_GLUE_JOB = True
    # noinspection PyUnresolvedReferences
    from awsglue.transforms import *

    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions

    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job

    # noinspection PyUnresolvedReferences
    from awsglue import DynamicFrame
except ModuleNotFoundError:
    IS_GLUE_JOB = False

from boto3.session import Session as AWSSession
from snowflake.connector import SnowflakeConnection

from utilities.snowflake_utils import get_snowflake_connection
from utilities.oauth_utils import get_assumed_role_session, get_token
from utilities.logging_utils import get_structlog_logger
from utilities.SNSClient import SNSClient

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s : %(filename)s - %(funcName)s : %(lineno)d : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)

# Reduce noise from certain libraries
for noisy_logger in [
    "snowflake",
    "boto3",
    "botocore",
    "urllib3",
    "concurrent.futures",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


@dataclasses.dataclass(kw_only=True, frozen=True)
class CoreCenterConfig:
    rds_db_name: str
    snowflake_schema: str


@dataclasses.dataclass(kw_only=True, frozen=True)
class TableStub:
    table_name: str
    has_updatetime_column: bool
    summed_column_name: str | None = None
    record_count_variance_threshold: int = 0
    sum_variance_threshold: float = 0


@dataclasses.dataclass
class RDSClient:
    """
    Wrapper around AWS RDS Aurora to run queries using psycopg2,
    returning results as normal Python lists of tuples.
    """

    database_name: str
    user: str
    rds_cluster_hostname: str
    rds_cluster_port: int
    client: pg_connection
    logger: structlog.BoundLogger

    @classmethod
    def factory(
        cls,
        session: AWSSession,
        secret_arn: str,
        logger: structlog.BoundLogger,
        database_name: str,
        rds_cluster_hostname: str,
        rds_cluster_port: int,
    ):
        # Retrieve credentials from Secrets Manager
        sm_client = session.client("secretsmanager")
        secret_response = sm_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(secret_response["SecretString"])

        # Connect via psycopg2
        conn = psycopg2.connect(
            database=database_name,
            user=secret["username"],
            password=secret["password"],
            host=rds_cluster_hostname,
            port=rds_cluster_port,
        )

        logger.info(
            f"RDS Aurora Connection established successfully with {database_name} database"
        )
        return cls(
            database_name=database_name,
            user=secret["username"],
            rds_cluster_hostname=rds_cluster_hostname,
            rds_cluster_port=rds_cluster_port,
            client=conn,
            logger=logger,
        )

    def run_query(self, query: str):
        """
        Run a query via psycopg2 and return all rows as list-of-tuples.
        """
        try:
            with self.client.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()  # list of tuples
            return rows
        except Exception as exc:
            error_message = f"Error executing RDS query {query}"
            self.logger.error(error_message)
            raise Exception(error_message) from exc


def insert_into_error_log(
    sf_connection: SnowflakeConnection,
    logger: structlog.BoundLogger,
    error_message: str,
    core_center: str,
    recon_batchid: int | None = None,
) -> None:
    if len(str(error_message)) > 8000:
        error_message = str(error_message)[:7997] + "..."

    try:
        query = """
            INSERT INTO ETL_CTRL.JC_RECON_ERROR_LOG (
                RECON_BATCHID,
                CORE_CENTER,
                ERROR_MESSAGE,
                ROW_INSERT_TIMESTAMP
            )
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
        """

        # Execute the query
        with sf_connection.cursor() as cursor:
            cursor.execute(query, (recon_batchid, core_center, error_message))
        logger.info(
            f"Error logged successfully in JC_RECON_ERROR_LOG for batch ID: {recon_batchid}"
        )

    except Exception:
        logger.error("Failed to log error in JC_RECON_ERROR_LOG")
        raise


def get_next_batch_id(
    sf_connection: SnowflakeConnection, logger: structlog.BoundLogger
) -> int:
    """
    Retrieve the next value from ETL_CTRL.SEQ_RECON_BATCHID.
    """
    logger.info(
        "Fetching next batch_id for the current Job from ETL_CTRL.SEQ_RECON_BATCHID."
    )
    try:
        with sf_connection.cursor() as cursor:
            cursor.execute("SELECT ETL_CTRL.SEQ_RECON_BATCHID.NEXTVAL AS BATCHID;")
            batch_id = cursor.fetchone()[0]
            logger.info(
                f"Batchid for reconciliation for the current batch is {batch_id}"
            )
            return batch_id
    except Exception as exc:
        error_message = "Error fetching batch_id for the job from Snowflake Sequence 'SEQ_RECON_BATCHID'"
        logger.error(error_message)
        insert_into_error_log(
            sf_connection=sf_connection,
            error_message=f"{error_message}: {str(exc)}",
            logger=logger,
            core_center="N/A",
        )
        raise Exception(error_message) from exc


@dataclasses.dataclass(kw_only=True)
class Table(abc.ABC):
    recon_batchid: int
    recon_batch_date: datetime.date
    table_name: str
    sf_schema: str
    rds_schema: str
    logger: structlog.BoundLogger
    core_center: str
    updatetime_column_present: bool | None = None
    record_count_variance_threshold: int = 0
    ids_missing_from_sf: list[int] = dataclasses.field(default_factory=list)
    ids_missing_from_rds: list[int] = dataclasses.field(default_factory=list)
    rds_record_count: int = -1
    sf_record_count: int = -1
    rds_less_sf_record_count: int | None = None
    recon_process_successful: bool = True
    recon_result: bool = False
    amount_recon_result: bool = False
    summed_column_name: str | None = None
    sum_variance_threshold: float = 0
    rds_sum: float | None = None
    sf_sum: float | None = None
    sum_difference: float | None = None
    table_in_EDW: str = False

    @classmethod
    def factory(
        cls,
        table_name: str,
        logger: structlog.BoundLogger,
        has_updatetime_column: bool,
        summed_column_name: str | None,
        **kwargs,
    ) -> (
        TableSansUpdatetime
        | TableWithUpdatetime
        | SumAuditTableWithUpdatetime
        | SumAuditTableSansUpdatetime
    ):
        bound_logger = logger.bind(table_name=table_name)
        if has_updatetime_column:
            if summed_column_name:
                return SumAuditTableWithUpdatetime(
                    table_name=table_name,
                    summed_column_name=summed_column_name,
                    logger=bound_logger,
                    **kwargs,
                )
            return TableWithUpdatetime(
                table_name=table_name,
                summed_column_name=summed_column_name,
                logger=bound_logger,
                **kwargs,
            )
        if summed_column_name:
            return SumAuditTableSansUpdatetime(
                table_name=table_name,
                summed_column_name=summed_column_name,
                logger=bound_logger,
                **kwargs,
            )
        return TableSansUpdatetime(
            table_name=table_name,
            summed_column_name=summed_column_name,
            logger=bound_logger,
            **kwargs,
        )

    @property
    @abc.abstractmethod
    def rds_record_count_query(self) -> str:
        pass

    def get_rds_record_count(self, rds_connection: RDSClient) -> int:
        """
        Fetch total RDS count, plus updatetime-based count if column is present.
        """
        result = rds_connection.run_query(self.rds_record_count_query)
        return result[0][0]

    @property
    @abc.abstractmethod
    def rds_min_id_max_id_query(self) -> str:
        pass

    def get_rds_min_id_max_id(
        self, rds_connection: RDSClient
    ) -> tuple[int | None, int | None]:
        try:
            result = rds_connection.run_query(self.rds_min_id_max_id_query)
            if not result or len(result) == 0:
                error_message = f"No min/max id value for table {self.table_name}"
                self.logger.error(error_message)
                raise Exception(error_message)
            min_id, max_id = result[0]
            return min_id, max_id
        except Exception as exc:
            self.logger.error(f"Error parsing min/max values")
            raise exc

    @property
    @abc.abstractmethod
    def rds_id_batch_query(self) -> str:
        pass

    def get_rds_id_batch(
        self, rds_connection: RDSClient, start_id: int, end_id: int
    ) -> list[int]:
        result = rds_connection.run_query(
            self.rds_id_batch_query.format(start_id=start_id, end_id=end_id)
        )
        return [row[0] for row in result]

    def get_rds_all_ids(
        self,
        rds_connection: RDSClient,
        chunk_size: int = 50000,
    ) -> set[int]:
        min_id, max_id = self.get_rds_min_id_max_id(rds_connection=rds_connection)
        ids_set = set()
        if min_id is None or max_id is None:
            self.logger.info(
                f"No IDs found in RDS for table {self.table_name}, assuming empty set"
            )
            return ids_set
        start_id = min_id
        while start_id <= max_id:
            end_id = start_id + chunk_size - 1
            id_batch = self.get_rds_id_batch(
                rds_connection=rds_connection, start_id=start_id, end_id=end_id
            )
            ids_set.update(id_batch)
            start_id = end_id + 1
        return ids_set

    def set_rds_table_count(
        self, rds_connection: RDSClient, sf_connection: SnowflakeConnection
    ) -> None:
        try:
            record_count = self.get_rds_record_count(rds_connection=rds_connection)
            if record_count is None:
                raise ValueError("RDS count is NONE")
            self.rds_record_count = record_count
            self.logger.debug(
                "RDS record count set",
                rds_record_count=self.rds_record_count,
            )
        except Exception as exc:
            self.rds_record_count = -1
            error_message = f"Error fetching count from {self.table_name} table in RDS"
            self.logger.error(error_message)
            insert_into_error_log(
                sf_connection=sf_connection,
                logger=self.logger,
                error_message=f"{error_message}: {str(exc)}",
                recon_batchid=self.recon_batchid,
                core_center=self.core_center,
            )
            raise Exception(error_message) from exc

    @property
    @abc.abstractmethod
    def sf_record_count_query(self) -> str:
        pass

    def get_sf_record_count(self, sf_connection: SnowflakeConnection) -> int:
        """
        Fetch total Snowflake count (only rows with GW_DELETE_FLAG='N'), plus
        updatetime-based count if the column is present.
        """

        with sf_connection.cursor() as cur:
            cur.execute(self.sf_record_count_query)
            return cur.fetchone()[0]

    @property
    @abc.abstractmethod
    def sf_ids_query(self) -> str:
        pass

    def get_sf_ids(self, sf_connection: SnowflakeConnection) -> set[int]:
        with sf_connection.cursor() as cursor:
            result = cursor.execute(self.sf_ids_query).fetchall()
            return {row[0] for row in result}

    def set_sf_table_count(self, sf_connection: SnowflakeConnection) -> None:
        """
        Fetch total Snowflake count (only rows with GW_DELETE_FLAG='N'), plus
        updatetime-based count if the column is present.
        """
        try:
            record_count = self.get_sf_record_count(sf_connection=sf_connection)
            if record_count is None:
                raise ValueError("Snowflake record count is NONE")
            self.sf_record_count = record_count
            self.logger.debug(
                f"Snowflake record count set",
                sf_record_count=self.sf_record_count,
            )
        except Exception as exc:
            self.sf_record_count = -1
            error_message = (
                f"Error fetching count from {self.table_name} table in Snowflake"
            )
            self.logger.error(error_message)
            insert_into_error_log(
                sf_connection=sf_connection,
                logger=self.logger,
                error_message=f"{error_message}: {str(exc)}",
                recon_batchid=self.recon_batchid,
                core_center=self.core_center,
            )
            raise Exception(error_message) from exc

    def set_record_count_and_missing_ids(
        self,
        rds_connection: RDSClient,
        sf_connection: SnowflakeConnection,
    ):
        self.set_rds_table_count(
            rds_connection=rds_connection,
            sf_connection=sf_connection,
        )
        self.set_sf_table_count(sf_connection=sf_connection)

        self.recon_result = True
        if self.rds_record_count == -1 or self.sf_record_count == -1:
            self.recon_result = False
            self.recon_process_successful = False
        else:
            self.rds_less_sf_record_count = self.rds_record_count - self.sf_record_count
            self.recon_result = (
                abs(self.rds_less_sf_record_count)
                <= self.record_count_variance_threshold
            )
        self.logger.debug(
            "Recon count complete",
            rds_less_sf_record_count=self.rds_less_sf_record_count,
            record_count_variance_threshold=self.record_count_variance_threshold,
            recon_result=self.recon_result,
        )
        if self.recon_result:
            self.logger.debug(
                "Recon count complete: Count variance within threshold.",
                rds_less_sf_record_count=self.rds_less_sf_record_count,
                record_count_variance_threshold=self.record_count_variance_threshold,
                recon_result=self.recon_result,
            )
            return
        self.logger.debug(
            "Recon count complete: Count variance exceeds threhold",
            rds_less_sf_record_count=self.rds_less_sf_record_count,
            record_count_variance_threshold=self.record_count_variance_threshold,
            recon_result=self.recon_result,
        )

        try:
            sf_ids = self.get_sf_ids(sf_connection=sf_connection)
            rds_ids = self.get_rds_all_ids(rds_connection=rds_connection)
            self.ids_missing_from_sf = list(set(rds_ids).difference(set(sf_ids)))
            self.ids_missing_from_rds = list(set(sf_ids).difference(set(rds_ids)))
        except Exception as exc:
            self.logger.error(
                f"Error fetching or comparing IDs for table {self.table_name}: {str(exc)}"
            )
            self.recon_process_successful = False
            self.ids_missing_from_sf = []
            self.ids_missing_from_rds = []
            insert_into_error_log(
                sf_connection=sf_connection,
                logger=self.logger,
                error_message=f"Error fetching or comparing IDs for table {self.table_name}: {str(exc)}",
                recon_batchid=self.recon_batchid,
                core_center=self.core_center,
            )

    @property
    def insert_recon_tracker_dict(self) -> dict:
        return {
            "RECON_BATCHID": self.recon_batchid,
            "RECON_BATCH_DATE": self.recon_batch_date,
            "SCHEMA_NAME": self.sf_schema,
            "TABLE_NAME": self.table_name,
            "TABLE_IN_EDW": self.table_in_EDW,
            "SUMMED_COLUMN_NAME": self.summed_column_name,
            "UPDATETIME_COLUMN_PRESENT": self.updatetime_column_present,
            "RDS_COUNT": self.rds_record_count,
            "SF_COUNT": self.sf_record_count,
            "RECON_RESULT": self.recon_result,
            "RECORD_COUNT_VARIANCE_THRESHOLD": self.record_count_variance_threshold,
            "RECON_PROCESS_SUCCESSFUL": self.recon_process_successful,
            "IDS_MISSING_FROM_SF": self.ids_missing_from_sf,
            "IDS_MISSING_FROM_RDS": self.ids_missing_from_rds,
            "RDS_SUM": self.rds_sum,
            "SF_SUM": self.sf_sum,
            "AMOUNT_RECON_RESULT": self.amount_recon_result,
            "SUM_VARIANCE_THRESHOLD": self.sum_variance_threshold,
        }


@dataclasses.dataclass(kw_only=True)
class TableSansUpdatetime(Table):
    updatetime_column_present: bool = False

    @property
    def rds_record_count_query(self) -> str:
        return f"""
            SELECT COUNT(*)
            FROM {self.rds_schema}.{self.table_name};
        """

    @property
    def rds_min_id_max_id_query(self) -> str:
        return f"""
            SELECT MIN(ID),
                MAX(ID)
            FROM {self.rds_schema}.{self.table_name};
        """

    @property
    def rds_id_batch_query(self) -> str:
        return f"""
            SELECT
                ID
            FROM {self.rds_schema}.{self.table_name}
            WHERE ID BETWEEN {{start_id}} AND {{end_id}};
        """

    @property
    def rds_sum_query(self) -> str:
        if self.summed_column_name is None:
            raise Exception("summed_column_name is undefined")
        return f"""
            SELECT SUM({self.summed_column_name}) AS TOTAL_AMOUNT
            FROM {self.rds_schema}.{self.table_name};
        """

    @property
    def sf_record_count_query(self) -> str:
        return f"""
            SELECT COUNT(*)
            FROM {self.sf_schema}.{self.table_name}
            WHERE GW_DELETE_FLAG='N';
        """

    @property
    def sf_ids_query(self) -> str:
        return f"""
            SELECT ID
            FROM {self.sf_schema}.{self.table_name}
            WHERE GW_DELETE_FLAG='N';
        """

    @property
    def sf_sum_query(self) -> str:
        if self.summed_column_name is None:
            raise Exception("summed_column_name is undefined")
        return f"""
            SELECT SUM({self.summed_column_name}) AS total_amount
            FROM {self.sf_schema}.{self.table_name}
            WHERE GW_DELETE_FLAG='N';
        """


@dataclasses.dataclass(kw_only=True)
class TableWithUpdatetime(Table):
    updatetime_column_present: bool = True

    @property
    def rds_record_count_query(self) -> str:
        return f"""
            SELECT COUNT(*)
            FROM {self.rds_schema}.{self.table_name}
            WHERE updatetime
                BETWEEN (TO_DATE('{self.recon_batch_date}', 'YYYY-MM-DD') - INTERVAL '1 day') + INTERVAL '18 hours'
                AND TO_TIMESTAMP('{self.recon_batch_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS');
        """

    @property
    def rds_min_id_max_id_query(self) -> str:
        return f"""
            SELECT MIN(ID),
                MAX(ID)
            FROM {self.rds_schema}.{self.table_name}
            WHERE updatetime
                BETWEEN (TO_DATE('{self.recon_batch_date}', 'YYYY-MM-DD') - INTERVAL '1 day') + INTERVAL '18 hours'
                AND TO_TIMESTAMP('{self.recon_batch_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS');
        """

    @property
    def rds_id_batch_query(self) -> str:
        return f"""
            SELECT
                ID
            FROM {self.rds_schema}.{self.table_name}
            WHERE (
                updatetime BETWEEN (TO_DATE('{self.recon_batch_date}', 'YYYY-MM-DD') - INTERVAL '1 day') + INTERVAL '18 hours'
                AND TO_TIMESTAMP('{self.recon_batch_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS'))
                AND (ID BETWEEN {{start_id}} AND {{end_id}});
        """

    @property
    def rds_sum_query(self) -> str:
        if self.summed_column_name is None:
            raise Exception("summed_column_name is undefined")
        return f"""
            SELECT SUM({self.summed_column_name}) AS TOTAL_AMOUNT
            FROM {self.rds_schema}.{self.table_name}
            WHERE updatetime <= TO_TIMESTAMP('{self.recon_batch_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS');
        """

    @property
    def sf_record_count_query(self) -> str:
        return f"""
            SELECT COUNT(*)
            FROM {self.sf_schema}.{self.table_name}
            WHERE
                (updatetime BETWEEN DATEADD(HOUR, 18, DATEADD(DAY, -1, '{self.recon_batch_date}'::DATE))
                    AND '{self.recon_batch_date} 18:00:00'::TIMESTAMP)
                AND GW_DELETE_FLAG = 'N';
        """

    @property
    def sf_ids_query(self) -> str:
        return f"""
            SELECT ID
            FROM {self.sf_schema}.{self.table_name}
            WHERE updatetime
                BETWEEN DATEADD(HOUR, 18, DATEADD(DAY, -1, '{self.recon_batch_date}'::DATE))
                    AND '{self.recon_batch_date} 18:00:00'::TIMESTAMP
                AND GW_DELETE_FLAG = 'N';
        """

    @property
    def sf_sum_query(self) -> str:
        if self.summed_column_name is None:
            raise Exception("summed_column_name is undefined")
        return f"""
            SELECT SUM({self.summed_column_name}) AS total_amount
            FROM {self.sf_schema}.{self.table_name}
            WHERE GW_DELETE_FLAG='N'
            AND updatetime <= '{self.recon_batch_date} 18:00:00'::TIMESTAMP;
        """


@dataclasses.dataclass(kw_only=True)
class SumAuditTable(Table, abc.ABC):
    summed_column_name: str
    rds_sum: float = -1
    sf_sum: float = -1
    sum_difference: float = -1

    @property
    @abc.abstractmethod
    def rds_sum_query(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def sf_sum_query(self) -> str:
        pass

    def set_rds_sum(self, rds_connection: RDSClient) -> None:
        rds_sum_result = rds_connection.run_query(self.rds_sum_query)
        if rds_sum_result and rds_sum_result[0][0] is not None:
            self.rds_sum = float(rds_sum_result[0][0])
            self.logger.info(
                f"RDS Sum for {self.summed_column_name}", rds_sum=self.rds_sum
            )
            return
        self.logger.warn(f"Unable to calculate RDS Sum for {self.summed_column_name}")

    def set_sf_sum(self, sf_connection: SnowflakeConnection) -> None:
        with sf_connection.cursor() as cursor:
            result = cursor.execute(self.sf_sum_query).fetchone()
            if result is not None and result[0] is not None:
                self.sf_sum = float(result[0])
                self.logger.info(
                    f"Snowflake Sum for {self.summed_column_name}", sf_sum=self.sf_sum
                )
                return
        self.logger.warn(
            f"Unable to calculate Snowflake Sum for {self.summed_column_name}"
        )

    def set_record_count_and_missing_ids(
        self,
        rds_connection: RDSClient,
        sf_connection: SnowflakeConnection,
    ):
        super().set_record_count_and_missing_ids(
            rds_connection=rds_connection, sf_connection=sf_connection
        )
        self.set_rds_sum(rds_connection=rds_connection)
        self.set_sf_sum(sf_connection=sf_connection)
        self.sum_difference = abs(self.rds_sum - self.sf_sum)
        sum_within_threshold = self.sum_difference <= self.sum_variance_threshold
        self.amount_recon_result = sum_within_threshold
        if self.sum_difference > self.sum_variance_threshold:
            self.logger.warn(
                f"Amount difference exceeds threshold for table {self.table_name} for column {self.summed_column_name}",
                rds_sum=self.rds_sum,
                sf_sum=self.sf_sum,
                sum_variance_threshold=self.sum_variance_threshold,
                sum_difference=self.sum_difference,
            )


class SumAuditTableWithUpdatetime(TableWithUpdatetime, SumAuditTable):
    pass


class SumAuditTableSansUpdatetime(TableSansUpdatetime, SumAuditTable):
    pass


@dataclasses.dataclass
class CoreCenterHandler:
    recon_batchid: int
    core_center: str
    logger: structlog.BoundLogger
    sns_client: SNSClient
    s3_client: BaseClient
    sf_connection: SnowflakeConnection
    sf_schema: str
    rds_connection: RDSClient
    rds_schema: str
    ignored_tables: Dict[str, IgnoreTable]

    def insert_into_error_log(self, error_message: str) -> None:
        insert_into_error_log(
            sf_connection=self.sf_connection,
            logger=self.logger,
            error_message=error_message,
            recon_batchid=self.recon_batchid,
            core_center=self.core_center,
        )

    def fetch_sf_batch_details(
        self,
    ) -> tuple[int, datetime.date]:
        self.logger.info(
            "Fetching latest batch details from Snowflake table: JC_BATCH_STATUS"
        )
        query = f"""
            SELECT BATCHID, DAY_ID
            FROM ETL_CTRL.JC_BATCH_STATUS
            WHERE BATCH_NAME IN ('Incremental load for {self.core_center.lower()}', 'Initial load for {self.core_center.lower()}')
            AND STATUS = 'COMPLETED'
            ORDER BY
                CASE
                    WHEN BATCH_NAME = 'Incremental load for {self.core_center.lower()}' THEN 1
                    WHEN BATCH_NAME = 'Initial load for {self.core_center.lower()}' THEN 2
                END,
                END_TIME DESC
            LIMIT 1;
        """
        try:
            with self.sf_connection.cursor() as cursor:
                result = cursor.execute(query).fetchone()
                if not result:
                    raise Exception(
                        f"Batch ID not found from ETL_CTRL.JC_BATCH_STATUS for recon batchid  '{self.recon_batchid}'"
                    )
                sf_batchid, sf_batch_date = result
                if not sf_batch_date:
                    raise Exception(
                        f"Could not find latest Snowflake batch date from ETL_CTRL.JC_BATCH_STATUS for recon batchid  '{self.recon_batchid}'."
                    )
                self.logger.info(
                    f"Latest Snowflake Batch ID: {sf_batchid}, Latest Snowflake Batch Date: {sf_batch_date}"
                )
                return sf_batchid, sf_batch_date
        except Exception as exc:
            error_message = f"Error fetching batch details from ETL_CTRL.JC_BATCH_STATUS for recon batchid  '{self.recon_batchid}'"
            self.logger.error(error_message)
            self.insert_into_error_log(error_message=f"{error_message}: {str(exc)}")
            raise Exception(error_message) from exc

    def update_jc_batch_status(
        self,
        status,
        sf_batch_id,
        sf_batch_date,
        recon_batch_date,
    ):
        query = dedent(
            f"""
            MERGE INTO ETL_CTRL.JC_RECON_BATCH_STATUS AS TARGET
            USING (
                SELECT
                    {self.recon_batchid}     AS RECON_BATCHID,
                    '{self.core_center}'     AS CORE_CENTER,
                    '{sf_batch_date}'::DATE    AS SF_BATCH_DATE,
                    '{recon_batch_date}'::DATE AS RECON_BATCH_DATE,
                    {sf_batch_id}       AS SF_BATCHID,
                    UPPER('{status}')   AS STATUS
            ) AS SOURCE
            ON TARGET.RECON_BATCHID = SOURCE.RECON_BATCHID
                AND TARGET.CORE_CENTER = SOURCE.CORE_CENTER
            WHEN MATCHED AND UPPER(SOURCE.STATUS) IN ('COMPLETED', 'FAILED') THEN
                UPDATE SET
                    TARGET.ROW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
                    TARGET.STATUS               = SOURCE.STATUS
            WHEN MATCHED AND UPPER(SOURCE.STATUS) NOT IN ('COMPLETED', 'FAILED') THEN
                UPDATE SET
                    TARGET.ROW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
                    TARGET.STATUS               = SOURCE.STATUS
            WHEN NOT MATCHED THEN
                INSERT (
                    RECON_BATCHID,
                    CORE_CENTER,
                    SF_BATCHID,
                    SF_BATCH_DATE,
                    RECON_BATCH_DATE,
                    ROW_INSERT_TIMESTAMP,
                    ROW_UPDATE_TIMESTAMP,
                    STATUS
                )
                VALUES (
                    SOURCE.RECON_BATCHID,
                    SOURCE.CORE_CENTER,
                    SOURCE.SF_BATCHID,
                    SOURCE.SF_BATCH_DATE,
                    SOURCE.RECON_BATCH_DATE,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP,
                    SOURCE.STATUS
                )
            """
        )
        try:
            with self.sf_connection.cursor() as cursor:
                cursor.execute(query)
            self.logger.info(
                f"Status '{status}' updated successfully for batch ID {self.recon_batchid} "
                f"in JC_RECON_BATCH_STATUS for {self.core_center}."
            )
        except Exception as exc:
            error_message = (
                "Error updating status in the table ETL_CTRL.JC_RECON_BATCH_STATUS"
            )
            self.logger.error(error_message)
            self.insert_into_error_log(error_message=f"{error_message}: {str(exc)}")
            raise Exception(error_message) from exc

    def fetch_rds_tables(self) -> dict[str, bool]:
        rds_tables_query = f"""
                SELECT DISTINCT LOWER(TABLE_NAME) AS TABLE_NAME,
                                BOOL_OR(UPPER(COLUMN_NAME) = 'UPDATETIME')
                                OVER (
                                    PARTITION BY
                                        TABLE_CATALOG,
                                        TABLE_SCHEMA,
                                        TABLE_NAME
                                    )             AS HAS_UPDATETIME_COLUMN
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = CURRENT_DATABASE()
                AND UPPER(TABLE_SCHEMA) = '{self.rds_schema.upper()}';
            """
        rds_resp = self.rds_connection.run_query(rds_tables_query)
        rds_tables = {
            table_name: has_updatetime_column
            for (table_name, has_updatetime_column) in rds_resp
        }
        rds_table_count = len(rds_tables)
        self.logger.info(
            f"Number of tables found in RDS for Core Center '{self.core_center}' are: {rds_table_count}"
        )
        self.insert_into_jc_recon_process_log(
            process_name="Number of Tables present in RDS Database",
            count=rds_table_count,
        )
        return rds_tables

    def fetch_snowflake_tables(self) -> dict[str, TableStub]:
        with self.sf_connection.cursor() as cur:
            result = cur.execute(
                f"""
                WITH CTE AS (
                    SELECT DISTINCT LOWER(TABLE_NAME) AS TABLE_NAME,
                                    BOOLOR_AGG(UPPER(COLUMN_NAME) = 'UPDATETIME')
                                            OVER (
                                                PARTITION BY
                                                    TABLE_CATALOG,
                                                    TABLE_SCHEMA,
                                                    TABLE_NAME
                                                )  AS HAS_UPDATETIME_COLUMN
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = CURRENT_DATABASE()
                    AND LOWER(TABLE_SCHEMA) = '{self.sf_schema.lower()}'
                )
                SELECT CTE.TABLE_NAME,
                    CTE.HAS_UPDATETIME_COLUMN,
                    CTRL.SUMMED_COLUMN_NAME,
                    NVL(CTRL.RECORD_COUNT_VARIANCE_THRESHOLD, 0) AS RECORD_COUNT_VARIANCE_THRESHOLD,
                    NVL(CTRL.SUM_VARIANCE_THRESHOLD, 0)          AS SUM_VARIANCE_THRESHOLD
                FROM CTE
                        LEFT JOIN ETL_CTRL.JC_RECON_CONTROL AS CTRL
                                ON CTE.TABLE_NAME = LOWER(CTRL.TABLE_NAME)
                                    AND LOWER(CTRL.CORE_CENTER) = '{self.core_center.lower()}';
                """
            ).fetchall()
            sf_tables = {
                table_name: TableStub(
                    table_name=table_name,
                    has_updatetime_column=has_updatetime_column,
                    summed_column_name=summed_column_name,
                    record_count_variance_threshold=record_count_variance_threshold,
                    sum_variance_threshold=sum_variance_threshold,
                )
                for (
                    table_name,
                    has_updatetime_column,
                    summed_column_name,
                    record_count_variance_threshold,
                    sum_variance_threshold,
                ) in result
            }
        sf_table_count = len(sf_tables.keys())
        self.logger.info(
            f"Number of tables found in Snowflake for Core Center '{self.core_center}' are: {sf_table_count}"
        )
        self.insert_into_jc_recon_process_log(
            process_name="Number of Tables present in Snowflake Database",
            count=sf_table_count,
        )
        return sf_tables

    def set_table_in_edw_flag(
        self,
        tables: list[TableSansUpdatetime | TableWithUpdatetime | SumAuditTable],
    ) -> None:
        query = f"""
            SELECT LOWER(table_name)
            FROM ETL_CTRL.JC_SF_TO_MSSQL_TABLES
            WHERE LOWER(core_center) = '{self.core_center.lower()}'
        """
        with self.sf_connection.cursor() as cursor:
            result = cursor.execute(query).fetchall()
        edw_table_names = {row[0] for row in result}
        for table in tables:
            table.table_in_EDW = (
                True if table.table_name.lower() in edw_table_names else False
            )

    def fetch_common_tables(
        self,
        recon_batch_date: datetime.date,
    ) -> list[TableSansUpdatetime | TableWithUpdatetime]:
        ignored_table_names = [
            table.name for table in self.ignored_tables.values() if table.ignore_table
        ]
        try:
            return_tables: list[TableSansUpdatetime | TableWithUpdatetime] = []
            sf_tables = self.fetch_snowflake_tables()
            rds_tables = self.fetch_rds_tables()
            sf_table_names = set(sf_tables.keys())
            rds_table_names = set(rds_tables.keys())
            common_table_names = rds_table_names.intersection(sf_table_names)
            for table_name in common_table_names:
                if table_name in ignored_table_names:
                    continue

                sf_table_stub = sf_tables[table_name]
                rds_has_updatetime_column = rds_tables[table_name]
                if sf_table_stub.has_updatetime_column != rds_has_updatetime_column:
                    raise Exception(
                        f"{table_name}: HAS_UPDATETIME_COLUMN discrepancy. "
                        f"Snowflake: {sf_table_stub.has_updatetime_column}. "
                        f"RDS: {rds_has_updatetime_column}"
                    )

                # noinspection PyTypeChecker
                return_tables.append(
                    Table.factory(
                        **dataclasses.asdict(sf_table_stub),
                        recon_batchid=self.recon_batchid,
                        recon_batch_date=recon_batch_date,
                        sf_schema=self.sf_schema,
                        rds_schema=self.rds_schema,
                        logger=self.logger,
                        core_center=self.core_center,
                    )
                )
            self.set_table_in_edw_flag(return_tables)
            common_table_count = len(return_tables)
            self.logger.info(
                f"Number of common tables between RDS and Snowflake are: {common_table_count}"
            )
            self.insert_into_jc_recon_process_log(
                process_name="Number of common tables between RDS and Snowflake",
                count=common_table_count,
            )
            return return_tables
        except Exception as exc:
            error_message = f"Error in fetching common tables for Recon Batchid {self.recon_batchid}"
            self.logger.error(error_message)
            self.insert_into_error_log(error_message=f"{error_message}: {str(exc)}")
            raise Exception(error_message) from exc

    def insert_into_jc_recon_process_log(self, process_name, count):
        query = """-- noinspection SqlResolveForFile
            INSERT INTO ETL_CTRL.JC_RECON_PROCESS_LOG (
                RECON_BATCHID,
                CORE_CENTER,
                PROCESS_NAME,
                COUNT,
                ROW_INSERT_TIMESTAMP
            )
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        try:
            with self.sf_connection.cursor() as cursor:
                cursor.execute(
                    query, (self.recon_batchid, self.core_center, process_name, count)
                )
                self.sf_connection.commit()
                self.logger.info(
                    f"Inserted record into JC_RECON_PROCESS_LOG: "
                    f"RECON_BATCHID={self.recon_batchid}, CORE_CENTER={self.core_center}, "
                    f"PROCESS_NAME={process_name}, COUNT={count}"
                )
        except Exception as e:
            self.logger.error(f"Error inserting record into JC_RECON_PROCESS_LOG: {e}")
            raise

    def parallel_set_record_count_and_missing_ids(
        self,
        tables: list[TableSansUpdatetime | TableWithUpdatetime],
        max_workers,
    ) -> list[Exception]:
        exceptions: list[Exception] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    table.set_record_count_and_missing_ids,
                    sf_connection=self.sf_connection,
                    rds_connection=self.rds_connection,
                ): table
                for table in tables
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    error_message = f"Error setting record count and missing ids"
                    table.logger.error(error_message)
                    self.insert_into_error_log(
                        error_message=f"{error_message}: {str(exc)}"
                    )
                    exceptions.append(exc)
        self.logger.info("Table Counts are set")
        return exceptions

    def insert_into_recon_tracker_table(
        self, tables: list[TableSansUpdatetime | TableWithUpdatetime]
    ):
        recon_count_tracker_table_name = f"JC_RECON_TRACKER_{self.core_center.upper()}"
        try:
            df = pd.DataFrame([table.insert_recon_tracker_dict for table in tables])
            self.logger.info(
                f"Inserting {len(df)} rows into {recon_count_tracker_table_name}"
            )
            success, _, n_rows, _ = write_pandas(
                self.sf_connection,
                df=df,
                schema="ETL_CTRL",
                table_name=recon_count_tracker_table_name,
            )
            if success:
                self.logger.info(
                    f"Successfully inserted {n_rows} rows into {recon_count_tracker_table_name}"
                )
            else:
                raise Exception(
                    f"Failed to insert rows into {recon_count_tracker_table_name}"
                )
        except Exception as exc:
            error_message = "Error occurred in insert_into_snowflake_table function"
            self.logger.error(error_message)
            raise Exception(error_message) from exc

    def handle_core_center(
        self,
        recon_days_offset: str,
        max_workers: int,
    ):
        self.logger.info(
            f"\n\nReconciliation for Core Center: '{self.core_center}' has started\n\n"
        )
        sf_batchid, sf_batch_date = self.fetch_sf_batch_details()
        recon_batch_date = sf_batch_date - timedelta(days=int(recon_days_offset))
        self.logger.info(
            f"The recon batch date considered for the batch is {recon_batch_date}"
        )
        try:
            self.update_jc_batch_status(
                status="Started",
                sf_batch_id=sf_batchid,
                sf_batch_date=sf_batch_date,
                recon_batch_date=recon_batch_date,
            )
            tables = self.fetch_common_tables(recon_batch_date=recon_batch_date)

            if len(tables) == 0:
                error_message = f"No common Tables found between RDS and Snowflake for core center : {self.core_center}"
                self.logger.error(error_message)
                self.insert_into_error_log(error_message=error_message)
                self.insert_into_jc_recon_process_log(
                    process_name="No common Tables found between RDS and Snowflake for core center",
                    count=0,
                )
                raise Exception(error_message)

            # 2) Set RDS & SF counts in parallel
            exceptions = self.parallel_set_record_count_and_missing_ids(
                tables=tables,
                max_workers=max_workers,
            )
            self.insert_into_recon_tracker_table(tables=tables)

            if len(exceptions) != 0:
                for exception in exceptions:
                    self.logger.error(exception)
                raise exceptions[0]

            self.update_jc_batch_status(
                status="Completed",
                sf_batch_id=sf_batchid if sf_batchid else 0,
                sf_batch_date=sf_batch_date if sf_batch_date else datetime.now(),
                recon_batch_date=recon_batch_date,
            )
            self.logger.info(f"Reconciliation for '{self.core_center}' is Completed")
        except Exception as e:
            self.update_jc_batch_status(
                status="Failed",
                sf_batch_id=sf_batchid if sf_batchid else 0,
                sf_batch_date=sf_batch_date if sf_batch_date else datetime.now(),
                recon_batch_date=(
                    recon_batch_date if recon_batch_date else datetime.now()
                ),
            )
            self.logger.info(
                f"The Reconciliation Job with Batch ID {self.recon_batchid} has Failed"
            )
            raise Exception(e)


def get_sf_stage_s3_url(sf_connection: SnowflakeConnection, stage_name: str) -> str:
    with sf_connection.cursor() as cursor:
        stage_urls_str = [
            property_value
            for (
                parent_property,
                property_name,
                property_type,
                property_value,
                property_default,
            ) in cursor.execute(f"DESCRIBE STAGE {stage_name};").fetchall()
            if property_name == "URL"
        ][0][1:-1]
        for stage_url in stage_urls_str.split(","):
            return stage_url[1:-1]
    raise Exception(f"No url found for stage: {stage_name}")


def write_report_to_s3(
    sf_connection: SnowflakeConnection,
    report_s3_prefix: str,
    recon_batchid: int,
    logger: structlog.BoundLogger,
):
    stage_name = "ETL_CTRL.EXTRACTS"
    file_path = (
        f"{report_s3_prefix}/"
        f"ReconBatchID_{recon_batchid}__{datetime.now().strftime('%Y_%m_%d')}.csv.gz"
    )
    with sf_connection.cursor() as cursor:
        result = cursor.execute(
            f"""
            COPY INTO @{stage_name}/{file_path}
                FROM (
                    SELECT RECON_BATCHID,
                           RECON_BATCH_DATE,
                           CORE_CENTER,
                           SCHEMA_NAME,
                           TABLE_NAME,
                           SUMMED_COLUMN_NAME,
                           RDS_COUNT,
                           SF_COUNT,
                           RDS_LESS_SF_RECORD_COUNT,
                           RECORD_COUNT_VARIANCE_THRESHOLD,
                           COUNT_UNDER_THRESHOLD,
                           RECURRENT_IDS_MISSING_FROM_SF,
                           RECURRENT_IDS_MISSING_FROM_RDS,
                           RDS_SUM,
                           SF_SUM,
                           RDS_LESS_SF_SUM,
                           SUM_VARIANCE_THRESHOLD,
                           SUM_UNDER_THRESHOLD,
                           AMOUNT_RECON_RESULT,
                           ROW_INSERT_TMS
                    FROM ETL_CTRL.VW_JC_RECON_REPORT
                    WHERE RECON_BATCHID = {recon_batchid}
                )
                FILE_FORMAT = (FORMAT_NAME = 'ETL_CTRL.FF_CSV_FORMAT')
                SINGLE = TRUE
                OVERWRITE = TRUE
                HEADER = TRUE
                DETAILED_OUTPUT  = TRUE;
            """
        ).fetchone()
        if not result:
            return
        (file_name, _, rows) = result
        stage_url = get_sf_stage_s3_url(
            sf_connection=sf_connection, stage_name=stage_name
        )

        logger.info(
            "Wrote Recurrent Missing ID Report",
            file_path=f"{stage_url}/{file_path}",
            mismatched_tables=rows,
        )

        full_path = f"{stage_url}/{file_path}"
        return full_path


# =============================================================================
# NEW FUNCTIONALITY: MissingIDsReconciler
# =============================================================================


@dataclasses.dataclass
class MissingIDsReconciler:
    sf_connection: SnowflakeConnection
    rds_clients: Dict[str, RDSClient]  # Mapping core center id -> RDSClient instance
    ignored_tables: Dict[str, IgnoreTable]
    batch_id: int
    logger: structlog.BoundLogger
    core_center: str

    def fetch_data_as_df(self, query: str) -> pd.DataFrame:
        try:
            self.logger.info(f"Executing query:\n{query}")
            with self.sf_connection.cursor() as cursor:
                cursor.execute(query)
                column_names = [desc[0] for desc in cursor.description]
                records = cursor.fetchall()
            df = pd.DataFrame(records, columns=column_names)
            self.logger.info(f"Query execution complete. Retrieved {len(df)} rows.")
            return df
        except Exception as e:
            error_msg = f"Error executing query: {query} - {e}"
            self.logger.error(error_msg)
            raise

    def clean_array_column(self, array_data):
        try:
            if isinstance(array_data, str):
                # For safety you might use ast.literal_eval instead of eval
                return sorted([int(x) for x in eval(array_data)])
            elif isinstance(array_data, list):
                return sorted(array_data)
        except Exception as e:
            self.logger.error(f"Error cleaning array column: {e}")
            raise

    def clean_ids_columns(
        self, df: pd.DataFrame, pattern: str = "IDS_MISSING"
    ) -> pd.DataFrame:
        try:
            self.logger.info("Starting clean_ids_columns process.")
            columns_to_clean = [col for col in df.columns if pattern in col]
            for col in columns_to_clean:
                self.logger.info(f"Cleaning column: {col}")
                df[col] = df[col].apply(self.clean_array_column)
            self.logger.info("Finished cleaning columns.")
            return df
        except Exception as e:
            error_msg = f"Error cleaning IDS columns: {e}"
            self.logger.error(error_msg)
            raise

    def filter_for_exemption_list(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(
                f"Starting exemption filtering on DataFrame with {len(df)} rows."
            )
            exempted_tables = {
                tbl.name for tbl in self.ignored_tables.values() if tbl.ignore_table
            }
            initial_count = len(df)
            filtered_df = df[~df["TABLE_NAME"].isin(exempted_tables)].copy()
            removed_count = initial_count - len(filtered_df)
            self.logger.info(
                f"Removed {removed_count} rows for fully exempted tables: {exempted_tables}"
            )
            ids_missing_columns = [
                col for col in filtered_df.columns if "IDS_MISSING" in col
            ]
            self.logger.info(
                f"Identified {len(ids_missing_columns)} 'IDS_MISSING' columns: {ids_missing_columns}"
            )
            for table in self.ignored_tables.values():
                if table.ignore_table:
                    continue
                if len(table.ignore_only_these_ids) == 0:
                    continue
                if table.name not in filtered_df["TABLE_NAME"].values:
                    self.logger.debug(
                        f"Table {table.name} not found in DataFrame, skipping exemption processing"
                    )
                    continue

                self.logger.info(
                    f"Processing table: {table.name}, removing specific IDs: {table.ignore_only_these_ids}"
                )
                for col in ids_missing_columns:
                    original_lengths = filtered_df.loc[
                        filtered_df["TABLE_NAME"] == table.name, col
                    ].apply(lambda x: len(x) if isinstance(x, list) else 0)
                    filtered_df.loc[
                        filtered_df["TABLE_NAME"] == table.name, col
                    ] = filtered_df.loc[
                        filtered_df["TABLE_NAME"] == table.name, col
                    ].apply(
                        lambda id_list: (
                            [
                                id_
                                for id_ in id_list
                                if id_ not in table.ignore_only_these_ids
                            ]
                            if isinstance(id_list, list)
                            else id_list
                        )
                    )
                    new_lengths = filtered_df.loc[
                        filtered_df["TABLE_NAME"] == table.name, col
                    ].apply(lambda x: len(x) if isinstance(x, list) else 0)
                    removed_counts = (original_lengths - new_lengths).sum()
                    self.logger.info(
                        f"Column '{col}' in table '{table.name}': Removed {removed_counts} IDs"
                    )
            self.logger.info(
                f"Exemption filtering complete. Final DataFrame has {len(filtered_df)} rows."
            )
            return filtered_df
        except Exception as e:
            error_msg = f"Error filtering exemption list: {e}"
            self.logger.error(error_msg)
            raise

    def execute_snowflake_query(self, query: str) -> set:
        try:
            self.logger.info("Executing query on Snowflake.")
            with self.sf_connection.cursor() as cursor:
                cursor.execute(query)
                result = {row[0] for row in cursor.fetchall() if row[0] is not None}
            self.logger.info(f"Snowflake query returned {len(result)} unique IDs.")
            return result
        except Exception as e:
            error_msg = f"Error executing Snowflake query: {query} - {e}"
            self.logger.error(error_msg)
            raise

    def execute_rds_query(self, rds_connection: RDSClient, query: str) -> set:
        try:
            self.logger.info("Executing query on RDS.")
            rows = rds_connection.run_query(query)
            result = {row[0] for row in rows if row[0] is not None}
            self.logger.info(f"RDS query returned {len(result)} unique IDs.")
            return result
        except Exception as e:
            error_msg = f"Error executing RDS query: {query} - {e}"
            self.logger.error(error_msg)
            raise

    def process_missing_ids_for_column(
        self,
        df,
        table_index,
        col,
        schema_name,
        table_name,
        db_connection,
        query_template,
        execute_query_fn,
        db_type,
    ):
        try:
            current_ids = df.at[table_index, col]
            if not isinstance(current_ids, list):
                current_ids = []
            current_ids = [x for x in current_ids if x is not None]
            initial_count = len(current_ids)
            if not current_ids:
                self.logger.info(
                    f"No missing IDs in column '{col}' for table '{table_name}'. Skipping."
                )
                return
            self.logger.info(
                f"Processing {initial_count} missing IDs in column '{col}' for table '{table_name}' ({db_type})."
            )
            updated_ids = []
            batch_size = 10000
            for i in range(0, len(current_ids), batch_size):
                batch_ids = current_ids[i : i + batch_size]
                query = query_template.format(
                    schema_name=schema_name,
                    table_name=table_name,
                    ids=", ".join(map(str, batch_ids)),
                )
                existing_ids = execute_query_fn(db_connection, query)
                batch_updated_ids = [
                    id_ for id_ in batch_ids if id_ not in existing_ids
                ]
                updated_ids.extend(batch_updated_ids)

            # 2) If these are RDS‑missing IDs, check for IDs that has been hard‑deleted
            #    in Snowflake (i.e. no longer show up where GW_DELETE_FLAG='N').
            if db_type == "RDS" and updated_ids:
                self.logger.info(
                    f"For column '{col}' in table '{table_name}' verifying if the missing IDs in RDS are still present in snowflake or marked GW_DELETE_FLAG = 'Y'"
                )
                sf_existing_ids: set[int] = set()
                for i in range(0, len(updated_ids), batch_size):
                    batch_ids = updated_ids[i : i + batch_size]
                    sf_query = (
                        f"SELECT DISTINCT ID "
                        f"FROM {schema_name}.{table_name} "
                        f"WHERE ID IN ({', '.join(map(str, batch_ids))}) "
                        f"AND GW_DELETE_FLAG = 'N'"
                    )
                    sf_existing_ids.update(self.execute_snowflake_query(sf_query))

                # keep only IDs still present in Snowflake
                final_ids = [id_ for id_ in updated_ids if id_ in sf_existing_ids]
                removed_sf = len(updated_ids) - len(final_ids)
                if removed_sf:
                    self.logger.info(
                        f"Column '{col}' in table '{table_name}': "
                        f"removed {removed_sf} IDs that are no longer present in Snowflake."
                    )
                updated_ids = final_ids
            df.at[table_index, col] = updated_ids
            removed_total = initial_count - len(updated_ids)
            self.logger.info(
                f"Column '{col}' in table '{table_name}': removed {removed_total} IDs; remaining {len(updated_ids)}."
            )
        except Exception as e:
            error_msg = f"Error processing missing IDs for column '{col}' in table '{table_name}': {e}"
            self.logger.error(error_msg)
            raise

    def process_table(
        self,
        table_name,
        rds_connection,
        previous_alert_checked_df,
        sf_missing_columns,
        rds_missing_columns,
    ):
        try:
            table_rows = previous_alert_checked_df[
                previous_alert_checked_df["TABLE_NAME"] == table_name
            ]
            if table_rows.empty:
                self.logger.info(
                    f"No rows found for table '{table_name}' in previous_alert_checked_df. Skipping."
                )
                return None
            table_index = table_rows.index[0]
            schema_name = previous_alert_checked_df.at[table_index, "SCHEMA_NAME"]
            has_missing = any(
                isinstance(previous_alert_checked_df.at[table_index, col], list)
                and len(previous_alert_checked_df.at[table_index, col]) > 0
                for col in sf_missing_columns + rds_missing_columns
            )
            if not has_missing:
                self.logger.debug(
                    f"No missing IDs to process for table '{table_name}'."
                )
                return table_index, {}
            self.logger.info(f"Processing missing IDs for table '{table_name}'.")
            for col in sf_missing_columns:
                self.process_missing_ids_for_column(
                    df=previous_alert_checked_df,
                    table_index=table_index,
                    col=col,
                    schema_name=schema_name,
                    table_name=table_name,
                    db_connection=self.sf_connection,
                    query_template="SELECT DISTINCT ID FROM {schema_name}.{table_name} WHERE ID IN ({ids})",
                    execute_query_fn=lambda _, q: self.execute_snowflake_query(q),
                    db_type="Snowflake",
                )
            for col in rds_missing_columns:
                self.process_missing_ids_for_column(
                    df=previous_alert_checked_df,
                    table_index=table_index,
                    col=col,
                    schema_name=schema_name,
                    table_name=table_name,
                    db_connection=rds_connection,
                    query_template="SELECT DISTINCT ID FROM public.{table_name} WHERE ID IN ({ids})",
                    execute_query_fn=lambda conn, q: self.execute_rds_query(conn, q),
                    db_type="RDS",
                )
            updated_columns = {
                col: previous_alert_checked_df.at[table_index, col]
                for col in sf_missing_columns + rds_missing_columns
            }
            self.logger.info(f"Finished processing table '{table_name}'.")
            return (table_index, updated_columns)
        except Exception as e:
            error_msg = f"Error processing table '{table_name}': {e}"
            self.logger.error(error_msg)
            raise

    @staticmethod
    def transition_ids_in_row(row):
        def to_set(x):
            return set(x) if isinstance(x, list) else set()

        def to_list(s):
            return sorted(list(s))

        sf_once = to_set(row.get("IDS_MISSING_FROM_SF_ONCE", []))
        sf_twice = to_set(row.get("IDS_MISSING_FROM_SF_TWICE", []))
        sf_frequent = to_set(row.get("IDS_MISSING_FROM_SF_FREQUENT", []))
        rds_once = to_set(row.get("IDS_MISSING_FROM_RDS_ONCE", []))
        rds_twice = to_set(row.get("IDS_MISSING_FROM_RDS_TWICE", []))
        rds_frequent = to_set(row.get("IDS_MISSING_FROM_RDS_FREQUENT", []))
        sf_frequent = sf_frequent.union(sf_twice)
        sf_twice = sf_once
        sf_once = set()
        rds_frequent = rds_frequent.union(rds_twice)
        rds_twice = rds_once
        rds_once = set()
        row["IDS_MISSING_FROM_SF_FREQUENT"] = to_list(sf_frequent)
        row["IDS_MISSING_FROM_SF_TWICE"] = to_list(sf_twice)
        row["IDS_MISSING_FROM_SF_ONCE"] = []
        row["IDS_MISSING_FROM_RDS_FREQUENT"] = to_list(rds_frequent)
        row["IDS_MISSING_FROM_RDS_TWICE"] = to_list(rds_twice)
        row["IDS_MISSING_FROM_RDS_ONCE"] = []
        return row

    def insert_into_missing_ids_trends_table(
        self, update_alert_df: pd.DataFrame, trends_table_name: str
    ) -> None:
        try:
            self.logger.info(
                f"Inserting {len(update_alert_df)} rows into {trends_table_name}."
            )
            success, _, n_rows, _ = write_pandas(
                self.sf_connection,
                df=update_alert_df,
                schema="ETL_CTRL",
                table_name=trends_table_name,
            )
            if success:
                self.logger.info(
                    f"Successfully inserted {n_rows} rows into {trends_table_name}."
                )
            else:
                raise Exception(f"Failed to insert rows into {trends_table_name}.")
        except Exception as e:
            error_msg = f"Error in insert_into_missing_ids_trends_table: {e}"
            self.logger.error(error_msg)
            raise

    def clean_duplicate_missing_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(
                "Starting duplicate ID removal for rows with UPDATETIME_COLUMN_PRESENT = True."
            )
            filtered_df = df[df["UPDATETIME_COLUMN_PRESENT"] == False].copy()

            def remove_common_ids(row, col1, col2):
                if isinstance(row[col1], list) and isinstance(row[col2], list):
                    set1, set2 = set(row[col1]), set(row[col2])
                    row[col1] = list(set1 - set2)
                return row

            filtered_df = filtered_df.apply(
                lambda row: remove_common_ids(
                    row, "IDS_MISSING_FROM_SF_ONCE", "IDS_MISSING_FROM_SF_TWICE"
                ),
                axis=1,
            )
            filtered_df = filtered_df.apply(
                lambda row: remove_common_ids(
                    row, "IDS_MISSING_FROM_SF_ONCE", "IDS_MISSING_FROM_SF_FREQUENT"
                ),
                axis=1,
            )
            filtered_df = filtered_df.apply(
                lambda row: remove_common_ids(
                    row, "IDS_MISSING_FROM_RDS_ONCE", "IDS_MISSING_FROM_RDS_TWICE"
                ),
                axis=1,
            )
            filtered_df = filtered_df.apply(
                lambda row: remove_common_ids(
                    row, "IDS_MISSING_FROM_RDS_ONCE", "IDS_MISSING_FROM_RDS_FREQUENT"
                ),
                axis=1,
            )
            df.update(filtered_df)
            self.logger.info(
                f"Completed duplicate ID removal for {len(filtered_df)} rows."
            )
            return df
        except Exception as e:
            error_msg = f"Error cleaning duplicate missing IDs: {e}"
            self.logger.error(error_msg)
            raise

    def transition_ids_chunk(self, df_chunk: pd.DataFrame) -> pd.DataFrame:
        try:
            sf_once = df_chunk["IDS_MISSING_FROM_SF_ONCE"].apply(set)
            sf_twice = df_chunk["IDS_MISSING_FROM_SF_TWICE"].apply(set)
            sf_frequent = df_chunk["IDS_MISSING_FROM_SF_FREQUENT"].apply(set)
            rds_once = df_chunk["IDS_MISSING_FROM_RDS_ONCE"].apply(set)
            rds_twice = df_chunk["IDS_MISSING_FROM_RDS_TWICE"].apply(set)
            rds_frequent = df_chunk["IDS_MISSING_FROM_RDS_FREQUENT"].apply(set)
            sf_frequent = sf_frequent.combine(sf_twice, lambda x, y: x.union(y))
            sf_twice = sf_once
            sf_once = pd.Series([set()] * len(df_chunk), index=df_chunk.index)
            rds_frequent = rds_frequent.combine(rds_twice, lambda x, y: x.union(y))
            rds_twice = rds_once
            rds_once = pd.Series([set()] * len(df_chunk), index=df_chunk.index)
            df_chunk["IDS_MISSING_FROM_SF_FREQUENT"] = sf_frequent.apply(sorted)
            df_chunk["IDS_MISSING_FROM_SF_TWICE"] = sf_twice.apply(sorted)
            df_chunk["IDS_MISSING_FROM_SF_ONCE"] = [[] for _ in range(len(df_chunk))]
            df_chunk["IDS_MISSING_FROM_RDS_FREQUENT"] = rds_frequent.apply(sorted)
            df_chunk["IDS_MISSING_FROM_RDS_TWICE"] = rds_twice.apply(sorted)
            df_chunk["IDS_MISSING_FROM_RDS_ONCE"] = [[] for _ in range(len(df_chunk))]
            return df_chunk
        except Exception as e:
            error_msg = f"Error transitioning IDs in chunk: {e}"
            self.logger.error(error_msg)
            raise

    def run_reconciliation_for_center(self, core_center_id: str):
        try:
            center_upper = core_center_id.upper()
            current_alert_table = f"JC_RECON_TRACKER_{center_upper}"
            previous_trends_table = f"JC_RECON_MISSING_IDS_TRENDS_{center_upper}"
            trends_table_name = previous_trends_table

            # --- Fetch current alert data ---
            current_alert_query = f"""
                SELECT *
                FROM ETL_CTRL.{current_alert_table}
                WHERE RECON_BATCHID = {self.batch_id}
            """
            self.logger.info(f"current_alert_query is {current_alert_query}")
            self.logger.info(
                f"Fetching current alert data for core center '{core_center_id}'."
            )
            current_alert_df = self.fetch_data_as_df(current_alert_query)
            current_alert_df = self.clean_ids_columns(current_alert_df, "IDS_MISSING")
            if current_alert_df.empty:
                raise Exception(
                    f"Current alert DataFrame for core center '{core_center_id}' is empty."
                )
            current_alert_df = current_alert_df[
                current_alert_df["RECON_PROCESS_SUCCESSFUL"] == True
            ]
            current_alert_df = self.filter_for_exemption_list(current_alert_df)

            # --- Fetch previous alert data ---
            previous_alert_query = f"""
                SELECT *
                FROM ETL_CTRL.{previous_trends_table}
                WHERE RECON_BATCHID = (
                    SELECT MAX(RECON_BATCHID)
                    FROM ETL_CTRL.{previous_trends_table}
                )
            """
            self.logger.info(
                f"Fetching previous alert data for core center '{core_center_id}'."
            )
            previous_alert_df = self.fetch_data_as_df(previous_alert_query)
            if not previous_alert_df.empty:
                previous_alert_df = self.clean_ids_columns(
                    previous_alert_df, "IDS_MISSING"
                )
                previous_alert_df = self.filter_for_exemption_list(previous_alert_df)
            # --- Prepare Final DataFrame for Insertion ---
            if previous_alert_df.empty:
                self.logger.info(
                    f"Previous alert DataFrame is empty for core center '{core_center_id}'. Using current alert data."
                )
                # If previous alert is empty, process only current_alert_df.
                # (Assume necessary columns have been cleaned/renamed.)
                current_alert_df["IDS_MISSING_FROM_SF"] = current_alert_df[
                    "IDS_MISSING_FROM_SF"
                ].apply(self.clean_array_column)
                current_alert_df["IDS_MISSING_FROM_RDS"] = current_alert_df[
                    "IDS_MISSING_FROM_RDS"
                ].apply(self.clean_array_column)
                column_mapping = {
                    "IDS_MISSING_FROM_SF": "IDS_MISSING_FROM_SF_ONCE",
                    "IDS_MISSING_FROM_RDS": "IDS_MISSING_FROM_RDS_ONCE",
                }
                renamed_df = current_alert_df.rename(columns=column_mapping)
                required_columns = [
                    "RECON_BATCHID",
                    "RECON_BATCH_DATE",
                    "SCHEMA_NAME",
                    "TABLE_NAME",
                    "UPDATETIME_COLUMN_PRESENT",
                    "TABLE_IN_EDW",
                    "IDS_MISSING_FROM_SF_ONCE",
                    "IDS_MISSING_FROM_RDS_ONCE",
                    "IDS_MISSING_FROM_SF_TWICE",
                    "IDS_MISSING_FROM_SF_FREQUENT",
                    "IDS_MISSING_FROM_RDS_TWICE",
                    "IDS_MISSING_FROM_RDS_FREQUENT",
                ]
                for col in required_columns:
                    if col not in renamed_df.columns:
                        self.logger.info(
                            f"Adding missing column '{col}' as empty list."
                        )
                        renamed_df[col] = [[] for _ in range(len(renamed_df))]
                update_alert_df = renamed_df[required_columns]
                self.logger.info(
                    f"Prepared update_alert_df shape: {update_alert_df.shape}"
                )
                self.insert_into_missing_ids_trends_table(
                    update_alert_df, trends_table_name
                )
                return update_alert_df

            else:
                self.logger.info(
                    f"Processing missing IDs for core center '{core_center_id}' using previous alert data."
                )
                sf_missing_cols = [
                    col
                    for col in previous_alert_df.columns
                    if "IDS_MISSING_FROM_SF_" in col
                ]
                rds_missing_cols = [
                    col
                    for col in previous_alert_df.columns
                    if "IDS_MISSING_FROM_RDS_" in col
                ]
                table_names = previous_alert_df["TABLE_NAME"].unique()
                self.logger.info(f"Found {len(table_names)} unique tables to process.")
                results = {}
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_table = {
                        executor.submit(
                            self.process_table,
                            tbl,
                            self.rds_clients[core_center_id],
                            previous_alert_df,
                            sf_missing_cols,
                            rds_missing_cols,
                        ): tbl
                        for tbl in table_names
                    }
                    for future in as_completed(future_to_table):
                        try:
                            result = future.result()
                            if result is not None:
                                table_index, updated_cols = result
                                results[table_index] = updated_cols
                        except Exception as exc:
                            self.logger.error(
                                f"Error processing table '{future_to_table[future]}': {exc}"
                            )
                self.logger.info("Transitioning IDs in previous alert DataFrame.")
                for col in [
                    "IDS_MISSING_FROM_SF_ONCE",
                    "IDS_MISSING_FROM_SF_TWICE",
                    "IDS_MISSING_FROM_SF_FREQUENT",
                    "IDS_MISSING_FROM_RDS_ONCE",
                    "IDS_MISSING_FROM_RDS_TWICE",
                    "IDS_MISSING_FROM_RDS_FREQUENT",
                ]:
                    previous_alert_df[col] = previous_alert_df[col].apply(
                        lambda x: x if isinstance(x, list) else []
                    )
                num_workers = 10
                chunk_size = max(1, len(previous_alert_df) // num_workers)
                chunk_results = []
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = [
                        executor.submit(
                            self.transition_ids_chunk,
                            previous_alert_df.iloc[i : i + chunk_size].copy(),
                        )
                        for i in range(0, len(previous_alert_df), chunk_size)
                    ]
                    for future in as_completed(futures):
                        chunk_results.append(future.result())

                previous_alert_transitioned_df = pd.concat(chunk_results).sort_index()
                self.logger.info("Completed ID transitions.")

                # --- Identify rows present only in previous alert (absent in current) ---
                # Note: We use the full transitioned previous DataFrame
                previous_missing_in_current_df = previous_alert_transitioned_df[
                    ~previous_alert_transitioned_df["TABLE_NAME"].isin(
                        current_alert_df["TABLE_NAME"]
                    )
                ].copy()

                # Extract current defaults from current_alert_df
                if not current_alert_df.empty:
                    current_recon_batchid = current_alert_df["RECON_BATCHID"].iloc[0]
                    current_recon_batch_date = current_alert_df[
                        "RECON_BATCH_DATE"
                    ].iloc[0]
                else:
                    raise Exception(
                        "Recon Batchid and Batchdate are missing in current_alert_df"
                    )
                previous_missing_in_current_df["RECON_BATCHID"] = current_recon_batchid
                previous_missing_in_current_df[
                    "RECON_BATCH_DATE"
                ] = current_recon_batch_date

                # Remove the 'ROW_INSERT_TMS' column if it exists
                if "ROW_INSERT_TMS" in previous_missing_in_current_df.columns:
                    previous_missing_in_current_df = (
                        previous_missing_in_current_df.drop(columns=["ROW_INSERT_TMS"])
                    )

                # --- Prepare DataFrames for merging ---
                self.logger.info(
                    "Preparing final DataFrame to insert into the trends table."
                )
                # For previous alert, keep only the necessary columns:
                cols_to_keep_previous = [
                    "SCHEMA_NAME",
                    "TABLE_NAME",
                    "IDS_MISSING_FROM_SF_TWICE",
                    "IDS_MISSING_FROM_SF_FREQUENT",
                    "IDS_MISSING_FROM_RDS_TWICE",
                    "IDS_MISSING_FROM_RDS_FREQUENT",
                ]
                previous_alert_cleaned_df = previous_alert_transitioned_df[
                    cols_to_keep_previous
                ]

                # For current alert, keep the necessary columns:
                cols_to_keep_current = [
                    "RECON_BATCHID",
                    "RECON_BATCH_DATE",
                    "SCHEMA_NAME",
                    "TABLE_NAME",
                    "UPDATETIME_COLUMN_PRESENT",
                    "TABLE_IN_EDW",
                    "IDS_MISSING_FROM_SF",
                    "IDS_MISSING_FROM_RDS",
                ]
                current_alert_cleaned_df = current_alert_df[
                    current_alert_df["RECON_PROCESS_SUCCESSFUL"] == True
                ][cols_to_keep_current]

                # Rename IDS columns from current as required downstream
                current_alert_cleaned_df = current_alert_cleaned_df.rename(
                    columns={
                        "IDS_MISSING_FROM_SF": "IDS_MISSING_FROM_SF_ONCE",
                        "IDS_MISSING_FROM_RDS": "IDS_MISSING_FROM_RDS_ONCE",
                    }
                )

                # --- Merge current and previous alerts ---
                # Use a left join to retain all current rows and add previous details where available.
                merged_df = current_alert_cleaned_df.merge(
                    previous_alert_cleaned_df,
                    on=["TABLE_NAME", "SCHEMA_NAME"],
                    how="left",
                    suffixes=("", "_prev"),
                )

                # --- Concatenate with rows that are only in previous alert ---
                if not previous_missing_in_current_df.empty:
                    update_alert_df = pd.concat(
                        [merged_df, previous_missing_in_current_df],
                        ignore_index=True,
                        sort=False,
                    )
                else:
                    update_alert_df = merged_df

                # Fill null IDS columns with empty lists
                update_alert_df["IDS_MISSING_FROM_SF_ONCE"] = update_alert_df[
                    "IDS_MISSING_FROM_SF_ONCE"
                ].apply(lambda x: x if isinstance(x, list) else [])
                update_alert_df["IDS_MISSING_FROM_SF_TWICE"] = update_alert_df[
                    "IDS_MISSING_FROM_SF_TWICE"
                ].apply(lambda x: x if isinstance(x, list) else [])
                update_alert_df["IDS_MISSING_FROM_SF_FREQUENT"] = update_alert_df[
                    "IDS_MISSING_FROM_SF_FREQUENT"
                ].apply(lambda x: x if isinstance(x, list) else [])
                # Fill null IDS columns with empty lists
                update_alert_df["IDS_MISSING_FROM_RDS_ONCE"] = update_alert_df[
                    "IDS_MISSING_FROM_RDS_ONCE"
                ].apply(lambda x: x if isinstance(x, list) else [])
                update_alert_df["IDS_MISSING_FROM_RDS_TWICE"] = update_alert_df[
                    "IDS_MISSING_FROM_RDS_TWICE"
                ].apply(lambda x: x if isinstance(x, list) else [])
                update_alert_df["IDS_MISSING_FROM_RDS_FREQUENT"] = update_alert_df[
                    "IDS_MISSING_FROM_RDS_FREQUENT"
                ].apply(lambda x: x if isinstance(x, list) else [])

                # --- Clean duplicate missing IDs ---
                update_alert_df = self.clean_duplicate_missing_ids(update_alert_df)
                self.logger.info(update_alert_df.head(10))
                self.logger.info(
                    f"Final update_alert_df shape: {update_alert_df.shape}"
                )

                # --- Insert final DataFrame into the trends table ---
                self.insert_into_missing_ids_trends_table(
                    update_alert_df, trends_table_name
                )

                # --- Delete old rows from the trends table ---
                delete_query = f"""
                    DELETE FROM ETL_CTRL.{trends_table_name}
                    WHERE RECON_BATCHID != {current_recon_batchid}
                """
                try:
                    self.logger.info(
                        f"Deleting old rows from the trends table {trends_table_name} using this query: \n {delete_query}."
                    )
                    with self.sf_connection.cursor() as cursor:
                        cursor.execute(delete_query)
                        rows_deleted = cursor.rowcount
                        self.logger.info(
                            f"Deleted {rows_deleted} rows from {trends_table_name}."
                        )
                except Exception as e:
                    error_msg = f"Error deleting old rows: {e}"
                    self.logger.error(error_msg)
                    raise

                self.logger.info(
                    f"Missing IDs reconciliation completed for core center '{core_center_id}'."
                )
                return update_alert_df
        except Exception as e:
            error_msg = f"Error in run_reconciliation_for_center for core center '{core_center_id}': {e}"
            self.logger.error(error_msg)
            raise

    def run(self):
        for core_center in self.rds_clients.keys():
            self.logger.info(
                f"--- Starting missing IDs reconciliation for core center '{core_center}' ---"
            )
            try:
                update_alert_df = self.run_reconciliation_for_center(core_center)
                self.logger.info(
                    f"\n--- Completed missing IDs reconciliation for core center '{core_center}' ---\n\n"
                )
                return update_alert_df
            except Exception as exc:
                self.logger.error(
                    f"Error during missing IDs reconciliation for core center '{core_center}': {exc}"
                )
                insert_into_error_log(
                    sf_connection=self.sf_connection,
                    logger=self.logger,
                    error_message=exc,
                    recon_batchid=self.batch_id,
                    core_center=self.core_center,
                )
                raise Exception


@dataclasses.dataclass(kw_only=True, frozen=True)
class IgnoreTable:
    name: str
    ignore_table: bool = False
    ignore_only_these_ids: list = dataclasses.field(default_factory=list)


def get_ignored_tables(
    logger: structlog.BoundLogger, sf_connection: SnowflakeConnection
) -> dict[str, IgnoreTable]:
    query = """
        SELECT TABLE_NAME, IGNORE_TABLE, IGNORE_IDS
        FROM ETL_CTRL.JC_RECON_EXEMPTION_LIST;
    """
    with sf_connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    ignored_tables = {}
    for row in rows:
        table_name, ignore_flag, ignore_ids_str = row
        ignore_ids = []
        if ignore_ids_str and ignore_ids_str.strip() != "":
            try:
                # Parse the string assuming it is a JSON array (e.g., "[9702,9708]")
                ignore_ids = json.loads(ignore_ids_str)
            except Exception as e:
                logger.error(f"Error parsing IGNORE_IDS for table {table_name}: {e}")
        ignored_tables[table_name] = IgnoreTable(
            name=table_name,
            ignore_table=ignore_flag,
            ignore_only_these_ids=ignore_ids,
        )

    if ignored_tables:
        logger.info("Fetched all records from JC_RECON_EXEMPTION_LIST:")
        for tbl in ignored_tables:
            logger.info(f"- {tbl}: {ignored_tables[tbl]}")
    else:
        logger.info("No records found in JC_RECON_EXEMPTION_LIST.")
    logger.info(f"Ignore table list is : {ignored_tables}.")
    return ignored_tables


def send_combined_missing_ids_notification(
    combined_update_alert_df: pd.DataFrame,
    sns_client: SNSClient,
    report_file_path: str,
    logger: structlog.BoundLogger,
):
    if not combined_update_alert_df.empty:
        # Helper function to count how many tables have non-empty lists
        def count_non_empty(df: pd.DataFrame, col_name: str) -> int:
            return (
                df[col_name].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
            )

        # Extract some basic info
        recon_batchid = combined_update_alert_df["RECON_BATCHID"].iloc[0]
        recon_batch_date = combined_update_alert_df["RECON_BATCH_DATE"].iloc[0]
        # Initialize message body
        message_body = "Hi Team,\n\nBelow is a summary of missing IDs from the latest reconciliation:\n\n"
        message_body += (
            f"  • Reconciliation Batch Id: {recon_batchid}\n"
            f"  • Reconciliation Batch date considered: {recon_batch_date}\n\n"
        )
        # Loop through each core center
        for core_center in combined_update_alert_df["SCHEMA_NAME"].unique():
            core_center_df = combined_update_alert_df[
                combined_update_alert_df["SCHEMA_NAME"] == core_center
            ]

            # Count non-empty IDs
            sf_once_count = count_non_empty(core_center_df, "IDS_MISSING_FROM_SF_ONCE")
            sf_twice_count = count_non_empty(
                core_center_df, "IDS_MISSING_FROM_SF_TWICE"
            )
            sf_frequent_count = count_non_empty(
                core_center_df, "IDS_MISSING_FROM_SF_FREQUENT"
            )

            rds_once_count = count_non_empty(
                core_center_df, "IDS_MISSING_FROM_RDS_ONCE"
            )
            rds_twice_count = count_non_empty(
                core_center_df, "IDS_MISSING_FROM_RDS_TWICE"
            )
            rds_frequent_count = count_non_empty(
                core_center_df, "IDS_MISSING_FROM_RDS_FREQUENT"
            )

            # Add core center-specific info to the message
            message_body += (
                f"Core Center: {core_center}\n"
                f"  ► No. of Tables with IDs missing in SF Twice:    {sf_twice_count}\n"
                f"  ► No. of Tables with IDs missing in SF Frequent: {sf_frequent_count}\n\n"
                f"  ► No. of Tables with IDs missing in RDS Twice:    {rds_twice_count}\n"
                f"  ► No. of Tables with IDs missing in RDS Frequent: {rds_frequent_count}\n\n\n"
            )

        message_body += "Thanks,\n"
        message_body += f"\nRefer to the report for current batch reconcilation results: {report_file_path}\n"

        logger.info(
            f"An SNS alert has been initiated with the following message: \n{message_body}"
        )
        subject_line = "Reconciliation Alert: IDs found to be missing"

        # Send SNS notification (example method)
        sns_client.send_notification(subject=subject_line, message=message_body)
    else:
        logger.info("combined_update_alert_df is empty. No missing IDs to notify.")


def main(
    snowflake_secret_name: str,
    snowflake_token_endpoint: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    rds_cluster_hostname: str,
    rds_cluster_secret_name: str,
    rds_cluster_port: int,
    core_center_config_dict: dict[str, CoreCenterConfig],
    rds_schema: str,
    recon_days_offset: str,
    report_s3_prefix: str,
    sns_topic_name: str | None,
    max_workers: int,
    logger: structlog.BoundLogger,
):
    authorization_token = get_token(
        session=aws_session,
        secret_name=snowflake_secret_name,
        token_endpoint=snowflake_token_endpoint,
        role_name=snowflake_role,
    )
    sf_connection = get_snowflake_connection(
        logger=_structlogger,
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
    s3_client = aws_session.client("s3")

    recon_batchid = get_next_batch_id(
        logger=logger,
        sf_connection=sf_connection,
    )

    ignored_tables = get_ignored_tables(
        logger=logger,
        sf_connection=sf_connection,
    )

    combined_update_alert_df = pd.DataFrame()

    logger.info(f"The Reconciliation Job with Batch ID {recon_batchid} has started")

    # Process each core center sequentially: run core center reconciliation and then missing IDs reconciliation.
    for core_center, core_center_config in core_center_config_dict.items():
        rds_connection = RDSClient.factory(
            session=aws_session,
            secret_arn=rds_cluster_secret_name,
            logger=logger,
            database_name=core_center_config.rds_db_name,
            rds_cluster_hostname=rds_cluster_hostname,
            rds_cluster_port=rds_cluster_port,
        )
        core_center_handler = CoreCenterHandler(
            recon_batchid=recon_batchid,
            core_center=core_center,
            logger=logger,
            sns_client=sns_client,
            s3_client=s3_client,
            sf_connection=sf_connection,
            sf_schema=core_center_config.snowflake_schema,
            rds_connection=rds_connection,
            rds_schema=rds_schema,
            ignored_tables=ignored_tables,
        )
        core_center_handler.handle_core_center(
            recon_days_offset=recon_days_offset,
            max_workers=max_workers,
        )

        missing_ids_reconciler = MissingIDsReconciler(
            sf_connection=sf_connection,
            rds_clients={core_center: rds_connection},
            ignored_tables=ignored_tables,
            batch_id=recon_batchid,
            logger=logger,
            core_center=core_center,
        )
        update_alert_df = missing_ids_reconciler.run()

        if not update_alert_df.empty:
            combined_update_alert_df = pd.concat(
                [combined_update_alert_df, update_alert_df], ignore_index=True
            )

    report_file_path = write_report_to_s3(
        sf_connection=sf_connection,
        recon_batchid=recon_batchid,
        report_s3_prefix=report_s3_prefix,
        logger=logger,
    )

    send_combined_missing_ids_notification(
        combined_update_alert_df=combined_update_alert_df,
        sns_client=sns_client,
        report_file_path=report_file_path,
        logger=logger,
    )

    logger.info(
        f"The Reconciliation Job with Batch ID {recon_batchid} is completed for all Core Centers"
    )


if __name__ == "__main__" and IS_GLUE_JOB:
    _args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "snowflake_secret_name",
            "snowflake_token_endpoint",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "snowflake_role",
            "rds_cluster_hostname",
            "rds_cluster_secret_name",
            "rds_cluster_port",
            "rds_db_name_cc",
            "rds_db_name_bc",
            "rds_db_name_cm",
            "rds_db_name_pc",
            "rds_schema",
            "recon_days_offset",
            "report_s3_prefix",
            "sns_topic_name",
        ],
    )
    _sc = SparkContext()
    _glue_context = GlueContext(_sc)
    _job = Job(_glue_context)
    _job.init(_args["JOB_NAME"], _args)
    _structlogger = get_structlog_logger(_glue_context.get_logger())
    _structlogger.info("The Job is running in AWS Glue")
    aws_session = AWSSession(region_name="us-east-1")

    main(
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        snowflake_role=_args["snowflake_role"],
        rds_cluster_hostname=_args["rds_cluster_hostname"],
        rds_cluster_secret_name=_args["rds_cluster_secret_name"],
        rds_cluster_port=int(_args["rds_cluster_port"]),
        core_center_config_dict={
            "cc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_cc"],
                snowflake_schema="mrg_cc",
            ),
            "bc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_bc"],
                snowflake_schema="mrg_bc",
            ),
            "cm": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_cm"],
                snowflake_schema="mrg_cm",
            ),
            "pc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_pc"],
                snowflake_schema="mrg_pc",
            ),
        },
        rds_schema=_args["rds_schema"],
        recon_days_offset=_args["recon_days_offset"],
        report_s3_prefix=_args["report_s3_prefix"],
        sns_topic_name=(
            None if _args["sns_topic_name"] == "" else _args["sns_topic_name"]
        ),
        max_workers=10,
        logger=_structlogger,
    )
    _job.commit()


if __name__ == "__main__" and not IS_GLUE_JOB:
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    _structlogger = get_structlog_logger(logger=_logger)
    _structlogger.info("The Job is running in Local")
    _args = {
        "JOB_NAME": "RDS_SF_Tables_Count_Reconciliation",
        "snowflake_secret_name": "dev-aws-glue",
        "snowflake_token_endpoint": "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token",
        "snowflake_account": "encova-dev.privatelink",
        "snowflake_user": "0oa25sh1ca4BsKnGC0h8",
        "snowflake_database": "DEV1E2E",
        "snowflake_warehouse": "WH_GLUE",
        "snowflake_role": "DEV1E2E__GLUE__SVC_ROLE",
        "rds_cluster_hostname": "gwcp.dev1e2e.rdscluster.aws.encova.dev.internal",
        "rds_cluster_secret_name": "D-AWS-SecretsHub-Dev/aws-use1-dev-smr-glue-cda-read-0001",
        "rds_cluster_port": 5432,
        "rds_db_name_cc": "encova_encovadev_dev1e2e_cc",
        "rds_db_name_bc": "encova_encovadev_dev1e2e_bc",
        "rds_db_name_cm": "encova_encovadev_dev1e2e_cm",
        "rds_db_name_pc": "encova_encovadev_dev1e2e_pc",
        "rds_schema": "public",
        "recon_days_offset": 1,
        "report_s3_prefix": "Reconciliation_Report",
        "sns_topic_name": "",
    }
    aws_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )

    main(
        snowflake_secret_name=_args["snowflake_secret_name"],
        snowflake_token_endpoint=_args["snowflake_token_endpoint"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        snowflake_role=_args["snowflake_role"],
        rds_cluster_hostname=_args["rds_cluster_hostname"],
        rds_cluster_secret_name=_args["rds_cluster_secret_name"],
        rds_cluster_port=int(_args["rds_cluster_port"]),
        core_center_config_dict={
            "cc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_cc"],
                snowflake_schema="mrg_cc",
            ),
            "bc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_bc"],
                snowflake_schema="mrg_bc",
            ),
            "cm": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_cm"],
                snowflake_schema="mrg_cm",
            ),
            "pc": CoreCenterConfig(
                rds_db_name=_args["rds_db_name_pc"],
                snowflake_schema="mrg_pc",
            ),
        },
        rds_schema=_args["rds_schema"],
        recon_days_offset=_args["recon_days_offset"],
        report_s3_prefix=_args["report_s3_prefix"],
        sns_topic_name=(
            None if _args["sns_topic_name"] == "" else _args["sns_topic_name"]
        ),
        max_workers=10,
        logger=_structlogger,
    )
