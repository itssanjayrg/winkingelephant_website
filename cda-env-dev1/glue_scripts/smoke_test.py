from __future__ import annotations
import logging
import dataclasses
import sys
import json
import psycopg2
from boto3.session import Session as AWSSession
import time
from contextlib import contextmanager
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from utilities.SNSClient import SNSClient
from utilities.get_local_spark_session import get_local_spark_session
from utilities.snowflake_utils import get_snowflake_connection
from utilities.oauth_utils import get_assumed_role_session, get_token
from utilities.logging_utils import get_structlog_logger
from utilities.JDBCConnection import JDBCConnection, JDBCResultSet
from typing import Optional, Generator, List
from botocore.client import BaseClient

try:
    IS_GLUE_JOB = True
    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions

    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job
except ModuleNotFoundError:
    IS_GLUE_JOB = False

logging.basicConfig(
    format="%(asctime)s : %(filename)s - %(funcName)s : %(lineno)d : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
logger = get_structlog_logger(logger=_logger)


@dataclasses.dataclass(kw_only=True)
class MsSqlClient:
    ms_sql_jdbc_url: str
    ms_sql_jdbc_properties: dict[str, str | int]
    _connection: Optional[JDBCConnection] = dataclasses.field(default=None, init=False)

    def get_connection(self) -> JDBCConnection:
        try:
            logger.info("Establishing MS SQL Server connection using JDBC.")
            spark_session = SparkSession.builder.getOrCreate()
            # noinspection PyUnresolvedReferences,PyProtectedMember
            spark_session._jvm.Class.forName(self.ms_sql_jdbc_properties["driver"])
            # noinspection PyUnresolvedReferences,PyProtectedMember
            connection = spark_session._jvm.java.sql.DriverManager.getConnection(
                self.ms_sql_jdbc_url,
                self.ms_sql_jdbc_properties["user"],
                self.ms_sql_jdbc_properties["password"],
            )
            logger.info("Connection established to MS SQL Server.")
            return connection
        except Exception:
            logger.exception("Failed to establish MS SQL Server connection.")
            raise

    @property
    def connection(self) -> JDBCConnection:
        if self._connection is None:
            self._connection = self.get_connection()
        return self._connection

    @contextmanager
    def execute_query(self, query: str) -> Generator[JDBCResultSet, None, None]:
        stmt = self.connection.createStatement()
        try:
            yield stmt.executeQuery(query)
        finally:
            stmt.close()

    @contextmanager
    def execute_update(self, query: str) -> Generator[int, None, None]:
        stmt = self.connection.createStatement()
        try:
            yield stmt.executeUpdate(query)
        finally:
            stmt.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Connection to MS SQL Server closed.")


@dataclasses.dataclass(frozen=True, kw_only=True)
class S3Config:
    src_bucket: str
    src_file_key: str
    dest_bucket: str
    dest_file_key: str

    def test_guidewire_s3_read_encova_s3_write(self, s3_client: BaseClient) -> None:
        try:
            logger.info(f"Reading from bucket: {self.src_bucket}")
            response = s3_client.get_object(
                Bucket=self.src_bucket,
                Key=f"{self.src_file_key}manifest.json",
            )
            file_content = response["Body"].read()
            logger.info(
                f"Read successful from {self.src_bucket}; content length: {len(file_content)} bytes"
            )

            logger.info(f"Writing to bucket: {self.dest_bucket}")
            s3_client.put_object(
                Bucket=self.dest_bucket,
                Key=self.dest_file_key,
                Body=file_content,
            )
            logger.info("Write successful")

            logger.info(f"Deleting test object from bucket: {self.dest_bucket}")
            s3_client.delete_object(Bucket=self.dest_bucket, Key=self.dest_file_key)
            logger.info("Delete successful")
        except Exception as exc:
            error_msg = (
                f"S3 test error (src: {self.src_bucket} or dest: {self.dest_bucket})"
            )
            logger.exception(error_msg)
            raise Exception(error_msg) from exc


def get_s3_configs(
    src_bucket: str,
    gw_bc_dir: str,
    gw_cc_dir: str,
    gw_cm_dir: str,
    gw_pc_dir: str,
    tgt_s3_bc_bucket: str,
    tgt_s3_cc_bucket: str,
    tgt_s3_cm_bucket: str,
    tgt_s3_pc_bucket: str,
) -> list[S3Config]:
    return [
        S3Config(
            src_bucket=src_bucket,
            src_file_key=gw_bc_dir,
            dest_bucket=tgt_s3_bc_bucket,
            dest_file_key="test_connection/bc_manifest.json",
        ),
        S3Config(
            src_bucket=src_bucket,
            src_file_key=gw_cc_dir,
            dest_bucket=tgt_s3_cc_bucket,
            dest_file_key="test_connection/cc_manifest.json",
        ),
        S3Config(
            src_bucket=src_bucket,
            src_file_key=gw_cm_dir,
            dest_bucket=tgt_s3_cm_bucket,
            dest_file_key="test_connection/cm_manifest.json",
        ),
        S3Config(
            src_bucket=src_bucket,
            src_file_key=gw_pc_dir,
            dest_bucket=tgt_s3_pc_bucket,
            dest_file_key="test_connection/pc_manifest.json",
        ),
    ]


def test_snowflake_connection(
    authorization_token: str | None,
    snowflake_role: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
):
    if authorization_token is None:
        raise Exception("authorization_token is None")

    try:
        logger.info("Connecting to Snowflake...")
        with get_snowflake_connection(
            logger=logger,
            snowflake_account=snowflake_account,
            snowflake_user=snowflake_user,
            snowflake_database=snowflake_database,
            snowflake_warehouse=snowflake_warehouse,
            snowflake_role=snowflake_role,
            authorization_token=authorization_token,
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP;")
            result = cursor.fetchone()
            logger.info(f"Snowflake read successful; current timestamp: {result[0]}")
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING);"
            )
            cursor.execute("INSERT INTO test_table VALUES (1, 'TestUser');")
            conn.commit()
            logger.info("Snowflake write successful.")
            cursor.execute("DROP TABLE IF EXISTS test_table;")
            conn.commit()
            logger.info("Snowflake cleanup successful.")
    except Exception as exc:
        error_msg = f"Snowflake test error"
        logger.exception(error_msg)
        raise Exception(error_msg) from exc


def test_rds_connection(
    secret: dict | None,
    rds_cluster_hostname: str,
    rds_cluster_port: int,
    rds_schema: str,
    database_name: str,
) -> None:
    if secret is None:
        raise Exception("RDS secret is None")

    try:
        logger.info(f"Connecting to RDS database: {database_name}")
        conn = psycopg2.connect(
            database=database_name,
            user=secret["username"],
            password=secret["password"],
            host=rds_cluster_hostname,
            port=rds_cluster_port,
        )
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{rds_schema}';"
        )
        result = cursor.fetchall()
        table_names = [row[0] for row in result[:20]]
        logger.info(
            f"RDS read successful for {database_name}; tables: {', '.join(table_names)}"
        )
        cursor.close()
        conn.close()
    except Exception as exc:
        error_msg = f"RDS test error for {database_name}"
        logger.exception(error_msg)
        raise Exception(error_msg) from exc


def get_mssql_client(
    database_name: str,
    ms_sql_port: int,
    secret: dict | None,
) -> MsSqlClient:
    if secret is None:
        raise Exception("MS SQL secret is None")
    logger.info(f"Connecting to MS SQL database: {database_name}")
    ms_sql_jdbc_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": secret["username"],
        "password": secret["password"],
        "batchsize": 1000,
    }
    ms_sql_jdbc_url = (
        f"jdbc:sqlserver://{secret['server']}:{ms_sql_port};"
        f"databaseName={database_name};"
        f"{'' if IS_GLUE_JOB else 'encrypt=false;'}"
    )
    client = MsSqlClient(
        ms_sql_jdbc_url=ms_sql_jdbc_url,
        ms_sql_jdbc_properties=ms_sql_jdbc_properties,
    )
    time.sleep(5)
    return client


def test_mssql_read(database_name: str, mssql_client: MsSqlClient | None) -> None:
    if MsSqlClient is None:
        raise Exception("MS SQL secret is None")

    try:
        with mssql_client.execute_query(
            query="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        ) as result_set:
            result_list = []
            while result_set.next():
                result_list.append(result_set.getString(1))
            logger.info(
                f"Fetched table names from {database_name}: {', '.join(result_list[:5])}"
            )
    except Exception as exc:
        error_msg = f"MS SQL read error for {database_name}"
        logger.exception(error_msg)
        raise Exception(error_msg) from exc


def test_mssql_insert(database_name: str, mssql_client: MsSqlClient | None) -> None:
    if MsSqlClient is None:
        raise Exception("MS SQL secret is None")

    stmt = None
    try:
        stmt = mssql_client.connection.createStatement()
        stmt.executeUpdate(
            """
            IF NOT EXISTS (
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'test_table'
            )
            CREATE TABLE test_table (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
            """
        )
        logger.info(f"Created test_table in {database_name}")
        stmt.executeUpdate("INSERT INTO test_table (id, name) VALUES (1, 'TestUser')")
        logger.info(f"Inserted test data into {database_name}.test_table")
    except Exception as exc:
        error_msg = f"MS SQL write error for {database_name}"
        logger.exception(error_msg)
        raise Exception(error_msg) from exc
    finally:
        if stmt:
            stmt.close()


def test_mssql_delete(database_name: str, mssql_client: MsSqlClient | None) -> None:
    stmt = None
    try:
        stmt = mssql_client.connection.createStatement()
        stmt.executeUpdate("DELETE FROM test_table WHERE id = 1")
        logger.info(f"Deleted test data from {database_name}.test_table")
    except Exception as exc:
        error_msg = f"MS SQL delete error for {database_name}"
        logger.exception(error_msg)
        raise Exception(error_msg) from exc
    finally:
        if stmt:
            stmt.close()


def main(
    session: AWSSession,
    src_bucket: str,
    gw_bc_dir: str,
    gw_cc_dir: str,
    gw_cm_dir: str,
    gw_pc_dir: str,
    tgt_s3_bc_bucket: str,
    tgt_s3_cc_bucket: str,
    tgt_s3_cm_bucket: str,
    tgt_s3_pc_bucket: str,
    secret_name: str,
    token_endpoint: str,
    snowflake_role: str,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    rds_cluster_hostname: str,
    rds_cluster_secret_name: str,
    rds_cluster_port: int,
    rds_db_name_cc: str,
    rds_db_name_bc: str,
    rds_db_name_cm: str,
    rds_db_name_pc: str,
    rds_schema: str,
    ms_sql_db_pc: str,
    ms_sql_db_bc: str,
    ms_sql_db_cc: str,
    ms_sql_db_cm: str,
    ms_sql_port: int,
    ms_sql_secret_name: str,
    glue_sns_topic_name: str,
):
    s3_client = session.client("s3")
    sm_client = session.client("secretsmanager")
    summary: dict[str, bool] = {}
    all_errors: List[str] = []

    test_name = f"SNS Topic Creation and Use -{glue_sns_topic_name}"
    try:
        sns_client = SNSClient.factory(
            session=session, topic_name=glue_sns_topic_name, logger=logger
        )
        sns_client.send_notification(
            subject=f"Smoke testing {glue_sns_topic_name} SNS Topic Functionality",
            message="Hi team,\n\n" "This is a good sign!",
        )
        summary[test_name] = True
    except Exception as exc:
        summary[test_name] = False
        all_errors.append(str(exc))

    # S3 Test
    s3_configs = get_s3_configs(
        src_bucket=src_bucket,
        gw_bc_dir=gw_bc_dir,
        gw_cc_dir=gw_cc_dir,
        gw_cm_dir=gw_cm_dir,
        gw_pc_dir=gw_pc_dir,
        tgt_s3_bc_bucket=tgt_s3_bc_bucket,
        tgt_s3_cc_bucket=tgt_s3_cc_bucket,
        tgt_s3_cm_bucket=tgt_s3_cm_bucket,
        tgt_s3_pc_bucket=tgt_s3_pc_bucket,
    )
    for s3_config in s3_configs:
        test_name = f"Guidewire S3 Read and Encova S3 Write - {s3_config.dest_bucket}"
        try:
            s3_config.test_guidewire_s3_read_encova_s3_write(s3_client=s3_client)
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

    authorization_token = None
    test_name = "Snowflake Authorization Token"
    try:
        authorization_token = get_token(
            session=session,
            secret_name=secret_name,
            token_endpoint=token_endpoint,
            role_name=snowflake_role,
        )
        summary[test_name] = True
    except Exception as exc:
        summary[test_name] = False
        all_errors.append(str(exc))

    # Snowflake Test
    test_name = "Snowflake Connection"
    try:
        test_snowflake_connection(
            authorization_token=authorization_token,
            snowflake_role=snowflake_role,
            snowflake_account=snowflake_account,
            snowflake_user=snowflake_user,
            snowflake_database=snowflake_database,
            snowflake_warehouse=snowflake_warehouse,
        )
        summary[test_name] = True
    except Exception as exc:
        summary[test_name] = False
        all_errors.append(str(exc))

    rds_secret = None
    test_name = "Fetch RDS Secret Value"
    try:
        secret_response = sm_client.get_secret_value(SecretId=rds_cluster_secret_name)
        rds_secret = json.loads(secret_response["SecretString"])
        summary[test_name] = True
    except Exception as exc:
        summary[test_name] = False
        all_errors.append(str(exc))

    # RDS Test
    for database_name in [
        rds_db_name_cc,
        rds_db_name_bc,
        rds_db_name_cm,
        rds_db_name_pc,
    ]:
        test_name = f"RDS Connection - {database_name}"
        try:
            test_rds_connection(
                secret=rds_secret,
                rds_cluster_hostname=rds_cluster_hostname,
                rds_cluster_port=rds_cluster_port,
                database_name=database_name,
                rds_schema=rds_schema,
            )
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

    test_name = "Fetch MS_SQL Secret Value"
    ms_sql_secret = None
    try:
        secret_response = sm_client.get_secret_value(SecretId=ms_sql_secret_name)
        ms_sql_secret = json.loads(secret_response["SecretString"])
        summary[test_name] = True
    except Exception as exc:
        summary[test_name] = False
        all_errors.append(str(exc))

    # MS SQL Test
    for database_key, database_name in {
        "cm": ms_sql_db_cm,
        "pc": ms_sql_db_pc,
        "bc": ms_sql_db_bc,
        "cc": ms_sql_db_cc,
    }.items():
        mssql_client = None
        test_name = f"MS SQL Server Connection Established = {database_name}"
        try:
            mssql_client = get_mssql_client(
                database_name=database_name,
                ms_sql_port=ms_sql_port,
                secret=ms_sql_secret,
            )
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

        test_name = f"MS SQL Server Read = {database_name}"
        try:
            test_mssql_read(database_name=database_name, mssql_client=mssql_client)
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

        test_name = f"MS SQL Server Insert = {database_name}"
        try:
            test_mssql_insert(database_name=database_name, mssql_client=mssql_client)
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

        test_name = f"MS SQL Server Delete = {database_name}"
        try:
            test_mssql_delete(database_name=database_name, mssql_client=mssql_client)
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

        test_name = f"MS SQL Server Close Connection = {database_name}"
        try:
            mssql_client.connection.close()
            logger.info(f"Closed connection to {database_name}")
            summary[test_name] = True
        except Exception as exc:
            summary[test_name] = False
            all_errors.append(str(exc))

    # Log summary report
    logger.info("\n========== SUMMARY REPORT ==========")

    successful_tests = [test for test, result in summary.items() if result is True]
    logger.info(f"Successful Tests ({len(successful_tests)}):")
    for item in successful_tests:
        logger.info(f"   - {item}")

    failed_tests = [test for test, result in summary.items() if result is False]
    logger.info(f"Failed Tests ({len(failed_tests)}):")
    for item in failed_tests:
        logger.error(f"   - {item}")

    # Raise exception at end if any test failed
    if all_errors:
        raise Exception(
            "One or more tests failed. Check logs for details: " + "; ".join(all_errors)
        )
    else:
        logger.info("All tests completed successfully!")


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    _args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "src_bucket",
            "gw_bc_dir",
            "gw_cc_dir",
            "gw_cm_dir",
            "gw_pc_dir",
            "tgt_s3_bc_bucket",
            "tgt_s3_cc_bucket",
            "tgt_s3_cm_bucket",
            "tgt_s3_pc_bucket",
            "secret_name",
            "token_endpoint",
            "snowflake_role",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "rds_cluster_hostname",
            "rds_cluster_secret_name",
            "rds_cluster_port",
            "rds_db_name_cc",
            "rds_db_name_bc",
            "rds_db_name_cm",
            "rds_db_name_pc",
            "rds_schema",
            "ms_sql_db_pc",
            "ms_sql_db_bc",
            "ms_sql_db_cc",
            "ms_sql_db_cm",
            "ms_sql_port",
            "ms_sql_secret_name",
            "glue_sns_topic_name",
        ],
    )

    _sc = SparkContext()
    _glue_context = GlueContext(_sc)
    _job = Job(_glue_context)
    _job.init(_args["JOB_NAME"], _args)
    _session = AWSSession(region_name="us-east-1")
    _spark = _glue_context.spark_session

    main(
        session=_session,
        src_bucket=_args["src_bucket"],
        gw_bc_dir=_args["gw_bc_dir"],
        gw_cc_dir=_args["gw_cc_dir"],
        gw_cm_dir=_args["gw_cm_dir"],
        gw_pc_dir=_args["gw_pc_dir"],
        tgt_s3_bc_bucket=_args["tgt_s3_bc_bucket"],
        tgt_s3_cc_bucket=_args["tgt_s3_cc_bucket"],
        tgt_s3_cm_bucket=_args["tgt_s3_cm_bucket"],
        tgt_s3_pc_bucket=_args["tgt_s3_pc_bucket"],
        secret_name=_args["secret_name"],
        token_endpoint=_args["token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        rds_cluster_hostname=_args["rds_cluster_hostname"],
        rds_cluster_secret_name=_args["rds_cluster_secret_name"],
        rds_cluster_port=_args["rds_cluster_port"],
        rds_db_name_cc=_args["rds_db_name_cc"],
        rds_db_name_bc=_args["rds_db_name_bc"],
        rds_db_name_cm=_args["rds_db_name_cm"],
        rds_db_name_pc=_args["rds_db_name_pc"],
        rds_schema=_args["rds_schema"],
        ms_sql_db_pc=_args["ms_sql_db_pc"],
        ms_sql_db_bc=_args["ms_sql_db_bc"],
        ms_sql_db_cc=_args["ms_sql_db_cc"],
        ms_sql_db_cm=_args["ms_sql_db_cm"],
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        glue_sns_topic_name=_args["glue_sns_topic_name"],
    )
    _job.commit()

elif __name__ == "__main__" and not IS_GLUE_JOB:
    _args = {
        "src_bucket": "beta-3-us-east-1-encova-encova-cda-56c9a",
        "gw_bc_dir": "dev1e2e/bc/1745172445388/",
        "gw_cc_dir": "dev1e2e/cc/1745174274718/",
        "gw_cm_dir": "dev1e2e/cm/1745172304241/",
        "gw_pc_dir": "dev1e2e/pc/1745174051807/",
        "tgt_s3_bc_bucket": "encova-aws-use1-dev1-s3-cda-bc-0001",
        "tgt_s3_cc_bucket": "encova-aws-use1-dev1-s3-cda-cc-0001",
        "tgt_s3_cm_bucket": "encova-aws-use1-dev1-s3-cda-cm-0001",
        "tgt_s3_pc_bucket": "encova-aws-use1-dev1-s3-cda-pc-0001",
        "secret_name": "dev-aws-glue",
        "token_endpoint": "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token",
        "snowflake_role": "DEV1E2E__GLUE__SVC_ROLE",
        "snowflake_account": "encova-dev.privatelink",
        "snowflake_user": "0oa25sh1ca4BsKnGC0h8",
        "snowflake_database": "DEV1E2E",
        "snowflake_warehouse": "WH_GLUE",
        "rds_cluster_hostname": "gwcp.dev1e2e.rdscluster.aws.encova.dev.internal",
        "rds_cluster_secret_name": "D-AWS-SecretsHub-Dev/aws-use1-dev-smr-glue-cda-read-0001",
        "rds_cluster_port": 5432,
        "rds_db_name_cc": "encova_encovadev_dev1e2e_cc",
        "rds_db_name_bc": "encova_encovadev_dev1e2e_bc",
        "rds_db_name_cm": "encova_encovadev_dev1e2e_cm",
        "rds_db_name_pc": "encova_encovadev_dev1e2e_pc",
        "rds_schema": "public",
        "ms_sql_db_pc": "PolicyCenterR1",
        "ms_sql_db_bc": "BillingCenterR1",
        "ms_sql_db_cc": "ClaimCenterR1",
        "ms_sql_db_cm": "ContactManagerR1",
        "ms_sql_port": 59796,
        "ms_sql_secret_name": "arn:aws:secretsmanager:us-east-1:058264312281:secret:aws-use1-dev-smr-ohdwdbs0024va-0001-mbDya0",
        "glue_sns_topic_name": "sns-cda-dev1-glue-job-0001",
    }
    assumed_role_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )
    _spark = get_local_spark_session(aws_session=assumed_role_session)
    main(
        session=assumed_role_session,
        src_bucket=_args["src_bucket"],
        gw_bc_dir=_args["gw_bc_dir"],
        gw_cc_dir=_args["gw_cc_dir"],
        gw_cm_dir=_args["gw_cm_dir"],
        gw_pc_dir=_args["gw_pc_dir"],
        tgt_s3_bc_bucket=_args["tgt_s3_bc_bucket"],
        tgt_s3_cc_bucket=_args["tgt_s3_cc_bucket"],
        tgt_s3_cm_bucket=_args["tgt_s3_cm_bucket"],
        tgt_s3_pc_bucket=_args["tgt_s3_pc_bucket"],
        secret_name=_args["secret_name"],
        token_endpoint=_args["token_endpoint"],
        snowflake_role=_args["snowflake_role"],
        snowflake_account=_args["snowflake_account"],
        snowflake_user=_args["snowflake_user"],
        snowflake_database=_args["snowflake_database"],
        snowflake_warehouse=_args["snowflake_warehouse"],
        rds_cluster_hostname=_args["rds_cluster_hostname"],
        rds_cluster_secret_name=_args["rds_cluster_secret_name"],
        rds_cluster_port=_args["rds_cluster_port"],
        rds_db_name_cc=_args["rds_db_name_cc"],
        rds_db_name_bc=_args["rds_db_name_bc"],
        rds_db_name_cm=_args["rds_db_name_cm"],
        rds_db_name_pc=_args["rds_db_name_pc"],
        rds_schema=_args["rds_schema"],
        ms_sql_db_pc=_args["ms_sql_db_pc"],
        ms_sql_db_bc=_args["ms_sql_db_bc"],
        ms_sql_db_cc=_args["ms_sql_db_cc"],
        ms_sql_db_cm=_args["ms_sql_db_cm"],
        ms_sql_port=_args["ms_sql_port"],
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        glue_sns_topic_name=_args["glue_sns_topic_name"],
    )
