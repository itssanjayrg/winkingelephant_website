import sys
import pymssql
import pandas as pd
import boto3
import os
import re
import logging
from utilities.oauth_utils import get_assumed_role_session, get_secret
from boto3.session import Session as AWSSession

try:
    IS_GLUE_JOB = True
    from awsglue.utils import getResolvedOptions
except ModuleNotFoundError:
    IS_GLUE_JOB = False


def send_sns_notification(aws_session, topic_name, subject, message):
    """Send an SNS notification using the provided AWS session and topic name."""

    if IS_GLUE_JOB:
        sns_endpoint_url = (
            "https://vpce-083c550c684051773-u0gbdnwd.sns.us-east-1.vpce.amazonaws.com"
        )
        sns_client = aws_session.client("sns", endpoint_url=sns_endpoint_url)
    else:
        sns_client = aws_session.client("sns")

    sns_client.publish(TopicArn=topic_name, Subject=subject, Message=message)


class MSSQLFactReconciler:
    """A class to handle fact table reconciliation between MS SQL Server databases and EDW."""

    def __init__(
        self,
        aws_session,
        job_name,
        ms_sql_port,
        ms_sql_secret_name,
        fact_recon_s3_bucket,
        fact_recon_s3_key,
        sns_topic_arn,  # Added to pass the topic name
        databases_names,
        days_since_update=1,
        logger=None,
    ):
        self.logger = logger or self._setup_logger()
        self.logger.info(f"Initializing MSSQLFactReconciler for job: {job_name}")
        self.aws_session = aws_session
        self.job_name = job_name
        self.ms_sql_port = ms_sql_port
        self.ms_sql_secret_name = ms_sql_secret_name
        self.fact_recon_s3_bucket = fact_recon_s3_bucket
        self.fact_recon_s3_key = fact_recon_s3_key
        self.sns_topic_arn = sns_topic_arn  # Store the topic name
        self.databases_names = databases_names
        self.days_since_update = days_since_update
        self.ms_sql_clients = {}
        self.s3_client = self.aws_session.client("s3")
        self.batch_id = None
        self._setup_connections()

    def _setup_logger(self):
        """Configure a simple logger."""
        self.logger = logging.getLogger(__name__)
        self.logger.info("Setting up logger for MSSQLFactReconciler")
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        self.logger.info("Logger setup completed")
        return logger

    def _setup_connections(self):
        """Establish MS SQL Server connections."""
        self.logger.info("Starting database connection setup")
        try:
            secret = get_secret(self.aws_session, self.ms_sql_secret_name)
            self.logger.info("Retrieved MS SQL secret successfully")
            ms_sql_server = secret["server"]
            ms_sql_username = secret["username"]
            ms_sql_password = secret["password"]

            for db_key, db_name in self.databases_names.items():
                self.logger.info(f"Attempting connection to database: {db_name}")
                conn = pymssql.connect(
                    server=ms_sql_server,
                    port=self.ms_sql_port,
                    user=ms_sql_username,
                    password=ms_sql_password,
                    database=db_name,
                )
                self.ms_sql_clients[db_key] = conn
                self.logger.info(
                    f"Successfully established connection to database: {db_name}"
                )
        except Exception as e:
            self.logger.error(f"Failed to establish database connections: {str(e)}")
            if self.batch_id:
                self.insert_error_log(
                    "N/A", f"Connection setup failed: {str(e)}", "N/A"
                )
            raise
        self.logger.info("Database connections setup completed")

    def insert_batch_status(self, status):
        """Insert or update batch status in JC_EDW_FACT_RECON_BATCH_STATUS."""
        self.logger.info(
            f"Updating batch status to '{status}' for batch ID {self.batch_id}"
        )
        query = """
        MERGE JC_EDW_FACT_RECON_BATCH_STATUS AS target
        USING (VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP))
            AS source (BATCH_ID, JOB_NAME, STATUS, ROW_INSERT_TIMESTAMP, ROW_UPDATE_TIMESTAMP)
        ON target.BATCH_ID = source.BATCH_ID
        WHEN MATCHED THEN
            UPDATE SET STATUS = %s, ROW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (BATCH_ID, JOB_NAME, STATUS, ROW_INSERT_TIMESTAMP, ROW_UPDATE_TIMESTAMP)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        cursor = self.ms_sql_clients["ms_sql_db_edw"].cursor()
        try:
            cursor.execute(
                query,
                (
                    self.batch_id,
                    self.job_name,
                    status,
                    status,
                    self.batch_id,
                    self.job_name,
                    status,
                ),
            )
            self.ms_sql_clients["ms_sql_db_edw"].commit()
        except Exception as e:
            self.logger.error(
                f"Failed to update batch status to {status} for batch {self.batch_id}: {str(e)}"
            )
            raise
        finally:
            cursor.close()

    def insert_process_log(self, table_name, process_name, row_count=None):
        """Log process details in JC_EDW_FACT_RECON_PROCESS_LOG."""
        query = """
        INSERT INTO JC_EDW_FACT_RECON_PROCESS_LOG (BATCH_ID, TABLE_NAME, PROCESS_NAME, ROW_COUNT, ROW_INSERT_TIMESTAMP)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cursor = self.ms_sql_clients["ms_sql_db_edw"].cursor()
        try:
            cursor.execute(query, (self.batch_id, table_name, process_name, row_count))
            self.ms_sql_clients["ms_sql_db_edw"].commit()
        except Exception as e:
            self.logger.error(
                f"Failed to log process for {table_name} - {process_name}: {str(e)}"
            )
            self.insert_error_log(
                table_name,
                f"Process log insert failed: {str(e)}",
                query % (self.batch_id, table_name, process_name, row_count),
            )
            raise
        finally:
            cursor.close()

    def insert_error_log(self, table_name, error_message, error_sql):
        """Log errors in JC_EDW_FACT_RECON_ERROR_LOG."""
        self.logger.info(
            f"Logging error for table {table_name} - Message: {error_message}"
        )
        query = """
        INSERT INTO JC_EDW_FACT_RECON_ERROR_LOG (BATCH_ID, TABLE_NAME, ERROR_MESSAGE, ERROR_SQL, ROW_INSERT_TIMESTAMP)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cursor = self.ms_sql_clients["ms_sql_db_edw"].cursor()
        error_message = error_message[:8000]
        error_sql = error_sql[:8000]
        try:
            cursor.execute(query, (self.batch_id, table_name, error_message, error_sql))
            self.ms_sql_clients["ms_sql_db_edw"].commit()
            self.logger.info(
                f"Error logged - Table: {table_name}, Message: {error_message}"
            )
        except Exception as e:
            self.logger.error(f"Failed to log error for {table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def load_edw_queries(self):
        """Load reconciliation queries from CSV."""
        try:
            if IS_GLUE_JOB:
                self.logger.info(
                    f"Loading queries from S3: {self.fact_recon_s3_bucket}/{self.fact_recon_s3_key}"
                )
                response = self.s3_client.get_object(
                    Bucket=self.fact_recon_s3_bucket, Key=self.fact_recon_s3_key
                )
                df = pd.read_csv(response["Body"])
            else:
                local_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "edw_fact_mapping_queries.csv",
                )
                self.logger.info(f"Loading queries from local path: {local_path}")
                if not os.path.isfile(local_path):
                    raise FileNotFoundError(f"Query CSV not found at {local_path}")
                df = pd.read_csv(local_path)
                self.insert_process_log(
                    "N/A",
                    f"Loaded {len(df)} Tables's reconciliation queries from local path with updatetime offset as {self.days_since_update}",
                )
            self.logger.info(
                f"Successfully loaded {len(df)} table queries from edw_fact_mapping_queries.csv file"
            )
            return df
        except Exception as e:
            self.logger.error(f"Failed to load reconciliation queries: {str(e)}")
            self.insert_error_log("N/A", f"Query loading failed: {str(e)}", "N/A")
            raise

    def execute_count_query(self, connection, query, table_name):
        """Execute a count query."""
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            count = result[0] if result else None
            return count
        except Exception as e:
            self.logger.error(
                f"Count query execution failed for {table_name}: {str(e)}"
            )
            self.insert_error_log(
                table_name, f"Count query execution failed: {str(e)}", query
            )
            raise
        finally:
            cursor.close()

    def get_counts_for_row(self, row):
        """Get source and target counts for a row, continue on failure."""
        core_center = row["CORE_CENTER"].strip().lower()
        source_key = f"ms_sql_db_{core_center}"
        source_conn = self.ms_sql_clients.get(
            source_key, self.ms_sql_clients["ms_sql_db_edw"]
        )
        target_conn = self.ms_sql_clients["ms_sql_db_edw"]
        table_name = row["TABLE_NAME"]
        source_query = row["SOURCE_QUERY"]
        target_query = row["TARGET_QUERY"]

        if "DateAdd(D, ?, GetDate())" in source_query:
            source_query = source_query.replace("?", str(self.days_since_update))
            self.logger.info(
                f"Adjusted source query for {table_name} with {self.days_since_update} in the query"
            )

        gw_snapshot_count = None
        edw_count = None
        self.logger.info(f"Executing count query for table {table_name}")
        try:
            gw_snapshot_count = self.execute_count_query(
                source_conn, source_query, table_name
            )
            self.insert_process_log(
                table_name,
                f"Retrieved source count: {gw_snapshot_count}",
                gw_snapshot_count,
            )
        except Exception as e:
            self.logger.error(f"Failed to get source count for {table_name}: {str(e)}")

        try:
            edw_count = self.execute_count_query(target_conn, target_query, table_name)
            self.insert_process_log(
                table_name, f"Retrieved target count: {edw_count }", edw_count
            )
        except Exception as e:
            self.logger.error(f"Failed to get target count for {table_name}: {str(e)}")

        self.logger.info(
            f"Finished count retrieval for {table_name} - Source count: {gw_snapshot_count}, Target count: {edw_count }"
        )
        return pd.Series(
            {"GW_SNAPSHOT_COUNT": gw_snapshot_count, "EDW_COUNT": edw_count}
        )

    def get_next_batch_id(self):
        """Fetch next batch ID."""
        self.logger.info("Fetching next batch ID for the job")
        cursor = self.ms_sql_clients["ms_sql_db_edw"].cursor()
        try:
            cursor.execute("SELECT NEXT VALUE FOR JC_SEQ_EDW_RECON_BATCH_ID;")
            batch_id = cursor.fetchone()[0]
            self.logger.info(f"Generated new batch ID: {batch_id}")
            return batch_id
        except Exception as e:
            self.logger.error(f"Failed to generate batch ID: {str(e)}")
            self.insert_error_log(
                "N/A",
                f"Batch ID generation failed: {str(e)}",
                "SELECT NEXT VALUE FOR JC_SEQ_EDW_RECON_BATCH_ID;",
            )
            raise

    def insert_results(self, df):
        """Insert results into JC_EDW_FACT_RECON_RESULT, and fail the job if any row insert fails."""
        cursor = self.ms_sql_clients["ms_sql_db_edw"].cursor()
        query = """
        INSERT INTO JC_EDW_FACT_RECON_RESULT (CORE_CENTER, TABLE_NAME, GW_SNAPSHOT_COUNT, EDW_COUNT, COUNT_DIFFERENCE, RECON_RESULT, BATCH_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        successful_inserts = 0
        failed_inserts = []

        for _, row in df.iterrows():
            params = (
                row["CORE_CENTER"],
                row["TABLE_NAME"],
                row["GW_SNAPSHOT_COUNT"],
                row["EDW_COUNT"],
                row["COUNT_DIFFERENCE"],
                row["RECON_RESULT"],
                self.batch_id,
            )
            try:
                cursor.execute(query, params)
                successful_inserts += 1
                self.insert_process_log(
                    row["TABLE_NAME"],
                    f"Inserted reconciliation result - Source: {row['GW_SNAPSHOT_COUNT']}, Target: {row['EDW_COUNT']}, Diff: {row['COUNT_DIFFERENCE']}",
                    1,
                )
            except Exception as e:
                error_msg = f"Failed to insert result for {row['TABLE_NAME']}: {str(e)}"
                self.logger.error(error_msg)
                self.insert_error_log(
                    row["TABLE_NAME"],
                    f"Result insertion failed: {str(e)}",
                    query % params,
                )
                failed_inserts.append(row["TABLE_NAME"])

        self.ms_sql_clients["ms_sql_db_edw"].commit()
        cursor.close()

        self.logger.info(
            f"Finished inserting results - {successful_inserts} of {len(df)} successfully"
        )

        # Raise exception if any inserts failed
        if failed_inserts:
            failed_list = ", ".join(failed_inserts)
            raise Exception(
                f"Job failed due to insert errors in result table for tables: {failed_list}"
            )

    def notify_failed_recon_tables_from_df(self, df):
        """Filters DataFrame for failed reconciliations and sends SNS notification."""
        failed_df = df[df["RECON_RESULT"] == "failed"]
        if failed_df.empty:
            self.logger.info(
                f"No failed reconciliation results found for batch {self.batch_id}. No SNS notification sent."
            )
            return

        failed_tables = failed_df["TABLE_NAME"].unique().tolist()
        message_body = (
            f"Hi Team,\n\n"
            f"The {self.job_name} job has failed \n"
            f"The following table(s) have RECON_RESULT='failed' for batch {self.batch_id}:\n"
        )
        for table in failed_tables:
            message_body += f"  • {table}\n"
        message_body += "\nPlease investigate these failures.\n\nThanks\n"
        subject_line = "EDW Fact Reconciliation - Found mismatch in counts"
        try:
            if self.sns_topic_arn:
                send_sns_notification(
                    self.aws_session, self.sns_topic_arn, subject_line, message_body
                )
                failed_tables_str = "\n".join(
                    [f"  - {table}" for table in failed_tables]
                )
                self.logger.info(
                    f"Constructed the SNS, The topic name:\n {subject_line}\n The message is:\n {message_body}"
                )
                self.logger.info(
                    f"SNS notification sent for {len(failed_tables)} failed table(s) in batch {self.batch_id}:\n{failed_tables_str}"
                )
            else:
                self.logger.info("No SNS topic name provided. Skipping notification.")
        except Exception as e:
            self.logger.error(f"Failed to publish SNS notification: {str(e)}")
            self.insert_error_log("N/A", f"SNS notification failure: {str(e)}", "N/A")
            raise

    def run(self):
        """Run the reconciliation process."""
        try:
            self.batch_id = self.get_next_batch_id()
            self.insert_batch_status("started")
            df = self.load_edw_queries()

            counts_df = df.apply(self.get_counts_for_row, axis=1)
            df = pd.concat([df, counts_df], axis=1)

            df["BATCH_ID"] = self.batch_id
            self.logger.info("Calculating count differences and reconciliation results")
            df["COUNT_DIFFERENCE"] = df["GW_SNAPSHOT_COUNT"].fillna(0) - df[
                "EDW_COUNT"
            ].fillna(0)
            df["RECON_RESULT"] = df["COUNT_DIFFERENCE"].apply(
                lambda x: "successful" if x == 0 else "failed"
            )
            self.insert_results(df)
            self.logger.info(
                f"Trying to send an SNS Notification if there are failed results"
            )
            self.notify_failed_recon_tables_from_df(df)  # Call SNS notification
            self.insert_batch_status("completed")

            # --- Final Summary Log ---
            total_tables = len(df)
            successful_queries = (
                df[["GW_SNAPSHOT_COUNT", "EDW_COUNT"]].dropna().shape[0]
            )
            matched_tables = df[df["RECON_RESULT"] == "successful"].shape[0]
            mismatched_tables = df[df["RECON_RESULT"] == "failed"].shape[0]

            summary_message = (
                f"\n--- Final Reconciliation Summary ---\n"
                f"Total Tables Processed     : {total_tables}\n"
                f"Tables Successfully Queried: {successful_queries}\n"
                f"Count Match (Successful)   : {matched_tables}\n"
                f"Count Mismatch (Failed)    : {mismatched_tables}\n"
                f"--------------------------------------"
            )
            self.logger.info(summary_message)

            return df
        except Exception as e:
            self.logger.error(
                f"Job execution failed but continuing to mark as failed: {str(e)}"
            )
            if self.batch_id:
                self.insert_batch_status("failed")
                self.insert_error_log("N/A", f"Job execution failed: {str(e)}", "N/A")
            raise

    def close_connections(self):
        """Close all connections."""
        self.logger.info("Starting to close database connections")
        for db_key, conn in self.ms_sql_clients.items():
            try:
                conn.close()
                self.logger.info(f"Closed connection to {self.databases_names[db_key]}")
            except Exception as e:
                self.logger.error(
                    f"Failed to close connection to {self.databases_names[db_key]}: {str(e)}"
                )
                if self.batch_id:
                    self.insert_error_log(
                        "N/A", f"Connection closure failed: {str(e)}", "N/A"
                    )
        self.logger.info("Finished closing database connections")

    def __del__(self):
        """Destructor to close connections."""
        self.logger.info("Destructor called, closing connections")
        self.close_connections()
        self.logger.info("Destructor completed")


def main(
    job_name,
    aws_session,
    ms_sql_port,
    ms_sql_secret_name,
    fact_recon_s3_bucket,
    fact_recon_s3_key,
    sns_topic_arn,
    databases_names,
    days_since_update,
):
    """Main function."""
    logger = logging.getLogger(__name__)
    logger.info("EDW Fact Reconciliation Job has Started.............")

    reconciler = MSSQLFactReconciler(
        aws_session=aws_session,
        job_name=job_name,
        ms_sql_port=ms_sql_port,
        ms_sql_secret_name=ms_sql_secret_name,
        fact_recon_s3_bucket=fact_recon_s3_bucket,
        fact_recon_s3_key=fact_recon_s3_key,
        sns_topic_arn=sns_topic_arn,
        databases_names=databases_names,
        days_since_update=days_since_update,
    )
    reconciler.run()
    logger.info("EDW Fact Reconciliation Job Completed Successfully")


if __name__ == "__main__" and IS_GLUE_JOB:
    _args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "ms_sql_db_pc",
            "ms_sql_db_bc",
            "ms_sql_db_cc",
            "ms_sql_db_cm",
            "ms_sql_db_edw",
            "ms_sql_port",
            "ms_sql_secret_name",
            "fact_recon_s3_bucket",
            "fact_recon_s3_key",
            "days_since_update",
            "sns_topic_arn",
        ],
    )
    _aws_session = AWSSession(region_name="us-east-1")
    main(
        job_name=_args["JOB_NAME"],
        aws_session=_aws_session,
        ms_sql_port=int(_args["ms_sql_port"]),
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        fact_recon_s3_bucket=_args["fact_recon_s3_bucket"],
        fact_recon_s3_key=_args["fact_recon_s3_key"],
        sns_topic_arn=None
        if _args["sns_topic_arn"] == "N/A"
        else _args["sns_topic_arn"],
        databases_names={
            "ms_sql_db_pc": _args["ms_sql_db_pc"],
            "ms_sql_db_bc": _args["ms_sql_db_bc"],
            "ms_sql_db_cc": _args["ms_sql_db_cc"],
            "ms_sql_db_cm": _args["ms_sql_db_cm"],
            "ms_sql_db_edw": _args["ms_sql_db_edw"],
        },
        days_since_update=int(_args["days_since_update"]),
    )

elif __name__ == "__main__" and not IS_GLUE_JOB:
    _args = {
        "JOB_NAME": "edw_fact_recon_mssql",
        "ms_sql_db_pc": "PolicyCenterR1_ST",
        "ms_sql_db_bc": "BillingCenterR1_ST",
        "ms_sql_db_cc": "ClaimCenterR1_ST",
        "ms_sql_db_cm": "ContactManagerR1_ST",
        "ms_sql_db_edw": "EDW_ST",
        "ms_sql_port": "59796",
        "ms_sql_secret_name": "arn:aws:secretsmanager:us-east-1:058264312281:secret:aws-use1-dev-smr-ohdwdbs0024va-0001-mbDya0",
        "fact_recon_s3_bucket": "encova-aws-use1-dev1-s3-cda-code-0001",
        "fact_recon_s3_key": "edw_fact_recon/edw_fact_mapping_queries.csv",
        "days_since_update": "1",
        "sns_topic_arn": "arn:aws:sns:us-east-1:058264312281:sns-cda-dev1-glue-job-0001",
    }
    _aws_session = get_assumed_role_session(
        profile_name="dev",
        region_name="us-east-1",
        role_arn="arn:aws:iam::058264312281:role/aws-gbl-dev1-role-cda-gw-access-0001",
        role_session_name="AssumeRoleSession1",
    )
    main(
        job_name=_args["JOB_NAME"],
        aws_session=_aws_session,
        ms_sql_port=int(_args["ms_sql_port"]),
        ms_sql_secret_name=_args["ms_sql_secret_name"],
        fact_recon_s3_bucket=_args["fact_recon_s3_bucket"],
        fact_recon_s3_key=_args["fact_recon_s3_key"],
        sns_topic_arn=None
        if _args["sns_topic_arn"] == "N/A"
        else _args["sns_topic_arn"],
        databases_names={
            "ms_sql_db_pc": _args["ms_sql_db_pc"],
            "ms_sql_db_bc": _args["ms_sql_db_bc"],
            "ms_sql_db_cc": _args["ms_sql_db_cc"],
            "ms_sql_db_cm": _args["ms_sql_db_cm"],
            "ms_sql_db_edw": _args["ms_sql_db_edw"],
        },
        days_since_update=int(_args["days_since_update"]),
    )
