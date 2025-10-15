import os
import sys
from pathlib import Path

from boto3.session import Session as AWSSession
from pyspark.sql import SparkSession

REPO_DIR = Path(__file__).parent.parent.parent


def get_local_spark_session(aws_session: AWSSession) -> SparkSession:
    creds = aws_session.get_credentials()
    f_creds = creds.get_frozen_credentials()

    if "HADOOP_HOME" not in os.environ:
        raise Exception(
            "Hadoop needs to be installed locally. See the documentation on local Spark environment setup."
        )

    # Set environment variables for PySpark
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Initialize Spark Session
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("MS SQL Server and S3 Access")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.worker.connection.timeout", "1000")
        .config("spark.python.worker.reuse", "false")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config(
            "spark.jars",
            ",".join(
                [
                    rf"{REPO_DIR}\jar_files\mssql-jdbc-12.8.1.jre11.jar",
                    rf"{REPO_DIR}\jar_files\hadoop-aws-3.2.0.jar",
                    rf"{REPO_DIR}\jar_files\aws-java-sdk-bundle-1.11.375.jar",
                ]
            ),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", f_creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", f_creds.secret_key)
        .config("spark.hadoop.fs.s3a.session.token", f_creds.token)
        .config(
            "spark.driver.extraClassPath",
            ";".join(
                [
                    rf"{REPO_DIR}\jar_files\mssql-jdbc-12.8.1.jre11.jar",
                    rf"{REPO_DIR}\jar_files\hadoop-aws-3.2.0.jar",
                    rf"{REPO_DIR}\jar_files\aws-java-sdk-bundle-1.11.375.jar",
                ]
            ),
        )
        .getOrCreate()
    )

    # Access the Hadoop configuration
    # noinspection PyProtectedMember,PyUnresolvedReferences
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Set AWS credentials directly in Hadoop configuration
    hadoop_conf.set("fs.s3a.access.key", f_creds.access_key)
    hadoop_conf.set("fs.s3a.secret.key", f_creds.secret_key)
    hadoop_conf.set("fs.s3a.session.token", f_creds.token)
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark
