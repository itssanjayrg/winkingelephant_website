import logging

import structlog
from snowflake.connector import SnowflakeConnection, connect


def get_snowflake_connection(
    logger: structlog.BoundLogger,
    snowflake_account: str,
    snowflake_user: str,
    snowflake_database: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    authorization_token: str,
) -> SnowflakeConnection:
    # Log the start of the connection process
    logger.info("Attempting to connect to Snowflake...")

    snowflake_params = {
        "account": snowflake_account,
        "user": snowflake_user,
        "role": snowflake_role,
        "database": snowflake_database,
        "schema": "ETL_CTRL",
        "warehouse": snowflake_warehouse,
        "authenticator": "oauth",
        "token": authorization_token,
    }

    try:
        # Log the connection attempt
        logger.info(
            f"Connecting to Snowflake: account={snowflake_params['account']}, "
            f"user={snowflake_params['user']}, warehouse={snowflake_params['warehouse']}"
        )

        # Create the Snowflake connection
        conn = connect(**snowflake_params)

        # Log success
        logger.info("Snowflake connection established successfully.")

        return conn

    except Exception as exc:
        # Log any errors that occur during connection
        logger.exception(f"Failed to connect to Snowflake")
        raise exc
