# Snowflake to On-Prem Logic

*This Python script is designed to transfer data from Snowflake to Microsoft SQL Server (MS SQL) databases. It extracts data from Snowflake, stages it in an S3 bucket, and utilizes Apache Spark for processing, with JDBC connections to MS SQL for final data loading. The script supports incremental updates and soft deletes to ensure data consistency and integrity across systems. Structured to run as an AWS Glue job or locally, it includes robust features such as error logging, batch processing, and audit validation. It also manages database connections, executes SQL queries, and handles specialized data types like geography columns.*

### Table of Contents
<!-- TOC -->

  * [1. Retrieve Stage Name from Snowflake](#1-retrieve-stage-name-from-snowflake)
  * [2. Identify Tables with Incremental Data](#2-identify-tables-with-incremental-data)
  * [3. Fetch Incremental and Soft Delete Record Counts](#3-fetch-incremental-and-soft-delete-record-counts)
  * [4. Export Incremental Data to External Snowflake Stage](#4-export-incremental-data-to-external-snowflake-stage)
  * [5. Handle Failed Exports](#5-handle-failed-exports)
  * [6. Load Parquet Files into On-Prem SQL Tables](#6-load-parquet-files-into-on-prem-sql-tables)
  * [7. Process Incremental and Soft Delete Records](#7-process-incremental-and-soft-delete-records)
  * [8. Handle Soft Delete Records](#8-handle-soft-delete-records)
  * [9. Maintain Audit and Validation Tables](#9-maintain-audit-and-validation-tables)
  * [10. Cleanup and Final Updates](#10-cleanup-and-final-updates)
  * [11. Exception Handling and Self-Healing](#11-exception-handling-and-self-healing)
  * [12. Override Conditions for Table Load from Snowflake to MSSQL](#12-override-conditions-for-table-load-from-snowflake-to-mssql)
<!-- TOC -->

## 1. Retrieve Stage Name from Snowflake

It is used to fetch the external Snowflake stage path (S3-backed) using the DESCRIBE STAGE command.
This path is essential for exporting Parquet files that can be accessed by Spark for downstream processing.

- Fetch the stage name from Snowflake.
  ```python
  cursor.execute(
              """
              SELECT "property_value"
              FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
              WHERE "parent_property" = 'STAGE_LOCATION';
              """
          )
          (stage_locations_str,) = cursor.fetchone()
          stage_locations = json.loads(stage_locations_str)
  ```


## 2. Identify Tables with Incremental Data
Compares the latest batch ID from Snowflake with the on-prem load status to identify tables with new data. This ensures that only tables with fresh, unprocessed data are selected, avoiding unnecessary loading.
- Retrieve the table list and batch ID that received incremental data from the process log table in Snowflake.
  ```python
  query = f"""
                  SELECT CTL.TABLE_NAME,
                        MAX(LD_STS.BATCH_ID) AS LATEST_BATCH_ID
                  FROM ETL_CTRL.JC_SF_TO_MSSQL_TABLES AS CTL
                          INNER JOIN ETL_CTRL.JC_MRG_{core_center.upper()}_LOAD_STATUS AS LD_STS
                                ON LOWER(CTL.TABLE_NAME) = LOWER(LD_STS.TABLE_NAME)
                  WHERE LD_STS.STATUS = 'completed'
                  GROUP BY CTL.TABLE_NAME
              """
  ```

- Retrieve the max batch ID for these tables from the on-prem load status table.
  ```python
  query = f"""
                  SELECT ISNULL(MAX(SF_BATCH_ID), 0) AS last_successful_sf_table_batch_id
                  FROM {load_status_table_name}
                  WHERE STATUS = 'completed'
                  AND TABLE_NAME = '{table_name}'
              """
  ```

- Compare the batch ID values:
    - If the batch ID from the Snowflake table is greater than the batch ID from the on-prem table, qualify that table name to loading.
      ```python
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
      ```

## 3. Fetch Incremental and Soft Delete Record Counts
Counts records that are newly added (incremental) or marked as deleted (soft delete) since the last batch. This is done for accurate audit logging, reconciliation, and to plan downstream deletion and insert operation.

- For each table in the identified list identify  the incremental and soft delete record count.

    ```python
    export_count_query = f"""
                    SELECT NVL(COUNT_IF(GW_DELETE_FLAG <> 'Y'), 0) AS COUNT,
                        NVL(COUNT_IF(GW_DELETE_FLAG = 'Y'), 0)  AS SOFT_DELETE_COUNT
                    FROM {self.schema_name}.{self.base_table_name}
                    WHERE BATCHID > {self.last_successful_sf_table_batch_id}
                    --and GW_DELETE_FLAG <> 'Y' #TODO - this filter beats the purpose of fetching SOFT_DELETE_COUNT. Need to discuss
                """
    ```

## 4. Export Incremental Data to External Snowflake Stage
Unloads identified records from qualified tables into Parquet files stored in an external S3 stage using Snowflake’s COPY INTO command. This allows Snowflake to efficiently unload the data and turn the warehouses down when complete.
- Export incremental data as Parquet files to an external Snowflake stage location in parallel.
  ```python
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
  ```

- Retrieve the exported record count.
  ```python
  if result:
              self.s3_unload_record_count = result[0][0]
  ```

- Compare the exported record count with the sum of incremental and soft delete record counts.

    - If they do not match, raise an exception.
        ```python
      if self.s3_unload_record_count != total_count:
                      error_desc = (
                          f"Snowflake Table {self.base_table_name} Record Count {total_count} for Batchid "
                          f"{self.last_successful_sf_table_batch_id} is not matching with Records "
                          f"unloaded into S3 {self.s3_unload_record_count}"
                      )
                      self.logger.error(error_desc)
                      raise Exception(error_desc)
      ```

## 5. Handle Failed Exports
Captures tables that failed during the export and logs them into the error table with causes.
This prevents partial loads and ensures the pipeline either completes fully or fails transparently for reprocessing.
- If a table fails to export to the S3 bucket as a Parquet file:
    - Add the table name to the failed table list.
      ```python
      try:
                      future.result()
                  except Exception as exc:
                      failed_tables.append((table, exc))
      ```
    - After exporting all tables, check the failed table list:
        - If any table failed, log the error in the error log table and raise an exception.
          ```python
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
          ```

## 6. Load Parquet Files into On-Prem SQL Tables

Reads unloaded Parquet files into Spark DataFrames and prepares them for JDBC-based inserts.
This enables efficient bulk loading of data from Snowflake into MS SQL Server while Saving Snowflake credits.

- After exporting data to S3 as Parquet files:
    - Load `BATCH_STATUS` table in on-prem with status `Started`.
      ```python
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
      ```

    - Use the table list to fetch Parquet files from Snowflake stage.
    - Read Parquet files into a DataFrame.
      ```python
      df = spark.read.parquet(self.s3_path)
      ```

    - Check for the `batchid` column:
        - If found, retrieve the max batch ID from the DataFrame.
          ```python
          if "batchid" in [column.lower() for column in df.columns]:
              self.current_sf_table_batch_id = df.agg(
                  spark_max("batchid").alias("max_batch_id")
              ).collect()[0]["max_batch_id"]
              self.logger.info(
                  f"Extracted latest BATCH_ID for table '{self.base_table_name}' from spark dataframe is: {self.current_sf_table_batch_id}"
              )
              return df
          ```
        - If not found, raise an exception.
          ```python

          else:
              self.logger.warning(
                  f"BATCHID column not found in data for table '{self.base_table_name}'"
              )
              raise Exception(
                  f"BATCHID column not found in data for table '{self.base_table_name}'"
              )
          ```

## 7. Process Incremental and Soft Delete Records

Splits data into incremental and soft delete DataFrames and deletes old records from on-prem tables.
This maintains SCD Type-1 behavior—ensuring only the latest version of each record is present after inserts.

- Extract separate DataFrames for:
    - Incremental records
    - Soft delete records
    - Complete records
      ```python
      df_incremental = df.filter(df["GW_DELETE_FLAG"] != "Y")
          df_soft_delete = df.filter(df["GW_DELETE_FLAG"] == "Y")
      ```

- Update `LOAD_STATUS` table in on-prem with status `Started`.
  ```python
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
  ```
- If incremental records exist:
    - Retrieve IDs from the DataFrame and delete matching records in on-prem to maintain SCD Type-1.
      ```python
      query = f"DELETE FROM {self.table_name} WHERE ID IN ({batch_ids_str})"
      ```
    - Validate column compatibility:
        - Fetch column names and data types from on-prem SQL tables.
        - Extract column names from the DataFrame.
        - Identify common columns and create a list.
          ```python
          ms_sql_column_map = self.get_ms_sql_column_map(
              spark=spark,
              ms_sql_jdbc_url=ms_sql_jdbc_url,
              ms_sql_jdbc_properties=ms_sql_jdbc_properties,
          )

          # Identify common columns
          df_columns = set([column.lower() for column in df.columns])
          ms_sql_columns = set(ms_sql_column_map.keys())
          common_columns = df_columns.intersection(ms_sql_columns)
          ```
- Handle Geography Columns:
    - If any geography columns are found, extract `SRID` values separately and load them accordingly.
      ```python
      geo_df = geo_df.withColumn(
              col_name,
              concat(lit("0x"), geo_df[col_name].getField("wkb")),
          )
          return geo_df
      ```
- Handle Different Data Types:
    - Convert `VARCHAR`, `NVARCHAR`, `CHAR`, `INT`, `BIGINT`, `DECIMAL`, `FLOAT`, `DATE`, etc., to match SQL Server
      column types.
      ```python
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
      ```
- Load Data into On-Prem Tables:
    - Load non-geography data using JDBC connection.
      ```python
      df_incremental.write.jdbc(
              url=ms_sql_jdbc_url,
              table=self.table_name,
              mode="append",
              properties=ms_sql_jdbc_properties,
          )
      ```
    - Load geography data into a stage table first, then transform and update the main table.
      ```python
      stg_table_name = f"JC_MSSQL_{self.core_center.upper()}_STG"

          # Write geography-related data to the staging table
          geo_df.write.jdbc(
              url=ms_sql_jdbc_url,
              table=stg_table_name,
              mode="append",
              properties=ms_sql_jdbc_properties,
          )
      ```

## 8. Handle Soft Delete Records

Deletes records marked as GW_DELETE_FLAG = 'Y' from the on-prem SQL Server tables.

- If soft delete records exist:
    - Delete records from the on-prem SQL table using IDs.
      ```python
      query = f"DELETE FROM {self.table_name} WHERE ID IN ({batch_ids_str})"
      ```
    - Capture deleted record count for audit purposes.
      ```python
      self.load_process_log(
                      current_mssql_job_batch_id=self.current_mssql_job_batch_id,
                      current_sf_table_batch_id=self.current_sf_table_batch_id,
                      process_name=f"Deleted Soft delete records from the table",
                      table_name=self.table_name,
                      row_count=self.deleted_soft_delete_record_count,
                  )
      ```

## 9. Maintain Audit and Validation Tables

Captures and logs record counts from Snowflake, Spark, and MS SQL to an audit table.
This step ensures full traceability, reconciliation accuracy during loading.

- Capture record counts at every step:
    - Exported record count to S3
    - Record count when reading from Snowflake stage
    - Deleted records count (SCD Type-1)
    - Inserted records count
    - Deleted soft delete records count
- Use these counts to update the audit validation table.
  ```python
          df_incremental_rec_count = len(df_id_list_incremental)
          df_soft_delete_rec_count = len(df_id_list_soft_delete)

          # Fetch inserted incremental record count from on-prem database
          inserted_incremental_rec_count = self.fetch_ms_sql_inserted_count()

          # Compare record counts for discrepancies
          sf_vs_df_incremental_rec_count = (
              self.sf_incremental_record_count != df_incremental_rec_count
          )
          df_vs_ms_sql_incremental_rec_count = (
              df_incremental_rec_count != inserted_incremental_rec_count
          )
          sf_vs_df_soft_delete_record_count = (
              self.sf_soft_delete_record_count != df_soft_delete_rec_count
          )

          # Determine reconciliation status and log errors if discrepancies exist
          reconciliation_status = "Passed"
  ```

## 10. Cleanup and Final Updates

Truncates staging tables and deletes exported Parquet files from S3 after successful load.
This keeps the environment clean, prevents storage bloat, and avoids duplicate processing on future runs.

- After completion:
    - Delete temporary staging tables used for handling geography columns.
      ```python
      query = f"TRUNCATE TABLE {stg_table_name}"
          try:
              self.mssql_client.execute_query(query)
      ```
    - Delete Parquet files created in Snowflake stage.
      ```python
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
      ```
    - Update the `BATCH_STATUS` table with status `Completed`.
      ```python
      query = f"""
              UPDATE {status_table_name}
              SET ROW_UPDATE_TIMESTAMP = GETDATE(),
                  STATUS = '{status}'
              WHERE MSSQL_BATCH_ID = {self.current_mssql_job_batch_id};
          """
      ```

## 11. Exception Handling and Self-Healing

Captures and logs all errors, updates status tables, and stops the job gracefully on failure.
Failed tables will be retried automatically on the next run using their last successful batch checkpoint.

- If an exception occurs at any step:
    - Update `LOAD_STATUS` and `BATCH_STATUS` tables with status `Failed`.
    - Fail the process immediately.
- Self-healing mechanism:
    - On the next run, only `Completed` statuses are considered to fetch batch IDs.
    - If any table failed in the previous run, its batch ID will be reprocessed automatically from next run.


## 12. Override Conditions for Table Load from Snowflake to MSSQL

## Context

By default, the system performs **incremental loads** using the `BATCHID` and `gwcbi___operation` flags. However, certain situations require overriding this logic to perform **full loads** or **partial re-loads**. These override instructions are defined in the control table:

```
ETL_CTRL.JC_SF_T0_MSSQL_OVERRIDE
```

---

## Override Modes

The Snowflake-to-MSSQL data pipeline supports multiple override modes to accommodate various loading scenarios beyond the standard incremental strategy:

* **Full Load**
  If the `LOAD_ALL_IDS` flag is set to `TRUE` in the override control table, a full load is triggered. This bypasses both the `BATCHID` and `gwcbi___operation` filters.
  Resulting SQL: *(no WHERE clause applied)*

* **Bootstrap Incremental Load**
  If `LOAD_ALL_INCREMENTAL_IDS = TRUE`, the pipeline performs a bootstrap incremental load by applying the following filter:

  ```sql
  WHERE gwcbi___operation != 0
  ```

  It ignores the `BATCHID`.

* **Selective Record Load**
  If `LOAD_ALL_INCREMENTAL_IDS = FALSE` and `IDS_TO_LOAD` is populated with specific IDs (e.g., `'12, 45, 67'`), only those records are loaded:

  ```sql
  WHERE ID IN (12, 45, 67)
  ```

  This also bypasses both `BATCHID` and `gwcbi___operation`.

* **Default (Incremental Load)**
  If none of the above override flags are provided, the pipeline performs a regular incremental load using:

  ```sql
  WHERE BATCHID > {last_successful_batch_id} AND gwcbi___operation != 0
  ```

---

## How to Start an Override Load — Step-by-Step

### 1. Update the Control Table

Add or update a record in the `ETL_CTRL.JC_SF_T0_MSSQL_OVERRIDE` table with the following:

* `TABLE_NAME`: Name of the target table
* `CORE_CENTER`: e.g., `cc`, `pc`, `bc`, `cm`

Set one or more of these flags as needed:

* `LOAD_ALL_IDS = TRUE` for full load
* `LOAD_ALL_INCREMENTAL_IDS = TRUE` for bootstrap incremental
* `IDS_TO_LOAD = '12, 45, 67'` for selective record loading

### 2. Enable Override Mode in the Script

Set the boolean flag `is_override_tables_load = True` inside your main driver script:

* In **Glue**, this is a global variable just above the `main()` function call.
* In **local mode**, it is inside the `__main__` block.

```python
is_override_tables_load = True
```

### 3. Run the Job

Execute your Glue job or run it locally. The pipeline will now consider override values from the control table.

---

## How to Turn Off Override Load

### 1. Clear Override Flags from the Control Table

Either delete the entry from `JC_SF_T0_MSSQL_OVERRIDE` for that table and core center, or set all three override flags to `NULL`:

```sql
UPDATE ETL_CTRL.JC_SF_T0_MSSQL_OVERRIDE
SET LOAD_ALL_IDS = NULL,
    LOAD_ALL_INCREMENTAL_IDS = NULL,
    IDS_TO_LOAD = NULL
WHERE TABLE_NAME = 'YOUR_TABLE_NAME'
  AND CORE_CENTER = 'cc';
```

### 2. Disable Override Mode in the Script

Set `is_override_tables_load = False` in your driver script:

```python
is_override_tables_load = False
```

### 3. Run the Job Again

The pipeline will now follow the default incremental logic using `BATCHID` and `gwcbi___operation`.

---
