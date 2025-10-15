
# Database Schema Modification for Migration Readiness

This document outlines the necessary changes to the database schema to ensure it is prepared for migration.
These modifications include:

- Dropping specific referential integrity constraints to facilitate data movement.
- Adding audit columns to EDW (Enterprise Data Warehouse) tables.
- Creating essential Job Control (JC) tables for tracking and logging purposes.

---

## Table of Contents

1. [Objective](#objective)
2. [Schema Changes](#schema-changes)
   - [1. Drop Referential Integrity Constraints](#1-drop-referential-integrity-constraints)
   - [2. Drop Null Constraints](#2-drop-null-constraints)
   - [3. Add Audit Columns to EDW Tables](#3-add-audit-columns-to-edw-tables)
   - [4. Alter Precision of PUBLICID Column](#4-alter-precision-of-publicid-column)
   - [5. Create Job Control (JC) Tables](#5-create-job-control-jc-tables)
3. [Testing and Validation](#testing-and-validation)

---

## Objective

The objective of these schema changes is to **prepare the database for a seamless data migration** from the cloud to the on-premises MS SQL Server Snapshot database. By adjusting constraints and enhancing tables with audit capabilities, we aim to ensure data integrity, facilitate efficient data movement, and maintain comprehensive tracking of the migration process.

---

## Schema Changes

### 1. Drop Referential Integrity Constraints

As part of the CDA (Cloud Data Access) migration, data is loaded from Snowflake to the On-Premises MS-SQL Server Snapshot database. Primary and Foreign Keys Constraints have been identified as potential obstacles to migration. Referential integrity constraints, such as primary and foreign key constraints in SQL Server, can pose challenges during migration. Therefore, **these constraints need to be dropped before the migration process begins**.

**Note:** It is the responsibility of the DBAs to validate the requirement before execution, ensuring it aligns with the migration needs.

**Note:** The above process must be executed for each core center [PC, BC, CC, CM]

---
### 2. Drop Null constraints
To ensure smooth data migration and avoid issues with nullable columns, it is necessary to drop the null constraints on specific columns in the identified tables. Below is the list of tables and columns where the null constraints need to be removed.

Below is the list of tables and the columns from which the null constraints should be removed:

- **Table: pc_account**
  - Frozen

- **Table: pc_policyperiod**
  - ValidQuote
  - DoNotPurge
  - CustomBilling

- **Table: pcx_gl7classcodebasis_gle**
  - Auditable

- **Table: pcx_gl7classcode_gle**
  - Auditable

- **Table: pc_policy**
  - DoNotPurge

- **Table: bc_invoiceitem**
  - PrimaryPaidCommission
  - PrimaryPaidCommission_cur
  - PrimaryCmsnPayableAmount
  - PrimaryCmsnPayableAmount_cur

- **Table: bc_account**
  - PolicyLevelPaymentOption

- **Table: bc_producer**
  - SuspendNegativeAmounts

- **Table: bc_producerstatement**
  - Retired
  - StatementNumber


To drop the NULL constraint from a column in SQL, you can use the following syntax:

```sql
ALTER TABLE table_name
ALTER COLUMN column_name DROP NOT NULL;
```

### Example:
To drop the NULL constraint from the **Frozen** column in the **pc_account** table:

```sql
ALTER TABLE pc_account
ALTER COLUMN Frozen DROP NOT NULL;
```
---

### 3. Add Audit Columns to EDW Tables

As part of the migration, two additional columns need to be added to the existing EDW tables: `BATCHID` and `ROW_UPDATE_TIMESTAMP`. These columns are **not part of the control tables** created in the next section.

> **Note:** Ensure that the order of operations is strictly followed. If the control tables are created first, and then the audit columns are added, these audit columns will be replicated to the control tables. This replication is not desired and may lead to failures in the process.

A list of tables to be excluded from adding these columns is maintained. The script ensures that **excluded tables are not modified**.

```sql
DECLARE @tableName NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);

DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
AND TABLE_NAME NOT LIKE '%_SRG' -- Exclude tables starting with '_SRG'
AND TABLE_NAME NOT LIKE '%JC_ONPREM%'

OPEN table_cursor;

FETCH NEXT FROM table_cursor INTO @tableName;

WHILE @@FETCH_STATUS = 0
BEGIN

	SET @sql = 'ALTER TABLE ' + QUOTENAME(@tableName) + '
                ADD BATCHID INT NOT NULL,
                    ROW_UPDATE_TIMESTAMP DATETIME2(7) DEFAULT GETDATE()';


    EXEC sp_executesql @sql;

    FETCH NEXT FROM table_cursor INTO @tableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;
```

**Note:** The above query must be executed once per DB for each core center [PC, BC, CC, CM]

---

### 4. Alter Precision of PUBLICID Column

To prepare for migration, the precision of the `PUBLICID` column must be increased from **20 to 64** for all tables in the **BillingCenter**, **ClaimCenter** and **ContactManager**  Databases.
Below are the steps to achieve this:

1. Identify all BC and CC tables where the `PUBLICID` column exists.
2. Execute an `ALTER TABLE` statement to modify the precision of the `PUBLICID` column for each table.

**Example:**
To alter the precision of the `PUBLICID` column in the `bc_account` table:

```sql
ALTER TABLE bc_account
ALTER COLUMN PUBLICID VARCHAR(64);
```

This modification ensures that the `PUBLICID` column is adequately sized to handle larger identifiers.

---

### 5. Create Job Control (JC) Tables

Job Control (JC) tables are essential for tracking the `BATCHID`, status, logs, and progress of various ETL processes during the migration.

> **Note:** Ensure that the order of operations is strictly followed. Before creating control tables - audit columns must be added to EDW tables as mentioned in the Section 3 - Add Audit Columns to EDW Tables

#### Details

- **Purpose:** To track and log job execution and data load status.
- **Tables to Create:** This includes tables like `JC_ONPREM_AUDIT_COUNT_VALIDATION`, `JC_ONPREM_BATCH_STATUS`, `JC_ONPREM_LOAD_STATUS`, `JC_ONPREM_PROCESS_LOG`, and `JC_ONPREM_ERROR_LOG`.

#### 5.1. Table Creation Queries

Below are the queries to create the necessary Job Control tables. **Replace `'YOUR_CORE_CENTER_VALUE'`** with the appropriate Core Center value (**PC**, **BC**, **CC**, **CM**).
**Note:** The below query must be executed once per DB for each core center [PC, BC, CC, CM]
```sql
DECLARE @core_center NVARCHAR(50) = 'YOUR_CORE_CENTER_VALUE'; -- Replace with desired core center
DECLARE @sql NVARCHAR(MAX);

-- JC_MSSQL_AUDIT_COUNT_VALIDATION
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_AUDIT_COUNT_VALIDATION (
    MSSQL_BATCH_ID                  INT NOT NULL,            -- Batch ID of the MS SQL Glue job
    SF_BATCH_ID                     INT NOT NULL,            -- Batch ID of the Table as in SF
    TABLE_NAME                      VARCHAR(MAX)   NOT NULL, -- Name of the table being audited
    SF_INCREMENTAL_REC_COUNT        NUMERIC(38, 0),          -- Record count from Snowflake (Incremental)
    SF_SOFT_DELETE_REC_COUNT        NUMERIC(38, 0),          -- Soft delete count from Snowflake
    DF_INCREMENTAL_REC_COUNT        NUMERIC(38, 0),          -- Incremental record count from Spark DataFrame
    DF_SOFT_DELETE_REC_COUNT        NUMERIC(38, 0),          -- Soft delete record count from Spark DataFrame
    INSERTED_INCREMENTAL_REC_COUNT  NUMERIC(38, 0),          -- Incremental records inserted into on-prem
    DELETED_INCREMENTAL_REC_COUNT   NUMERIC(38, 0),          -- Incremental records deleted from on-prem
    DELETED_SOFT_DELETE_REC_COUNT   NUMERIC(38, 0),          -- Soft delete records deleted from on-prem
    DIFF_SF_VS_DF_INCREMENTAL       NUMERIC(38, 0),          -- Difference between SF and DF Incremental counts
    DIFF_DF_VS_MSSQL_INCREMENTAL   NUMERIC(38, 0),          -- Difference between DF and on-prem Incremental counts
    DIFF_SF_VS_DF_SOFT_DELETE       NUMERIC(38, 0),          -- Difference between SF and DF Soft delete counts
    DIFF_DF_VS_MSSQL_SOFT_DELETE   NUMERIC(38, 0),          -- Difference between DF and on-prem Soft delete counts
    RECONCILIATION_STATUS           VARCHAR(150),            -- Status of reconciliation
    ROW_INSERT_TIMESTAMP            DATETIME2(7)  DEFAULT CURRENT_TIMESTAMP NOT NULL -- Timestamp of row insertion
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_BATCH_STATUS
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_BATCH_STATUS (
    MSSQL_BATCH_ID          INT NOT NULL,                             -- Batch ID of the MS SQL Glue job
    JOB_NAME                VARCHAR(150) NOT NULL,                    -- Job name of the Glue job
    ROW_INSERT_TIMESTAMP    DATETIME2(7) DEFAULT CURRENT_TIMESTAMP,   -- Timestamp of record insertion (Job start time)
    ROW_UPDATE_TIMESTAMP    DATETIME2(7) DEFAULT CURRENT_TIMESTAMP,   -- Timestamp of record final update (Job end time)
    STATUS                  VARCHAR(20) NOT NULL                      -- Status of the batch (e.g., "started", "completed", "failed")
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_LOAD_STATUS
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_LOAD_STATUS (
    MSSQL_BATCH_ID        INT NOT NULL,                           -- Batch ID of the MS SQL Glue job
    SF_BATCH_ID           INT NOT NULL,                           -- Batch ID of the Table as in SF
    TABLE_NAME            VARCHAR(MAX) NOT NULL,                  -- Name of the table being loaded
    STATUS                VARCHAR(150) NOT NULL,                  -- Status of the load (e.g., "started", "completed", "failed")
    ROW_UPDATE_TIMESTAMP  DATETIME2(7) DEFAULT CURRENT_TIMESTAMP, -- Timestamp of the latest update
    ROW_INSERT_TIMESTAMP  DATETIME2(7) DEFAULT CURRENT_TIMESTAMP  -- Timestamp of record insertion
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_PROCESS_LOG
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_PROCESS_LOG (
    MSSQL_BATCH_ID      INT NOT NULL,                       -- Batch ID of the MS SQL Glue job
    SF_BATCH_ID         INT NOT NULL,                       -- Batch ID of the Table as in SF
    TABLE_NAME          VARCHAR(MAX) NOT NULL,              -- Name of the table involved in the process
    PROCESS_NAME        VARCHAR(100) NOT NULL,              -- Name of the process
    ROW_COUNT           NUMERIC(38, 0) NOT NULL,            -- Count of rows processed
    ROW_INSERT_TIMESTAMP DATETIME2(7) DEFAULT CURRENT_TIMESTAMP -- Timestamp of record insertion
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_ERROR_LOG
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_ERROR_LOG (
    MSSQL_BATCH_ID      INT NOT NULL,                            -- Batch ID of the MS SQL Glue job
    SF_BATCH_ID         INT,                                     -- Batch ID of the Table as in SF
    TABLE_NAME          VARCHAR(MAX) NOT NULL,                   -- Name of the table where the error occurred
    ERROR_MESSAGE       VARCHAR(8000) NULL,                      -- Description of the error
    ERROR_SQL           VARCHAR(8000) NULL,                      -- SQL statement causing the error
    ROW_INSERT_TIMESTAMP DATETIME2(7) DEFAULT CURRENT_TIMESTAMP  -- Timestamp of record insertion
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_STG
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_STG (
    ID                 INT NOT NULL,                            -- IDs of the Records of Geography column
    TABLE_NAME         VARCHAR(MAX) NOT NULL,                   -- Name of the table where geography columns exist
    GEO_COLUMN         VARCHAR(MAX),                            -- Geography column that has WKB data
    ROW_INSERT_TIMESTAMP DATETIME2(7) DEFAULT CURRENT_TIMESTAMP -- Timestamp of record insertion
);';
EXEC sp_executesql @sql;

-- JC_MSSQL_DELETE_STG
SET @sql = '
CREATE TABLE JC_MSSQL_' + @core_center + '_DELETE_STG (
    MSSQL_BATCH_ID      INT NOT NULL,                            -- Batch ID of the MS SQL Glue job
    TABLE_NAME         VARCHAR(250) NOT NULL,                   -- Name of the table
    DELETE_TYPE        VARCHAR(50) NOT NULL,                    -- Delete Type takes "incremental/soft"
    ID                 INT NOT NULL,                            -- IDs of the Records of Table
    ROW_INSERT_TIMESTAMP DATETIME2(7) DEFAULT CURRENT_TIMESTAMP -- Timestamp of record insertion
);';
EXEC sp_executesql @sql;

```

**Note:** Replace `'YOUR_CORE_CENTER_VALUE'` with the appropriate Core Center value (**PC**, **BC**, **CC**, **CM**).

---

#### 5.2. Table Creation Queries for EDW Fact Reconciliation
Job Control (JC) tables for fact tables reconciliation are essential for reconciling the counts between Core center database and EDW Database. The below tables are to created in the **Target EDW Database**

```sql
-- NOTE: The below tables are to created only in the EDW Database opted as target database for EDW fact reconciliation

DECLARE @sql NVARCHAR(MAX);


-- JC_SEQ_EDW_RECON_BATCH_ID
SET @sql = '
CREATE SEQUENCE [dbo].[JC_SEQ_EDW_RECON_BATCH_ID]
    AS [int]
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1
    NO CYCLE;';
EXEC sp_executesql @sql;

-- JC_EDW_FACT_RECON_BATCH_STATUS
SET @sql = '
CREATE TABLE [dbo].[JC_EDW_FACT_RECON_BATCH_STATUS](
    [BATCH_ID] [int] NOT NULL,                  -- Unique identifier for each reconciliation batch, generated by a sequence
    [JOB_NAME] [varchar](150) NOT NULL,        -- Name of the Glue job or process running the reconciliation
    [STATUS] [varchar](20) NOT NULL,           -- Current status of the batch (e.g., "started", "completed", "failed")
    [ROW_INSERT_TIMESTAMP] [datetime2](7) DEFAULT (getdate()), -- Timestamp when the batch record was created
    [ROW_UPDATE_TIMESTAMP] [datetime2](7) DEFAULT (getdate()), -- Timestamp when the batch status was last updated
    CONSTRAINT PK_JC_EDW_FACT_RECON_BATCH_STATUS PRIMARY KEY ([BATCH_ID])
);';
EXEC sp_executesql @sql;


-- JC_EDW_FACT_RECON_ERROR_LOG
SET @sql = '
CREATE TABLE [dbo].[JC_EDW_FACT_RECON_ERROR_LOG](
    [BATCH_ID] [int] NOT NULL,                -- Unique identifier for each reconciliation batch, generated by a sequence
    [TABLE_NAME] [varchar](250) NOT NULL,     -- Name of the table involved in the error
    [ERROR_MESSAGE] [varchar](8000),     -- Description of the error that occurred
    [ERROR_SQL] [varchar](8000),         -- SQL statement that caused the error, if applicable
    [ROW_INSERT_TIMESTAMP] [datetime2](7) DEFAULT (getdate()), -- Timestamp when the error was logged
);';
EXEC sp_executesql @sql;


-- JC_EDW_FACT_RECON_PROCESS_LOG
SET @sql = '
CREATE TABLE [dbo].[JC_EDW_FACT_RECON_PROCESS_LOG](
    [BATCH_ID] [int] NOT NULL,                -- Unique identifier for each reconciliation batch, generated by a sequence
    [TABLE_NAME] [varchar](250) NOT NULL,     -- Name of the table being processed
    [PROCESS_NAME] [varchar](100) NOT NULL,   -- Description of the process step
    [ROW_COUNT] [numeric](38, 0),        -- Number of rows affected or counted in the process step
    [ROW_INSERT_TIMESTAMP] [datetime2](7) DEFAULT (getdate()), -- Timestamp when the process step was logged
    CONSTRAINT PK_JC_EDW_FACT_RECON_PROCESS_LOG PRIMARY KEY ([BATCH_ID], [TABLE_NAME], [PROCESS_NAME])
);';
EXEC sp_executesql @sql;

-- JC_EDW_FACT_RECON_RESULT
SET @sql = '
CREATE TABLE [dbo].[JC_EDW_FACT_RECON_RESULT](
    [BATCH_ID] [int] NOT NULL,                -- Unique identifier for each reconciliation batch, generated by a sequence
    [CORE_CENTER] [varchar](5) NOT NULL,      -- Identifier for the source system (e.g., "pc" for PolicyCenter)
    [TABLE_NAME] [varchar](250) NOT NULL,     -- Name of the table being reconciled
    [GW_SNAPSHOT_COUNT] [int],                -- Count of rows from the source system
    [EDW_COUNT] [int],                        -- Count of rows in the target EDW system
    [COUNT_DIFFERENCE] [int],                 -- Difference between GW_SNAPSHOT_COUNT and EDW_COUNT
    [RECON_RESULT] [varchar](12),             -- Result of reconciliation ("successful" or "failed")
    [ROW_INSERT_TIMESTAMP] [datetime2](7) DEFAULT (getdate()), -- Timestamp when the result was recorded
    CONSTRAINT PK_JC_EDW_FACT_RECON_RESULT PRIMARY KEY ([BATCH_ID], [TABLE_NAME])
);';
EXEC sp_executesql @sql;
```


---


## Testing and Validation

1. **Pre-Execution Verification:**
   - **Review Scripts:** DBAs should meticulously review all SQL scripts to ensure they target the correct tables and constraints.
   - **Backup Database:** Always create a comprehensive backup of the database before performing schema changes.
   - **Environment Testing:** Execute the scripts in a staging or testing environment to validate their behavior without impacting production data.

2. **Post-Execution Validation:**
   - **Verify Constraints Dropped:** Confirm that the specified Foreign Key and Primary Key constraints have been successfully removed.
   - **Check Audit Columns:** Ensure that `BATCHID` and `ROW_UPDATE_TIMESTAMP` columns have been correctly added to all applicable EDW tables.
   - **Confirm JC Tables Creation:** Validate that all Job Control (JC) tables have been created with the correct schema and configurations.

3. **Error Handling:**
   - **Monitor Logs:** Check SQL Server logs and any custom logging mechanisms for errors encountered during script execution.
   - **Constraint Reapplication:** Plan and execute the reapplication of constraints post-migration, ensuring data integrity is maintained.


---
