variable "environment" {
  description = "The env of the project"
  type        = string
  nullable    = false
}

variable "region" {
  description = "The name of the AWS Region"
  type        = string
  nullable    = false
}

variable "s3_allow_origins_list" {
  description = "AWS S3 Cors Origin List"
  type        = list(any)
}

variable "gw_bucket_name" {
  description = "GW CDA bucket name"
  type        = string
}

variable "gw_bc_dir" {
  description = "GW BC core center directory name"
  type        = string
}

variable "gw_cc_dir" {
  description = "GW CC core center directory name"
  type        = string
}

variable "gw_cm_dir" {
  description = "GW CM core center directory name"
  type        = string
}

variable "gw_pc_dir" {
  description = "GW PC core center directory name"
  type        = string
}

variable "gwcdaenv" {
  description = "The variable is for GW bucket connection"
  type        = string
}

variable "cda_deployment_role" {
  description = "The arn of the deployment role assumed by ci-cd user"
  type        = string
  default     = ""
}

variable "is_prod_env" {
  description = "prod env condition"
  type        = bool
}

variable "glue_job_sns_email_addresses" {
  description = "SNS topic subscription email addresses for the Glue Jobs topic"
  type        = list(string)
}

variable "snowflake_sns_email_addresses" {
  description = "SNS topic subscription email addresses for the Snowflake SNS topic"
  type        = list(string)
}


variable "snowflake_user_arn" {
  description = "Snowflake Storage Integration User ARN"
  type        = string
}

variable "snowflake_storage_integration_external_id" {
  description = "Snowflake Storage Integration External ID"
  type        = string
}

variable "snowflake_notification_integration_external_id" {
  description = "Snowflake Notification Integration External ID"
  type        = string
}

variable "prefix_list_id" {
  description = "HTTPS Egress is allowed for this Prefix List ID in the default security group"
  type        = string
}

#TODO: PRJTASK0107357: DecryptSnowflakeKeys IAM Policy
#When you create DecryptSnowflakeKeys IAM Policy, please enable this variable, and set a value in tfvars file.
# variable "snowflakekey" {
#   description = "The snowflake secret key arn"
#   type = string
# }

#TODO: PRJTASK0107358: DecryptSQLDBKeys IAM Policy
#When you create DecryptSQLDBKeys IAM Policy, please enable this variable, and set a value in tfvars file.
# variable "sqldbkey" {
#   description = "The sqldb secret key arn"
#   type = string
# }

variable "private_subnet_ids" {
  description = "private subnet ids in the vpc"
  type        = list(string)
}

variable "sso_role_name" {
  description = "The name of the developer role assumed by SSO"
  type        = string
}

variable "ms_sql_edw_sever_cidr_blocks" {
  description = "CIDR blocks for on-prem EDW server access"
  type        = list(string)
}

variable "rds_cluster_cidr_blocks" {
  description = "CIDR blocks for RDS Cluster access"
  type        = list(string)
}

variable "ms_sql_edw_sever_ports" {
  description = "Ports for on-prem EDW server access"
  type        = list(string)
}

variable "pypi_mirror" {
  description = "Nexus PyPi Mirror host"
  type        = string
}

variable "snowflake_glue_secret_name" {
  description = "Name of the Secrets Manager Secret with OAuth credentials for the Glue Snowflake service user"
  type        = string
}

variable "rds_cluster_secret_name" {
  description = "Name of the Secrets Manager Secret with RDS Cluster credentials for the Glue Snowflake service user"
  type        = string
  default     = "--no-value--"
}

variable "mssql_server_secret_names" {
  description = "Name of the Secrets Manager Secrets with credentials for the On-prem MS SQL server"
  type        = list(string)
}

variable "ms_sql_glue_secret_name" {
  description = "Name of the Secrets Manager Secret with credentials for the On-prem MS SQL server, used in Glue Jobs"
  type        = string
}

variable "ms_sql_db_pc" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Policy Center."
  type        = string
}

variable "ms_sql_db_bc" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Billing Center."
  type        = string
}

variable "ms_sql_db_cc" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Claims Center."
  type        = string
}

variable "ms_sql_db_cm" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Contact Manager."
  type        = string
}

variable "glue_ms_sql_conn_port" {
  description = "Defines the port name used by Glue to establish a connection with the On-Premises MS SQL Server."
  type        = string
}


variable "rds_cluster_hostname" {
  description = "The hostname of the RDS cluster"
  type        = string
}

variable "rds_cluster_port" {
  description = "The port of the RDS cluster"
  type        = number
}

variable "snowflake_account" {
  description = "Snowflake account"
  type        = string
}

variable "snowflake_database" {
  description = "Snowflake database"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake role to be assumed by the Glue service User"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake Glue service user login name"
  type        = string
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse to be assumed by the Glue service user"
  type        = string
}

variable "oauth_token_endpoint" {
  description = "Okta authorization server endpoint"
  type        = string
}

variable "rds_cluster_name" {
  description = "RDS Cluster Name"
  type        = string
}

variable "rds_db_name_cc" {
  description = "RDS Database name for Claims Center (CC)"
  type        = string
}

variable "rds_db_name_bc" {
  description = "RDS Database name for Billing Center (BC)"
  type        = string
}

variable "rds_db_name_cm" {
  description = "RDS Database name for Contact Manager (CM)"
  type        = string
}

variable "rds_db_name_pc" {
  description = "RDS Database name for Policy Center (PC)"
  type        = string
}

variable "gw_s3_to_snowflake_mrg_glue_params" {
  description = "AWS Glue Job Parameters for AWS Glue Job that migrates data from GW to S3 to Snowflake"
  type = object({
    timeout = number
  })
}

variable "sf_to_ms_sql_mrg_glue_params" {
  description = "AWS Glue Job Parameters that migrates data from Snowflake to MS SQL Server"
  type = object({
    timeout       = number
    worker_type   = string
    no_of_workers = number
  })
}

variable "ms_sql_db_pc_fact" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Fact Reconciliation."
  type        = string
}

variable "ms_sql_db_bc_fact" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Fact Reconciliation."
  type        = string
}

variable "ms_sql_db_cc_fact" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Fact Reconciliation."
  type        = string
}

variable "ms_sql_db_cm_fact" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Fact Reconciliation."
  type        = string
}

variable "ms_sql_db_edw_fact" {
  description = "Specifies the name of the database hosted on the On-Premises MS SQL Server for Fact Reconciliation."
  type        = string
}
