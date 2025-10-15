region                = "us-east-1"
s3_allow_origins_list = ["*"]
is_prod_env           = false
environment           = "dev1"
gw_bucket_name        = "beta-3-us-east-1-encova-encovadev-cda-56c9a"
gw_bc_dir             = "dev1e2e/bc/1745172445388/"
gw_cc_dir             = "dev1e2e/cc/1745174274718/"
gw_cm_dir             = "dev1e2e/cm/1745172304241/"
gw_pc_dir             = "dev1e2e/pc/1745174051807/"
private_subnet_ids = [
  "subnet-03f222a5ee0f16810",
  "subnet-0e294f8131d0fbf52",
  "subnet-0958fcfeb8e39870b"
]
gw_s3_to_snowflake_mrg_glue_params = {
  timeout = 121
}
sf_to_ms_sql_mrg_glue_params = {
  timeout       = 120
  worker_type   = "G.2X"
  no_of_workers = 2
}
gwcdaenv                                       = "dev1e2e"
snowflake_user_arn                             = "arn:aws:iam::058264079549:user/8j9o0000-s"
snowflake_storage_integration_external_id      = "HMB07022_SFCRole=2_YosnVzN79FvRL9RMksU1hAOV7SE="
snowflake_notification_integration_external_id = "HMB07022_SFCRole=2_YoxsnVzN79FvRL9RMksU1hAOV7SE="
prefix_list_id                                 = "pl-63a5400a"
sso_role_name                                  = "Encova-PermSet-CDA-Dev-Custom"
ms_sql_edw_sever_cidr_blocks = [
  "10.160.128.0/24",
  "10.202.144.0/24"
]
ms_sql_edw_sever_ports     = [61438, 59796]                        #  - replicate the same change to glue_ms_sql_conn_port below (#TODO remove duplicate variables)
ms_sql_glue_secret_name    = "aws-use1-dev-smr-ohdwdbs0024va-0001" # - replicate the same change to mssql_server_secret_names below
ms_sql_db_pc               = "PolicyCenterR1"
ms_sql_db_bc               = "BillingCenterR1"
ms_sql_db_cc               = "ClaimCenterR1"
ms_sql_db_cm               = "ContactManagerR1" # No longer ContactmangerR1
glue_ms_sql_conn_port      = 59796
pypi_mirror                = "nexus3.mmi.mig.corp"
snowflake_glue_secret_name = "dev-aws-glue"
rds_cluster_secret_name    = "D-AWS-SecretsHub-Dev/aws-use1-dev-smr-glue-cda-read-0001"
rds_cluster_cidr_blocks = [
  "10.82.14.0/24",
  "10.82.15.0/24",
  "10.82.16.0/24"
]
rds_cluster_hostname = "gwcp.dev1e2e.rdscluster.aws.encova.dev.internal"
rds_cluster_port     = 5432
rds_db_name_cc       = "encova_encovadev_dev1e2e_cc"
rds_db_name_bc       = "encova_encovadev_dev1e2e_bc"
rds_db_name_cm       = "encova_encovadev_dev1e2e_cm"
rds_db_name_pc       = "encova_encovadev_dev1e2e_pc"
mssql_server_secret_names = [
  "aws-use1-dev-smr-ohrwdbs0024va-0001",
  "aws-use1-dev-smr-ohdwdbs0024va-0001"
]
snowflake_account    = "encova-dev.privatelink"
snowflake_database   = "DEV1E2E"
snowflake_role       = "DEV1E2E__GLUE__SVC_ROLE"
snowflake_user       = "0oa25sh1ca4BsKnGC0h8"
snowflake_warehouse  = "WH_GLUE"
oauth_token_endpoint = "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token"
glue_job_sns_email_addresses = [
  "zane.clark@pwc.com",
  "sanjay.rg@pwc.com",
  "russell.stewart@encova.com",
  "ryan.downing@encova.com",
  "vashista.muddasani@encova.com",
  "sriram.v.vemuri@pwc.com",
  "amandeep.takhar@pwc.com",
  "arya.e.r@pwc.com",
  "payal.narayan@pwc.com",
]
snowflake_sns_email_addresses = [
  "zane.clark@pwc.com",
  "sanjay.rg@pwc.com",
  "russell.stewart@encova.com",
  "ryan.downing@encova.com",
  "vashista.muddasani@encova.com",
  "sriram.v.vemuri@pwc.com",
  "amandeep.takhar@pwc.com",
  "arya.e.r@pwc.com",
  "payal.narayan@pwc.com",
]
rds_cluster_name = "aws-use1-dev-rds-psql-dev1e2e"
# snowflakekey                                    = "" #TODO:PRJTASK0107357:DecryptSnowflakeKeys IAM Policy
# sqldbkey                                        = "" #TODO:PRJTASK0107358:DecryptSQLDBKeys IAM Policy

ms_sql_db_pc_fact  = "PolicyCenterR1_ST"
ms_sql_db_bc_fact  = "BillingCenterR1_ST"
ms_sql_db_cc_fact  = "ClaimCenterR1_ST"
ms_sql_db_cm_fact  = "ContactManagerR1_ST"
ms_sql_db_edw_fact = "EDW_ST"
