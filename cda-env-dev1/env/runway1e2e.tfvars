region                = "us-east-1"
s3_allow_origins_list = ["*"]
is_prod_env           = false
environment           = "runway1e2e"
gw_bucket_name        = "beta-3-us-east-1-encova-encovadev-cda-56c9a"
gw_bc_dir             = "runway1e2e/bc/1745956435271/"
gw_cc_dir             = "runway1e2e/cc/1745957937064/"
gw_cm_dir             = "runway1e2e/cm/1745956244390/"
gw_pc_dir             = "runway1e2e/pc/1745957555760/"
private_subnet_ids = [
  "subnet-0ebd36304c3f65029",
  "subnet-0b98c2421ddbf3af4",
  "subnet-01b695a0b1e6a248c"
]
gw_s3_to_snowflake_mrg_glue_params = {
  timeout = 360
}
sf_to_ms_sql_mrg_glue_params = {
  timeout       = 120
  worker_type   = "G.4X"
  no_of_workers = 2
}
gwcdaenv                                       = "runway1e2e"
snowflake_user_arn                             = "arn:aws:iam::869935084112:user/8zgt0000-s"
snowflake_storage_integration_external_id      = "FSB72619_SFCRole=3_5yBN8J3uVMiUYIbEv1nr7GLebVM="
snowflake_notification_integration_external_id = "HMB07022_SFCRole=2_YoxsnVzN79FvRL9RMksU1hAOV7SE="
prefix_list_id                                 = "pl-63a5400a"
sso_role_name                                  = "Encova-PermSet-CDA-Test-Custom"
ms_sql_edw_sever_cidr_blocks = [
  "10.160.128.0/24",
  "10.202.144.0/24"
]
ms_sql_edw_sever_ports     = [61438, 59796, 54858]                                        #TODO - replicate the same change to glue_ms_sql_conn_port below (#TODO remove duplicate variables)
ms_sql_glue_secret_name    = "T-AWS-SecretsHub-Test/aws-use1-test-smr-ohuwdbs0024va-0001" # - replicate the same change to mssql_server_secret_names below (#TODO remove duplicate variables)
ms_sql_db_pc               = "PolicyCenterR1_ST"
ms_sql_db_bc               = "BillingCenterR1_ST"
ms_sql_db_cc               = "ClaimCenterR1_ST"
ms_sql_db_cm               = "ContactManagerR1_ST"
glue_ms_sql_conn_port      = 54858
pypi_mirror                = "nexus3.mmi.mig.corp"
snowflake_glue_secret_name = "aws-use1-test-smr-glue-0001"
rds_cluster_secret_name    = "T-AWS-SecretsHub-Test/aws-use1-test-smr-glue-cda-read-0001"
rds_cluster_cidr_blocks = [
  "10.83.14.0/24",
  "10.83.15.0/24",
  "10.83.16.0/24"
]
rds_cluster_hostname      = "gwcp.runway1e2e.rdscluster.aws.encova.test.internal"
rds_cluster_port          = 5432
rds_db_name_cc            = "encova_encovadev_runway1e2e_cc"
rds_db_name_bc            = "encova_encovadev_runway1e2e_bc"
rds_db_name_cm            = "encova_encovadev_runway1e2e_cm"
rds_db_name_pc            = "encova_encovadev_runway1e2e_pc"
mssql_server_secret_names = ["T-AWS-SecretsHub-Test/aws-use1-test-smr-ohuwdbs0024va-0001"]
snowflake_account         = "encova-test.privatelink"
snowflake_database        = "RUNWAY1E2E"
snowflake_role            = "RUNWAY1E2E__GLUE__SVC_ROLE"
snowflake_user            = "0oa29awt6gg41jdmm0h8"
snowflake_warehouse       = "WH_GLUE"
oauth_token_endpoint      = "https://okta-test.encova.com/oauth2/aus1r76oohfmEfKfs0h8/v1/token"
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
rds_cluster_name = "aws-use1-test-rds-psql-runway1e2e-cluster"
# snowflakekey                                    = "" #TODO:PRJTASK0107357:DecryptSnowflakeKeys IAM Policy
# sqldbkey                                        = "" #TODO:PRJTASK0107358:DecryptSQLDBKeys IAM Policy

ms_sql_db_pc_fact  = "PolicyCenterR1_V8"
ms_sql_db_bc_fact  = "BillingCenterR1_V8"
ms_sql_db_cc_fact  = "ClaimCenterR1_V8"
ms_sql_db_cm_fact  = "ContactManagerR1_V8"
ms_sql_db_edw_fact = "EDW_V8"
