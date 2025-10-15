region                = "us-east-1"
s3_allow_origins_list = ["*"]
is_prod_env           = false
environment           = "preprod"
## TODO: GW Directory values need to be changed to a lookup
gw_bucket_name = "epsilon-3-us-east-1-encova-encovapre-cda-b8405"
gw_bc_dir      = "perfprod/bc/1743595218960/"
gw_cc_dir      = "perfprod/cc/1743595232635/"
gw_cm_dir      = "perfprod/cm/1743595246475/"
gw_pc_dir      = "perfprod/pc/1745901178555/"
private_subnet_ids = [
  "subnet-05e945d9fe5145231",
  "subnet-0cc43aa9c4749572a",
  "subnet-0374fa49e6ed5c365"
]
gw_s3_to_snowflake_mrg_glue_params = {
  timeout = 480
}
sf_to_ms_sql_mrg_glue_params = {
  timeout       = 480
  worker_type   = "G.4X"
  no_of_workers = 8
}
gwcdaenv                                       = "perfprod"
snowflake_user_arn                             = "arn:aws:iam::664418965543:user/1z4w0000-s"
snowflake_storage_integration_external_id      = "VSB98459_SFCRole=3_8+ZZ69d4hHwX0FPSfQOePix3O8E="
snowflake_notification_integration_external_id = "placeholder_for_preprod_notification_integration_external_id" # TODO: Fill me in with Snowflake Notification Integration External ID
prefix_list_id                                 = "pl-63a5400a"
sso_role_name                                  = "Encova-PermSet-CDA-PreProd"
ms_sql_edw_sever_cidr_blocks = [
  "10.202.144.0/24"
]
ms_sql_edw_sever_ports     = [61438]
ms_sql_glue_secret_name    = "T-AWS-SecretsHub-PreProd/aws-use1-preprod-smr-ohrwdbs0024va-0001"
ms_sql_db_pc               = "PolicyCenterR1"
ms_sql_db_bc               = "BillingCenterR1"
ms_sql_db_cc               = "ClaimCenterR1"
ms_sql_db_cm               = "ContactManagerR1"
glue_ms_sql_conn_port      = 61438
pypi_mirror                = "nexus3.mmi.mig.corp"
snowflake_glue_secret_name = "T-AWS-SecretsHub-PreProd/aws-use1-preprod-smr-glue-okta-0001"
rds_cluster_secret_name    = "T-AWS-SecretsHub-PreProd/aws-use1-preprod-smr-glue-cda-read-0001"
rds_cluster_cidr_blocks = [
  "10.84.14.0/24",
  "10.84.15.0/24",
  "10.84.16.0/24"
]
rds_cluster_hostname      = "gwcp.perfprod.rdscluster.aws.encova.preprod.internal"
rds_cluster_port          = 5432
rds_db_name_cc            = "encova_encovapre_perfprod_cc"
rds_db_name_bc            = "encova_encovapre_perfprod_bc"
rds_db_name_cm            = "encova_encovapre_perfprod_cm"
rds_db_name_pc            = "encova_encovapre_perfprod_pc"
mssql_server_secret_names = ["T-AWS-SecretsHub-PreProd/aws-use1-preprod-smr-ohrwdbs0024va-0001"]
snowflake_account         = "encova-preprod_global.privatelink"
snowflake_database        = "PREPROD"
snowflake_role            = "PREPROD__GLUE__SVC_ROLE"
snowflake_user            = "0oa2brb50m3IkbJf80h8"
snowflake_warehouse       = "WH_GLUE"
oauth_token_endpoint      = "https://okta-test.encova.com/oauth2/aus1mgz8y0qpWznIp0h8/v1/token"
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
rds_cluster_name = "aws-use1-preprod-rds-psql-perfprod-cluster"
# snowflakekey                                    = "" #TODO:PRJTASK0107357:DecryptSnowflakeKeys IAM Policy
# sqldbkey                                        = "" #TODO:PRJTASK0107358:DecryptSQLDBKeys IAM Policy

ms_sql_db_pc_fact  = "" # TODO: Fill me in
ms_sql_db_bc_fact  = "" # TODO: Fill me in
ms_sql_db_cc_fact  = "" # TODO: Fill me in
ms_sql_db_cm_fact  = "" # TODO: Fill me in
ms_sql_db_edw_fact = "" # TODO: Fill me in
