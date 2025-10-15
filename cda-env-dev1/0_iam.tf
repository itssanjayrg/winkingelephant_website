locals {
  s3_bucket_arns = [
    module.cda_raw_bucket_pc.s3_bucket_arn,
    module.cda_raw_bucket_bc.s3_bucket_arn,
    module.cda_raw_bucket_cc.s3_bucket_arn,
    module.cda_raw_bucket_cm.s3_bucket_arn,
    module.cda_raw_bucket_code.s3_bucket_arn,
    module.cda_raw_bucket_archive.s3_bucket_arn,
    module.cda_extracts_bucket.s3_bucket_arn
  ]
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

#Create IAM role policy
data "aws_iam_policy" "required-policy" {
  name = "AWSGlueServiceRole"
}

data "aws_iam_policy_document" "rds_access_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "rds-data:ExecuteStatement",
      "rds-data:BeginTransaction",
      "rds-data:CommitTransaction",
      "rds-data:RollbackTransaction"
    ]
    resources = [
      "arn:aws:rds:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:cluster:${var.rds_cluster_name}-*"
    ]
  }
}

resource "aws_iam_policy" "rds_access" {
  name        = "aws-gbl-cda-${var.environment}-policy-rdw-access-0001"
  description = "IAM policy for RDS access"
  policy      = data.aws_iam_policy_document.rds_access_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

resource "aws_iam_policy" "glue_access" {
  name        = "aws-gbl-cda-${var.environment}-policy-glue-access-0001"
  description = "CDA IAM policy, for glue connections and service"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:DescribeConnectionType",
          "glue:ListConnectionTypes",
          "glue:ListJobs",
          "glue:ResetJobBookmark"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:GetEntityRecords",
          "glue:ListEntities",
          "glue:RefreshOAuth2Tokens"
        ]
        Resource = sort(toset([for conn in values(aws_glue_connection.default) : conn.arn]))
      }
    ]
  })
  lifecycle {
    create_before_destroy = false
  }
}


data "aws_iam_policy_document" "glue_job_access_policy" {
  version = "2012-10-17"
  statement {
    sid       = "ActionsThatDoNotRequireResources"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:BatchGetJobs",
      "glue:BatchStopJobRun",
      "glue:GetJob",
      "glue:GetJobBookmark",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:GetJobUpgradeAnalysis",
      "glue:GetJobs",
      "glue:GetTags",
      "glue:ListJobUpgradeAnalyses",
      "glue:StartJobRun",
      "glue:StartJobUpgradeAnalysis",
      "glue:StopJobUpgradeAnalysis",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:UpdateJob",
      "glue:UpgradeJob"
    ]
    resources = sort([
      module.snowflake_resource_monitor_notification.glue_job_arn,
      module.gw_s3_to_snowflake_mrg.glue_job_arn,
      module.gw_s3_to_encova_s3.glue_job_arn,
      module.gw_access_2.glue_job_arn,
      module.sf_to_ms_sql_mrg.glue_job_arn,
      module.s3_migration.glue_job_arn,
    ])
  }
}

resource "aws_iam_policy" "glue_job_access" {
  name        = "aws-gbl-cda-${var.environment}-policy-call-glue-0001"
  description = "CDA IAM policy, for calling glue jobs"
  policy      = data.aws_iam_policy_document.glue_job_access_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "s3_read_list_gw_bucket_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ]
    resources = ["arn:aws:s3:::${var.gw_bucket_name}/${var.gwcdaenv}/*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]
    resources = ["arn:aws:s3:::${var.gw_bucket_name}"]
  }
}

resource "aws_iam_policy" "s3_read_list" {
  name        = "aws-gbl-cda-${var.environment}-policy-s3-read-list"
  description = "CDA IAM policy, to read and list GW S3 buckets"
  policy      = data.aws_iam_policy_document.s3_read_list_gw_bucket_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "s3_cda_bucket_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion"
    ]
    resources = sort(toset([for arn in local.s3_bucket_arns : "${arn}/*"]))
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]
    resources = local.s3_bucket_arns
  }
}

resource "aws_iam_policy" "s3_cda_bucket" {
  name        = "aws-gbl-cda-${var.environment}-policy-s3-cda-bucket"
  description = "CDA IAM policy, for CDA buckets"
  policy      = data.aws_iam_policy_document.s3_cda_bucket_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "sns_glue_job_access_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "sns:ListSubscriptionsByTopic",
      "sns:Publish"
    ]
    resources = [
      aws_sns_topic.glue_job.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "sns:ListEndpointsByPlatformApplication",
      "sns:ListOriginationNumbers",
      "sns:ListPlatformApplications",
      "sns:ListSMSSandboxPhoneNumbers",
      "sns:ListSubscriptions",
      "sns:ListTopics"
    ]
    resources = [
      "arn:aws:sns:us-east-1:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

resource "aws_iam_policy" "sns_glue_job_access" {
  name        = "aws-gbl-cda-${var.environment}-policy-sns-glue-job-access"
  description = "Grants list and publish on SNS topic dedicated to Glue jobs"
  policy      = data.aws_iam_policy_document.sns_glue_job_access_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "sns_snowflake_access_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "sns:ListSubscriptionsByTopic",
      "sns:Publish"
    ]
    resources = [
      aws_sns_topic.snowflake.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "sns:ListEndpointsByPlatformApplication",
      "sns:ListOriginationNumbers",
      "sns:ListPlatformApplications",
      "sns:ListSMSSandboxPhoneNumbers",
      "sns:ListSubscriptions",
      "sns:ListTopics"
    ]
    resources = [
      "arn:aws:sns:us-east-1:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

resource "aws_iam_policy" "sns_snowflake_access" {
  name        = "aws-gbl-cda-${var.environment}-policy-sns-snowflake-access"
  description = "Grants publish on SNS topic dedicated to Snowflake"
  policy      = data.aws_iam_policy_document.sns_snowflake_access_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "kms_policy" {
  version = "2012-10-17"
  statement {
    sid    = ""
    effect = "Allow"
    actions = [
      "kms:ReEncryptFrom",
      "kms:ReEncryptTo",
      "kms:GenerateDataKey",
      "kms:GenerateDataKeyWithoutPlaintext",
      "kms:GenerateDataKeyPair",
      "kms:Encrypt",
      "kms:DescribeKey",
      "kms:Decrypt"
    ]
    resources = [
      module.cda-kms.kms-arn
    ]
  }
}

resource "aws_iam_policy" "kms" {
  name        = "aws-gbl-cda-${var.environment}-policy-kms-0001"
  description = "CDA IAM policy, for use kms key"
  policy      = data.aws_iam_policy_document.kms_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "log_group_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutRetentionPolicy",
      "logs:CreateLogGroup",
      "logs:PutLogEvents",
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*:/aws-glue/*" # TODO: Make this specific to the environment
    ]
  }
}

resource "aws_iam_policy" "log_group" {
  name        = "aws-gbl-cda-${var.environment}-policy-create-log-group"
  description = "CDA IAM policy, for create log group"
  policy      = data.aws_iam_policy_document.log_group_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

data "aws_iam_policy_document" "secret_manager_policy" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetResourcePolicy",
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
      "secretsmanager:ListSecretVersionIds"
    ]
    resources = sort(toset(
      flatten([
        [
          "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.snowflake_glue_secret_name}-*",
          "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.rds_cluster_secret_name}-*",
        ],
        [for secret_name in var.mssql_server_secret_names : "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${secret_name}-*"]
      ])
    ))
  }

  statement {
    effect  = "Allow"
    actions = ["secretsmanager:ListSecrets"]
    resources = sort(toset(
      flatten([
        [
          "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.snowflake_glue_secret_name}-*"
        ],
        [for secret_name in var.mssql_server_secret_names : "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${secret_name}-*"]
      ]))
    )
  }
}


resource "aws_iam_policy" "secret_manager" {
  name        = "aws-gbl-cda-${var.environment}-policy-secret-manager"
  description = "CDA IAM policy, for create secret manager access, attached to aws-gbl-${var.environment}-role-cda-gw-access-0001"
  policy      = data.aws_iam_policy_document.secret_manager_policy.json
  lifecycle {
    create_before_destroy = false
  }
}

#Create IAM Role

#This aws-gbl-<env>-role-cda-gw-access-0001 role can be edited & redeployed,
#but if it is redeployed after GW has approved this role ARN, another GW ticket must be raised to re-approve this role, even if the ARN is unchanged.
module "cda_gw_role" {
  source           = "./modules/iam_role/cda_glue_role"
  role_name        = "aws-gbl-${var.environment}-role-cda-gw-access-0001"
  role_description = "Glue execution role for job that will read GW buckets and copy data to Encova landing zone buckets"
  sso_role_name    = var.sso_role_name
  policy_arns = [
    aws_iam_policy.rds_access.arn,
    aws_iam_policy.glue_access.arn,
    aws_iam_policy.glue_job_access.arn,
    aws_iam_policy.s3_read_list.arn,
    aws_iam_policy.s3_cda_bucket.arn,
    # aws_iam_policy.secret_manager_gw_access.arn, #TODO: PRJTASK0107357: DecryptSnowflakeKeys IAM Policy
    aws_iam_policy.sns_glue_job_access.arn,
    aws_iam_policy.kms.arn,
    aws_iam_policy.log_group.arn,
    aws_iam_policy.secret_manager.arn,
    data.aws_iam_policy.required-policy.arn
  ]
}

module "snowflake_storage_iam_role" {
  source                      = "./modules/iam_role/cda_snowflake_role"
  role_name                   = "aws-gbl-${var.environment}-role-cda-snowflake-storage-integration-0001"
  role_description            = "Role for user in Snowflake to assume to enable storage integration"
  trust_principal_arn         = var.snowflake_user_arn
  trust_principal_external_id = var.snowflake_storage_integration_external_id
  policy_arns = [
    aws_iam_policy.s3_cda_bucket.arn,
    aws_iam_policy.kms.arn
  ]
}

module "snowflake_ms_sql_iam_role" {
  source           = "./modules/iam_role/cda_glue_role"
  role_name        = "aws-gbl-${var.environment}-role-cda-snowflake-to-ms-sql-0001"
  role_description = "Glue execution role for job that will bring data from Snowflake to MS SQL"
  sso_role_name    = var.sso_role_name
  policy_arns = [
    aws_iam_policy.glue_access.arn,
    aws_iam_policy.glue_job_access.arn,
    data.aws_iam_policy.required-policy.arn
    # aws_iam_policy.secret_manager_snowflake_ms_sql.arn #TODO: PRJTASK0107358: DecryptSQLDBKeys IAM Policy
  ]

}

data "aws_iam_policy_document" "snowflake_notification_integration_role_assume_role_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_notification_integration_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_notification_integration_role" {
  name               = "aws-gbl-${var.environment}-role-snowflake-notification-integration-0001"
  description        = "Role assumed by Snowflake's notification integration for sending SNS notifications"
  assume_role_policy = data.aws_iam_policy_document.snowflake_notification_integration_role_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "attachment" {
  role       = aws_iam_role.snowflake_notification_integration_role.name
  policy_arn = aws_iam_policy.sns_snowflake_access.arn
}

output "snowflake_storage_role_arn" {
  description = "The ARN of the Glue IAM role"
  value       = module.snowflake_storage_iam_role.role_arn
}

output "cda_gw_role_arn" {
  description = "The ARN of the Glue IAM role"
  value       = module.cda_gw_role.role_arn
}

output "snowflake_ms_sql_iam_role_arn" {
  description = "The ARN of the Glue IAM role"
  value       = module.snowflake_ms_sql_iam_role.role_arn
}
