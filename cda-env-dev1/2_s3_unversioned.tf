module "cda_bucket_pc" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-pc-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_bc" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-bc-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_cc" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-cc-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_cm" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-cm-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_code" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-code-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_extracts" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-s3-cda-extracts-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

module "cda_bucket_archive" {
  source                = "./modules/s3"
  bucket_name           = "encova-aws-use1-${var.environment}-archive-0002"
  kms_key_arn           = module.cda-kms.kms-arn
  is_prod_env           = var.is_prod_env
  is_versioning_enabled = false
}

resource "aws_s3_bucket_lifecycle_configuration" "cda_bucket_archive_to_deep_archive" {
  bucket = module.cda_bucket_archive.s3_bucket_id

  rule {
    id = "rule-1"

    # Applies to all objects in bucket
    filter {}
    status = "Enabled"

    transition {
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

output "s3_cda_pc_0002_bucket_name" {
  value = module.cda_bucket_pc.s3_bucket_name
}

output "s3_cda_bc_0002_bucket_name" {
  value = module.cda_bucket_bc.s3_bucket_name
}

output "s3_cda_cc_0002_bucket_name" {
  value = module.cda_bucket_cc.s3_bucket_name
}

output "s3_cda_cm_0002_bucket_name" {
  value = module.cda_bucket_cm.s3_bucket_name
}

output "s3_cda_code_0002_bucket_name" {
  value = module.cda_bucket_code.s3_bucket_name
}

output "s3_cda_archive_0002_bucket_name" {
  value = module.cda_bucket_archive.s3_bucket_name
}
