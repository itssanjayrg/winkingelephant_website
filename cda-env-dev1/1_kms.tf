#Create KMS
module "cda-kms" {
  source = "./modules/cda_kms"
  region = data.aws_region.current.name
  name   = "alias/kms-cda-${var.environment}-0001"
  env    = var.environment
}
