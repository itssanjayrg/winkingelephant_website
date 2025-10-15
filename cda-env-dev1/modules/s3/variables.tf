variable "bucket_name" {
  description = "bucket name"
  type        = string
}

variable "kms_key_arn" {
  description = "kms key arn"
  type        = string
}

variable "is_prod_env" {
  description = "prod env condition"
  type        = bool
}

variable "is_versioning_enabled" {
  description = "Is versioning enabled on the S3 bucket"
  type        = bool
}
