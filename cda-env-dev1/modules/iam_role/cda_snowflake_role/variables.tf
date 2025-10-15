variable "role_name" {
  description = "The name of the IAM role"
  type        = string
}

variable "role_description" {
  description = "The descriptionc of the IAM role"
  type        = string
}

variable "policy_arns" {
  description = "The ARNs of the IAM policies to attach"
  type        = list(string)
}

variable "trust_principal_arn" {
  description = "Snowflake Storage Integration User ARN"
  type        = string

}

variable "trust_principal_external_id" {
  description = "Snowflake Storage Integration External ID"
  type        = string

}
