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


variable "sso_role_name" {
  description = "The name of the developer role assumed by SSO"
  type        = string
}
