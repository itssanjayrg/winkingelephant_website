variable "s3_bucket_name" {
  description = "(Required) The S3 bucket name that will house the Glue job's Python script"
  type        = string
}

variable "script_filepath" {
  description = "(Required) The filepath of the Python script to be called by the Glue job"
  type        = string
}

variable "name" {
  description = "(Required) The name you assign to the AWS Glue job. It must be unique in your account."
  type        = string
}

variable "description" {
  description = "(Optional) Description of the AWS Glue Job. Defaults to an empty string"
  type        = string
  default     = ""
}

variable "role_arn" {
  description = "(Required) The ARN of the IAM role assumed by this job."
  type        = string
}

variable "glue_version" {
  description = "(Optional) The version of glue to use. Defaults to 4.0."
  type        = string
  default     = "4.0"
}

variable "timeout" {
  description = "(Required) The job timeout in minutes"
  type        = number
}

variable "max_retries" {
  description = "(Optional) The maximum number of times to retry this job if it fails. Defaults to 0"
  type        = number
  default     = 0
}

variable "connection_names" {
  description = "(Required) A list of AWS Glue Connections names used for this job."
  type        = list(string)
}

variable "execution_class" {
  description = "(Optional) Indicates whether the job is run with a standard or flexible execution class. Defaults to STANDARD"
  type        = string
  default     = "STANDARD"
}

variable "max_concurrent_runs" {
  description = "(Optional) The maximum number of concurrent runs allowed for a job"
  type        = number
  default     = 1
}

variable "command_name" {
  description = "(Optional) The name of the job command. Defaults to glueetl"
  type        = string
  default     = "glueetl"
}

variable "worker_type" {
  description = "(Required) The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, G.2X, or G.025X for Spark jobs."
  type        = string
}

variable "number_of_workers" {
  description = "(Required) The number of workers of a defined worker_type that are allocated when a job runs."
  type        = number
}

variable "python_version" {
  description = "(Optional) The Python version being used to execute a Python shell job. Defaults to 3"
  type        = string
  default     = 3
}

variable "environment" {
  description = "(Required) The environment name (e.g. dev1e2e)"
  type        = string
}

variable "pypi_mirror" {
  description = "(Required) The URL of the PyPi mirror where AWS Glue jobs can source Python package"
  type        = string
}

variable "extra_py_files" {
  description = "(Optional) List of extra S3-hosted Python files in the Glue job's execution environment"
  type        = list(string)
  default     = []
}

variable "additional_python_modules" {
  description = "(Optional) List of additional Python modules / libraries to include in the Glue job's runtime environment"
  type        = list(string)
  default     = []
}

variable "default_arguments" {
  description = "(Optional) Map of default Glue job Parameters"
  type        = map(string)
  default     = {}
}
