locals {
  script_filename = basename(var.script_filepath)
  default_arguments = {
    "--JOB_NAME"                         = var.name
    "--env_name"                         = var.environment
    "--python-modules-installer-option"  = "--no-cache-dir --index-url https://${var.pypi_mirror}/repository/pypi-proxy/simple/"
    "--additional-python-modules"        = length(var.additional_python_modules) > 0 ? "${join(" ", var.additional_python_modules)} --trusted-host ${var.pypi_mirror}" : ""
    "--extra-py-files"                   = join(",", var.extra_py_files)
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }
}

resource "aws_s3_object" "this" {
  bucket      = var.s3_bucket_name
  key         = local.script_filename
  source      = var.script_filepath
  source_hash = filemd5(var.script_filepath)
  override_provider {
    default_tags {
      tags = {}
    }
  }
}

resource "aws_glue_job" "this" {
  name            = var.name
  description     = var.description
  role_arn        = var.role_arn
  glue_version    = var.glue_version
  timeout         = var.timeout
  max_retries     = var.max_retries
  connections     = sort(var.connection_names)
  execution_class = var.execution_class
  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  #noinspection TfUnknownProperty
  job_run_queuing_enabled = false

  command {
    name            = var.command_name
    script_location = "s3://${var.s3_bucket_name}/${local.script_filename}"
    python_version  = var.python_version
  }

  default_arguments = merge(local.default_arguments, var.default_arguments)
}
