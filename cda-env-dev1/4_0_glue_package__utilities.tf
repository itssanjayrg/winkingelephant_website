locals {
  glue_scripts_dir                = "${path.cwd}/glue_scripts"
  glue_packages_dir               = "${path.cwd}/dist"
  utilities_package_filename      = "utilities-0.1-py3-none-any.whl"
  utilities_package_filename_path = "${local.glue_packages_dir}/${local.utilities_package_filename}"
}

resource "aws_s3_object" "utilities_package_upload" {
  bucket      = module.cda_raw_bucket_code.s3_bucket_name
  key         = "Glue_Scripts/${local.utilities_package_filename}"
  source      = local.utilities_package_filename_path
  source_hash = filemd5(local.utilities_package_filename_path)
  override_provider {
    default_tags {
      tags = {}
    }
  }
}
