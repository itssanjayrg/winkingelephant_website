resource "aws_sns_topic" "glue_job" {
  name              = "sns-cda-${var.environment}-glue-job-0001"
  kms_master_key_id = module.cda-kms.kms-arn
}

resource "aws_sns_topic_subscription" "glue_job_sns_topic" {
  for_each  = toset(var.glue_job_sns_email_addresses)
  topic_arn = aws_sns_topic.glue_job.arn
  protocol  = "email"
  endpoint  = each.value
}
