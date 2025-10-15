resource "aws_sns_topic" "snowflake" {
  name              = "sns-cda-${var.environment}-snowflake-0001"
  kms_master_key_id = module.cda-kms.kms-arn
}

resource "aws_sns_topic_subscription" "snowflake_sns_topic" {
  for_each  = toset(var.snowflake_sns_email_addresses)
  topic_arn = aws_sns_topic.snowflake.arn
  protocol  = "email"
  endpoint  = each.value
}
