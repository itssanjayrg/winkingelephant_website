data "aws_caller_identity" "current" {}

resource "aws_kms_key" "default" {

  description = "KMS key for ${var.env} cda S3 bucket encryption"

  enable_key_rotation = true
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Id" : "key-default-1",
      "Statement" : [
        {
          "Sid" : "Enable IAM User Permissions",
          "Effect" : "Allow",
          "Principal" : {
            "AWS" : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
          },
          "Action" : "kms:*",
          "Resource" : "*"
        },
        {
          "Sid" : "allow_events_to_decrypt_key",
          "Effect" : "Allow",
          "Principal" : {
            "Service" : "events.amazonaws.com"
          },
          "Action" : [
            "kms:Decrypt",
            "kms:GenerateDataKey*"
          ],
          "Resource" : "*"
        }
      ]
    }
  )
}


resource "aws_kms_alias" "alias" {

  name          = var.name
  target_key_id = aws_kms_key.default.key_id
}
