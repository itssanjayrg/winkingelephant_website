resource "aws_iam_role" "role" {
  name        = var.role_name
  description = var.role_description
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          "AWS" : "${var.trust_principal_arn}"
        }
        Action = "sts:AssumeRole"
        "Condition" : {
          "StringEquals" : {
            "sts:ExternalId" : "${var.trust_principal_external_id}"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attachment" {
  count      = length(var.policy_arns)
  role       = aws_iam_role.role.name
  policy_arn = var.policy_arns[count.index]
}
