data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "sso_assume_role_policy" {
  statement {
    sid = "SSOAssumeRole"

    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_${var.sso_role_name}_dd8f77e0d258dd75"]
    }
  }
}


data "aws_iam_policy_document" "glue_assume_role_policy" {
  source_policy_documents = data.aws_caller_identity.current.account_id == "058264312281" ? [data.aws_iam_policy_document.sso_assume_role_policy.json] : []

  statement {
    sid = "GlueAssumeRole"

    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "role" {
  name               = var.role_name
  description        = var.role_description
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "attachment" {
  count      = length(var.policy_arns)
  role       = aws_iam_role.role.name
  policy_arn = var.policy_arns[count.index]
}
