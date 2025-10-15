output "role_arn" {
  description = "The ARN of the Glue IAM role"
  value       = aws_iam_role.role.arn
}
