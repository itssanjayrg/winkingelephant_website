output "s3_bucket_name" {
  description = "The name of the bucket."
  value       = aws_s3_bucket.this.id
}

output "s3_bucket_arn" {
  description = "The arn of the bucket."
  value       = aws_s3_bucket.this.arn
}
output "s3_bucket_id" {
  description = "The ID of the bucket."
  value       = aws_s3_bucket.this.id
}
