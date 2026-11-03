output "state_bucket" {
  description = "experiment/의 S3 backend 버킷"
  value       = aws_s3_bucket.this["state"].id
}

output "results_bucket" {
  description = "실험 결과(results/)와 소스 bundle(bundles/) 버킷"
  value       = aws_s3_bucket.this["results"].id
}

output "node_instance_profile" {
  value = aws_iam_instance_profile.node.name
}

output "scheduler_role_arn" {
  value = aws_iam_role.scheduler.arn
}
