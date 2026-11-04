output "experiment_id" {
  value = var.experiment_id
}

output "results_bucket" {
  value = aws_s3_bucket.results.id
}

output "node_instance_ids" {
  value = { for index, node in aws_instance.node : tostring(index) => node.id }
}

output "node_private_ips" {
  value = { for index, node in aws_instance.node : tostring(index) => node.private_ip }
}

output "coordinator_instance_id" {
  value = aws_instance.node[7].id
}

output "ssm_session_command" {
  value = "aws ssm start-session --region ${var.aws_region} --target ${aws_instance.node[7].id}"
}
