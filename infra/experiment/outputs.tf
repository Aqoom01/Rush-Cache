output "node_instance_ids" {
  value = aws_instance.node[*].id
}

output "node_private_ips" {
  value = aws_instance.node[*].private_ip
}

output "loadgen_instance_id" {
  value = aws_instance.loadgen.id
}

output "instance_ids" {
  description = "cache-server와 부하 생성 노드 전체"
  value       = local.instance_ids
}

output "storage_engine" {
  value = var.storage_engine
}

output "stops_at_utc" {
  description = "자동 정지 시각(UTC)"
  value       = timeadd(time_static.created.rfc3339, "${var.max_lifetime_minutes}m")
}
