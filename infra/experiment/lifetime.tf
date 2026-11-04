# destroy를 잊어도 생성 후 max_lifetime_minutes가 지나면 인스턴스를 정지한다(terminate 아님).
# 인스턴스가 다시 만들어지면 시각도 다시 잡는다.
locals {
  instance_ids = concat(aws_instance.node[*].id, [aws_instance.loadgen.id])
}

resource "time_static" "created" {
  triggers = {
    instance_ids = join(",", local.instance_ids)
  }
}

resource "aws_scheduler_schedule" "max_lifetime" {
  name                         = "openkache-max-lifetime"
  schedule_expression          = "at(${formatdate("YYYY-MM-DD'T'hh:mm:ss", timeadd(time_static.created.rfc3339, "${var.max_lifetime_minutes}m"))})"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    role_arn = "arn:aws:iam::${var.account_id}:role/openkache-scheduler"
    input    = jsonencode({ InstanceIds = local.instance_ids })
  }
}
