locals {
  # 교보재 조건: cache-server 8대 + 부하 생성 1대.
  max_project_instances = 9
  results_bucket        = "openstd-s15p31d111-results"
  # m6gd.medium instance store(약 59GB) 중 KV 저장소로 쓸 용량.
  nvme_capacity_bytes = 40 * 1024 * 1024 * 1024
}

# 상한 검사: 이 state 밖의 프로젝트 인스턴스(종료되지 않은 것)와 합쳐 9대를 넘으면 plan이 실패한다.
data "aws_instances" "project" {
  instance_tags        = { Project = "openkache" }
  instance_state_names = ["pending", "running", "stopping", "stopped"]
}

data "aws_instances" "managed_here" {
  instance_tags        = { Project = "openkache", ManagedBy = "terraform-experiment" }
  instance_state_names = ["pending", "running", "stopping", "stopped"]
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_instance" "node" {
  count = var.node_count

  ami                    = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = "openkache-node"

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    results_bucket         = local.results_bucket
    source_commit          = var.source_commit
    node_id                = count.index + 1
    role                   = "node"
    storage_engine         = var.storage_engine
    storage_capacity_bytes = local.nvme_capacity_bytes
  })
  user_data_replace_on_change = true

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  tags = {
    Name = "openkache-node-${count.index + 1}"
    Role = "node"
  }

  lifecycle {
    ignore_changes = [ami]

    # 새로 만들 대수 = cache-server node_count + 부하 생성 1.
    precondition {
      condition     = length(data.aws_instances.project.ids) - length(data.aws_instances.managed_here.ids) + var.node_count + 1 <= local.max_project_instances
      error_message = "프로젝트 인스턴스가 ${local.max_project_instances}대를 넘는다. 다른 곳에서 만든 인스턴스: ${length(data.aws_instances.project.ids) - length(data.aws_instances.managed_here.ids)}대."
    }
  }
}

# experiment-runner 전용. cache-server와 CPU를 나눠 쓰지 않게 따로 둔다.
resource "aws_instance" "loadgen" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = "openkache-node"

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    results_bucket         = local.results_bucket
    source_commit          = var.source_commit
    node_id                = 0
    role                   = "loadgen"
    storage_engine         = var.storage_engine
    storage_capacity_bytes = local.nvme_capacity_bytes
  })
  user_data_replace_on_change = true

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  tags = {
    Name = "openkache-loadgen"
    Role = "loadgen"
  }

  # 상한 검사는 plan 단계에서 aws_instance.node의 precondition이 이 1대를 포함해 한다(node_count >= 1).
  lifecycle {
    ignore_changes = [ami]
  }
}
