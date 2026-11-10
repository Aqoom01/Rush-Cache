locals {
  # 교보재 조건: cache-server 8대 + 부하 생성 1대.
  max_project_instances = 9
  results_bucket        = "openstd-s15p31d111-results"
  # m6gd.medium instance store(약 59GB) 중 KV 저장소로 쓸 용량.
  nvme_capacity_bytes = 40 * 1024 * 1024 * 1024

  # B1 모드는 모든 노드가 부팅 때 전체 노드 주소 목록(hosts)을 알아야 해서 private IP를 고정한다.
  # 10.42.1.0/24에서 .11-.18 = cache-server 1-8번, .10 = 부하 생성. (.0-.3, .255는 AWS 예약)
  node_private_ips   = [for i in range(var.node_count) : cidrhost(aws_subnet.public.cidr_block, 11 + i)]
  loadgen_private_ip = cidrhost(aws_subnet.public.cidr_block, 10)
}

# 상한 검사: 이 state 밖의 프로젝트 인스턴스(종료되지 않은 것)와 합쳐 9대를 넘으면 plan이 실패한다.
# Project 태그가 있는 인스턴스만 센다. 태그 없는 수동 생성은 운영 규칙상 하지 않는다(infra/README.md §1).
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
  private_ip             = local.node_private_ips[count.index]
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = "openkache-node"

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    results_bucket         = local.results_bucket
    source_commit          = var.source_commit
    node_id                = count.index + 1
    role                   = "node"
    storage_engine         = var.storage_engine
    storage_capacity_bytes = local.nvme_capacity_bytes
    server_mode            = var.server_mode
    b1_hosts               = join(",", local.node_private_ips)
    self_ip                = local.node_private_ips[count.index]
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

    precondition {
      condition     = var.server_mode == "a1" || var.storage_engine == "memory"
      error_message = "server_mode=b1은 in-memory KvEngine만 쓰므로 storage_engine=memory여야 한다."
    }
  }
}

# experiment-runner 전용. cache-server와 CPU를 나눠 쓰지 않게 따로 둔다.
resource "aws_instance" "loadgen" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  private_ip             = local.loadgen_private_ip
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = "openkache-node"

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    results_bucket         = local.results_bucket
    source_commit          = var.source_commit
    node_id                = 0
    role                   = "loadgen"
    storage_engine         = var.storage_engine
    storage_capacity_bytes = local.nvme_capacity_bytes
    server_mode            = var.server_mode
    b1_hosts               = join(",", local.node_private_ips)
    self_ip                = local.loadgen_private_ip
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
