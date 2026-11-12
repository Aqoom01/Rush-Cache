data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${var.instance_architecture}"
}

data "aws_ec2_instance_type" "selected" {
  instance_type = var.instance_type
}

locals {
  coordinator_index = var.node_count - 1
  jar_path = var.artifact_jar != "" ? abspath(var.artifact_jar) : abspath(
    "${path.module}/../../build/libs/openkache-failure-detector-lab-0.1.0-SNAPSHOT.jar"
  )
  software_dir = abspath("${path.module}/../../scripts/experiments/ec2")
  node_roles = {
    for index in range(var.node_count) : index => (
      index == 0 ? "observer" :
      index == 1 ? "target" :
      index == local.coordinator_index ? "coordinator" : "helper"
    )
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "results" {
  bucket        = "${var.experiment_id}-${data.aws_caller_identity.current.account_id}-${random_id.bucket_suffix.hex}"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket = aws_s3_bucket.results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "results" {
  bucket = aws_s3_bucket.results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "lab_jar" {
  bucket = aws_s3_bucket.results.id
  key    = "${var.experiment_id}/software/lab.jar"
  source = local.jar_path
  etag   = filemd5(local.jar_path)
}

resource "aws_s3_object" "runner_files" {
  for_each = fileset(local.software_dir, "*")

  bucket = aws_s3_bucket.results.id
  key    = "${var.experiment_id}/software/${each.value}"
  source = "${local.software_dir}/${each.value}"
  etag   = filemd5("${local.software_dir}/${each.value}")
}

resource "aws_vpc" "experiment" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.experiment_id}-vpc" }
}

resource "aws_internet_gateway" "experiment" {
  vpc_id = aws_vpc.experiment.id
  tags   = { Name = "${var.experiment_id}-igw" }
}

resource "aws_subnet" "experiment" {
  vpc_id                  = aws_vpc.experiment.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = { Name = "${var.experiment_id}-subnet" }
}

resource "aws_route_table" "experiment" {
  vpc_id = aws_vpc.experiment.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.experiment.id
  }

  tags = { Name = "${var.experiment_id}-routes" }
}

resource "aws_route_table_association" "experiment" {
  subnet_id      = aws_subnet.experiment.id
  route_table_id = aws_route_table.experiment.id
}

resource "aws_security_group" "nodes" {
  name        = "${var.experiment_id}-nodes"
  description = "OpenKache detector traffic restricted to ${var.node_count} experiment nodes"
  vpc_id      = aws_vpc.experiment.id

  tags = { Name = "${var.experiment_id}-nodes" }
}

resource "aws_vpc_security_group_ingress_rule" "udp_detector" {
  security_group_id            = aws_security_group.nodes.id
  referenced_security_group_id = aws_security_group.nodes.id
  ip_protocol                  = "udp"
  from_port                    = 19000
  to_port                      = 19000
  description                  = "Detector UDP and control frames"
}

resource "aws_vpc_security_group_ingress_rule" "tcp_detector" {
  security_group_id            = aws_security_group.nodes.id
  referenced_security_group_id = aws_security_group.nodes.id
  ip_protocol                  = "tcp"
  from_port                    = 19001
  to_port                      = 19001
  description                  = "Product-inspired persistent TCP heartbeat"
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  count = var.allowed_admin_cidr == "" ? 0 : 1

  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = var.allowed_admin_cidr
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  description       = "Optional break-glass SSH; SSM is preferred"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_iam_role" "node" {
  name = "${var.experiment_id}-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "node_s3" {
  name = "experiment-results"
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.results.arn,
        "${aws_s3_bucket.results.arn}/${var.experiment_id}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "${var.experiment_id}-node"
  role = aws_iam_role.node.name
}

resource "aws_iam_role" "coordinator" {
  name               = "${var.experiment_id}-coordinator"
  assume_role_policy = aws_iam_role.node.assume_role_policy
}

resource "aws_iam_role_policy_attachment" "coordinator_ssm" {
  role       = aws_iam_role.coordinator.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "coordinator" {
  name = "campaign-control"
  role = aws_iam_role.coordinator.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.results.arn,
          "${aws_s3_bucket.results.arn}/${var.experiment_id}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances", "ssm:ListCommands", "ssm:ListCommandInvocations", "ssm:GetCommandInvocation"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = "ssm:SendCommand"
        Resource = [
          "arn:${data.aws_partition.current.partition}:ssm:${var.aws_region}::document/AWS-RunShellScript",
          "arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "ec2:StopInstances"
        Resource = "arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*"
        Condition = {
          StringEquals = { "ec2:ResourceTag/ExperimentId" = var.experiment_id }
        }
      }
    ]
  })
}

resource "aws_iam_instance_profile" "coordinator" {
  name = "${var.experiment_id}-coordinator"
  role = aws_iam_role.coordinator.name
}

resource "aws_instance" "node" {
  count = var.node_count

  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.experiment.id
  vpc_security_group_ids      = [aws_security_group.nodes.id]
  associate_public_ip_address = true
  iam_instance_profile        = count.index == local.coordinator_index ? aws_iam_instance_profile.coordinator.name : aws_iam_instance_profile.node.name
  user_data_replace_on_change = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gib
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/user-data.sh.tftpl", {
    aws_region              = var.aws_region
    bucket                  = aws_s3_bucket.results.id
    experiment_id           = var.experiment_id
    node_id                 = count.index
    node_role               = local.node_roles[count.index]
    expected_nodes          = var.node_count
    repetitions             = var.repetitions
    auto_stop_on_completion = var.auto_stop_on_completion ? "1" : "0"
    jar_sha256              = filesha256(local.jar_path)
  })

  depends_on = [
    aws_s3_object.lab_jar,
    aws_s3_object.runner_files,
    aws_iam_role_policy_attachment.node_ssm,
    aws_iam_role_policy_attachment.coordinator_ssm
  ]

  tags = {
    Name          = format("%s-node-%02d", var.experiment_id, count.index)
    NodeId        = tostring(count.index)
    Role          = local.node_roles[count.index]
    StorageMedium = "local-nvme-instance-store"
  }

  lifecycle {
    precondition {
      condition     = fileexists(local.jar_path)
      error_message = "Build the failure-detector-lab JAR before terraform plan/apply."
    }
    precondition {
      condition     = contains(data.aws_ec2_instance_type.selected.supported_architectures, var.instance_architecture)
      error_message = "instance_architecture must be supported by instance_type."
    }
    precondition {
      condition     = data.aws_ec2_instance_type.selected.default_vcpus == 1
      error_message = "The experiment instance type must provide exactly one vCPU."
    }
    precondition {
      condition     = !data.aws_ec2_instance_type.selected.burstable_performance_supported
      error_message = "Burstable instance types are not allowed because CPU credits bias the benchmark."
    }
    precondition {
      condition     = can(regex("^m6gd\\.", var.instance_type))
      error_message = "Use an m6gd instance type so compute and local-NVMe characteristics stay in the selected family."
    }
    precondition {
      condition     = data.aws_ec2_instance_type.selected.instance_storage_supported && data.aws_ec2_instance_type.selected.total_instance_storage >= 59
      error_message = "The selected instance type must provide at least 59 GB of local instance storage."
    }
  }
}
