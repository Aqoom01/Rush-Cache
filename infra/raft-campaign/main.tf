terraform {
  required_version = ">= 1.13.0"
  backend "s3" {}
  required_providers {
    aws = { source = "hashicorp/aws", version = "5.100.0" }
  }
}

variable "campaign_id" {
  type = string
  validation {
    condition     = can(regex("^raft-ec2-[0-9a-f]{12}$", var.campaign_id))
    error_message = "A reserved detached campaign ID is required."
  }
}
variable "account_id" { type = string }
variable "subnet_id" { type = string }
variable "ami_id" { type = string }
variable "results_bucket" { type = string }

provider "aws" {
  region = "ap-northeast-2"
  default_tags {
    tags = {
      Project    = "openkache"
      ManagedBy  = "terraform-raft-campaign"
      CampaignId = var.campaign_id
    }
  }
}
data "aws_caller_identity" "current" {}
data "aws_subnet" "selected" { id = var.subnet_id }
data "aws_instances" "existing" { instance_state_names = ["pending", "running", "stopping", "stopped"] }
data "aws_instances" "ours" {
  instance_tags        = { CampaignId = var.campaign_id }
  instance_state_names = ["pending", "running", "stopping", "stopped"]
}
locals {
  name  = "openkache-${var.campaign_id}"
  trust = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }] })
  s3_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = "arn:aws:s3:::${var.results_bucket}", Condition = { StringLike = { "s3:prefix" = [var.campaign_id, "${var.campaign_id}/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = "arn:aws:s3:::${var.results_bucket}/${var.campaign_id}/*" }
    ]
  })
}
resource "aws_security_group" "cluster" {
  name        = local.name
  description = "Isolated Raft experiment peers, no public inbound access"
  vpc_id      = data.aws_subnet.selected.vpc_id
  ingress {
    protocol  = "tcp"
    from_port = 7000
    to_port   = 7001
    self      = true
  }
  ingress {
    protocol  = "udp"
    from_port = 7002
    to_port   = 7002
    self      = true
  }
  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = local.name }
}
resource "aws_iam_role" "node" {
  name               = "${local.name}-node"
  assume_role_policy = local.trust
}
resource "aws_iam_role" "coordinator" {
  name               = "${local.name}-coord"
  assume_role_policy = local.trust
}
resource "aws_iam_role_policy_attachment" "node_ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
resource "aws_iam_role_policy_attachment" "coord_ssm" {
  role       = aws_iam_role.coordinator.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
resource "aws_iam_role_policy" "node_s3" {
  role   = aws_iam_role.node.id
  name   = "campaign-results"
  policy = local.s3_policy
}
resource "aws_iam_role_policy" "coord_s3" {
  role   = aws_iam_role.coordinator.id
  name   = "campaign-results"
  policy = local.s3_policy
}
resource "aws_iam_instance_profile" "node" {
  name = aws_iam_role.node.name
  role = aws_iam_role.node.name
}
resource "aws_iam_instance_profile" "coordinator" {
  name = aws_iam_role.coordinator.name
  role = aws_iam_role.coordinator.name
}
resource "aws_instance" "node" {
  count                       = 3
  ami                         = var.ami_id
  instance_type               = "m6gd.medium"
  subnet_id                   = var.subnet_id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  iam_instance_profile        = count.index == 2 ? aws_iam_instance_profile.coordinator.name : aws_iam_instance_profile.node.name
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    node = count.index, bucket = var.results_bucket, campaign = var.campaign_id
  })
  metadata_options { http_tokens = "required" }
  root_block_device {
    volume_type           = "gp3"
    volume_size           = 20
    encrypted             = true
    delete_on_termination = true
  }
  tags       = { Name = "${local.name}-node-${count.index}", Role = count.index == 2 ? "coordinator" : "node" }
  depends_on = [aws_iam_role_policy_attachment.node_ssm, aws_iam_role_policy_attachment.coord_ssm, aws_iam_role_policy.node_s3, aws_iam_role_policy.coord_s3]
  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.account_id == var.account_id && endswith(data.aws_caller_identity.current.arn, ":user/OpenStd")
      error_message = "Only the verified OpenStd experiment account may apply."
    }
    precondition {
      condition     = length(data.aws_instances.existing.ids) - length(data.aws_instances.ours.ids) + 3 <= 9
      error_message = "The account would exceed the 9-instance experiment limit."
    }
  }
}
resource "aws_iam_role_policy" "coordinator_control" {
  name = "control-exact-three-nodes"
  role = aws_iam_role.coordinator.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["ec2:DescribeInstances", "ssm:GetCommandInvocation"], Resource = "*" },
    { Effect = "Allow", Action = ["ssm:SendCommand"], Resource = concat(["arn:aws:ssm:ap-northeast-2::document/AWS-RunShellScript"], aws_instance.node[*].arn) },
    { Effect = "Allow", Action = ["ec2:StopInstances"], Resource = aws_instance.node[*].arn }
  ] })
}
output "instance_ids" { value = aws_instance.node[*].id }
output "private_ips" { value = aws_instance.node[*].private_ip }
output "coordinator" { value = aws_instance.node[2].id }
output "campaign_id" { value = var.campaign_id }
output "security_group" { value = aws_security_group.cluster.id }
