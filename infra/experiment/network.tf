# NAT Gateway 없이 퍼블릭 서브넷에 두고 inbound는 서브넷 안에서만 연다. 접속은 SSM.
resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "openkache-experiment" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "openkache-experiment" }
}

data "aws_ec2_instance_type_offerings" "m6gd" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = sort(data.aws_ec2_instance_type_offerings.m6gd.locations)[0]
  map_public_ip_on_launch = true

  tags = { Name = "openkache-experiment-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "openkache-experiment-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "node" {
  name        = "openkache-node"
  description = "cache-server ports within the subnet only"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "node_ports" {
  for_each = { client = 7000, peer = 7001, metrics = 9091 }

  security_group_id = aws_security_group.node.id
  description       = each.key
  ip_protocol       = "tcp"
  from_port         = each.value
  to_port           = each.value
  cidr_ipv4         = aws_subnet.public.cidr_block
}

resource "aws_vpc_security_group_ingress_rule" "failure_detector" {
  security_group_id = aws_security_group.node.id
  description       = "swim-failure-detector"
  ip_protocol       = "udp"
  from_port         = 7002
  to_port           = 7002
  cidr_ipv4         = aws_subnet.public.cidr_block
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.node.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
