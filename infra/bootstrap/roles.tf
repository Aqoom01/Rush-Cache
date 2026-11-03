# 실험 인스턴스가 쓰는 Role. Access Key 없이 SSM 접속과 결과 버킷 접근만 한다.
resource "aws_iam_role" "node" {
  name = "openkache-node"

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
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "node_results" {
  name = "results-bucket"
  role = aws_iam_role.node.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadBundles"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.this["results"].arn}/bundles/*"
      },
      {
        Sid      = "WriteResults"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.this["results"].arn}/results/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "openkache-node"
  role = aws_iam_role.node.name
}

# 최대 수명이 지나면 EventBridge Scheduler가 이 Role로 프로젝트 인스턴스를 정지한다.
resource "aws_iam_role" "scheduler" {
  name = "openkache-scheduler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = var.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_stop" {
  name = "stop-project-instances"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "ec2:StopInstances"
      Resource  = "arn:aws:ec2:ap-northeast-2:${var.account_id}:instance/*"
      Condition = { StringEquals = { "aws:ResourceTag/Project" = "openkache" } }
    }]
  })
}
