# bootstrap 이후 프로젝트 IAM 사용자에게 남길 실험용 최소 권한.
# 붙인 뒤에는 AdministratorAccess와 기존 EC2 정책을 콘솔에서 뗀다(README §8).
locals {
  region   = "ap-northeast-2"
  ec2_arn  = "arn:aws:ec2:${local.region}:${var.account_id}"
  in_seoul = { StringEquals = { "aws:RequestedRegion" = local.region } }
  tagged   = { StringEquals = { "aws:ResourceTag/Project" = "openkache" } }
}

data "aws_iam_policy_document" "experiment" {
  statement {
    sid       = "Describe"
    actions   = ["ec2:Describe*", "ec2:GetConsoleOutput"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [local.region]
    }
  }

  # 인스턴스는 m6gd.medium이고 Project 태그가 있을 때만 만든다.
  statement {
    sid       = "RunOnlyM6gdMediumWithTag"
    actions   = ["ec2:RunInstances"]
    resources = ["${local.ec2_arn}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ec2:InstanceType"
      values   = ["m6gd.medium"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Project"
      values   = ["openkache"]
    }
  }

  statement {
    sid     = "RunInstancesDependencies"
    actions = ["ec2:RunInstances"]
    resources = [
      "${local.ec2_arn}:subnet/*",
      "${local.ec2_arn}:security-group/*",
      "${local.ec2_arn}:network-interface/*",
      "${local.ec2_arn}:volume/*",
      "arn:aws:ec2:${local.region}::image/*",
    ]
  }

  statement {
    sid       = "TagOnCreate"
    actions   = ["ec2:CreateTags"]
    resources = ["arn:aws:ec2:${local.region}:${var.account_id}:*/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Project"
      values   = ["openkache"]
    }
  }

  statement {
    sid = "ManageTaggedInstances"
    actions = [
      "ec2:StartInstances",
      "ec2:StopInstances",
      "ec2:TerminateInstances",
      "ec2:ModifyInstanceAttribute",
    ]
    resources = ["${local.ec2_arn}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = ["openkache"]
    }
  }

  # 네트워크 자원은 무료라 서울 리전 안에서만 허용한다.
  # NAT Gateway, Elastic IP, VPC Endpoint는 시간 과금이라 일부러 넣지 않았다.
  statement {
    sid = "Network"
    actions = [
      "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:ModifyVpcAttribute",
      "ec2:CreateSubnet", "ec2:DeleteSubnet", "ec2:ModifySubnetAttribute",
      "ec2:CreateInternetGateway", "ec2:DeleteInternetGateway",
      "ec2:AttachInternetGateway", "ec2:DetachInternetGateway",
      "ec2:CreateRouteTable", "ec2:DeleteRouteTable", "ec2:CreateRoute", "ec2:DeleteRoute",
      "ec2:AssociateRouteTable", "ec2:DisassociateRouteTable",
      "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress", "ec2:RevokeSecurityGroupEgress",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [local.region]
    }
  }

  statement {
    sid       = "AmiLookup"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = ["arn:aws:ssm:${local.region}::parameter/aws/service/*"]
  }

  statement {
    sid       = "StateAndResultsBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.this : b.arn]
  }

  statement {
    sid       = "StateAndResultsObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in aws_s3_bucket.this : "${b.arn}/*"]
  }

  # bootstrap이 만든 Role만 넘길 수 있다. Role을 새로 만들거나 고칠 수는 없다.
  statement {
    sid       = "PassProjectRoles"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.node.arn, aws_iam_role.scheduler.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com", "scheduler.amazonaws.com"]
    }
  }

  statement {
    sid       = "ReadProjectRoles"
    actions   = ["iam:GetRole", "iam:GetInstanceProfile"]
    resources = [aws_iam_role.node.arn, aws_iam_role.scheduler.arn, aws_iam_instance_profile.node.arn]
  }

  statement {
    sid       = "MaxLifetimeSchedules"
    actions   = ["scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:DeleteSchedule", "scheduler:GetSchedule"]
    resources = ["arn:aws:scheduler:${local.region}:${var.account_id}:schedule/default/openkache-*"]
  }

  statement {
    sid       = "IdleAlarms"
    actions   = ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:TagResource", "cloudwatch:ListTagsForResource"]
    resources = ["arn:aws:cloudwatch:${local.region}:${var.account_id}:alarm:openkache-*"]
  }

  statement {
    sid       = "DescribeAlarms"
    actions   = ["cloudwatch:DescribeAlarms"]
    resources = ["*"]
  }

  statement {
    sid       = "SsmOnTaggedInstances"
    actions   = ["ssm:StartSession", "ssm:SendCommand"]
    resources = ["${local.ec2_arn}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/Project"
      values   = ["openkache"]
    }
  }

  statement {
    sid     = "SsmDocuments"
    actions = ["ssm:StartSession", "ssm:SendCommand"]
    resources = [
      "arn:aws:ssm:${local.region}::document/AWS-RunShellScript",
      "arn:aws:ssm:${local.region}:${var.account_id}:document/SSM-SessionManagerRunShell",
    ]
  }

  statement {
    sid       = "SsmResults"
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"]
    resources = ["*"]
  }

  statement {
    sid       = "OwnSessions"
    actions   = ["ssm:TerminateSession", "ssm:ResumeSession"]
    resources = ["arn:aws:ssm:*:${var.account_id}:session/${var.iam_user_name}-*"]
  }

  statement {
    sid       = "WhoAmI"
    actions   = ["sts:GetCallerIdentity"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "experiment" {
  name        = "openkache-experiment"
  description = "OpenKache 실험 클러스터 apply/destroy용 최소 권한 (m6gd.medium, Project=openkache, ap-northeast-2)"
  policy      = data.aws_iam_policy_document.experiment.json
}

resource "aws_iam_user_policy_attachment" "experiment" {
  user       = var.iam_user_name
  policy_arn = aws_iam_policy.experiment.arn
}
