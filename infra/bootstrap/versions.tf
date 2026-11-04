terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = "ap-northeast-2"
  profile = var.aws_profile

  # 다른 계정이면 plan 전에 실패한다. 같은 계정의 다른 IAM 사용자는 identity.tf가 막는다.
  allowed_account_ids = [var.account_id]

  default_tags {
    tags = {
      Project   = "openkache"
      ManagedBy = "terraform-bootstrap"
    }
  }
}
