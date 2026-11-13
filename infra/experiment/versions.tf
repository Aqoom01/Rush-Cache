terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }

  # bootstrap이 만든 버킷. backend 블록은 변수를 쓸 수 없어 값을 직접 적는다.
  # 자격 증명은 환경에서 받는다: 로컬은 scripts/common.sh의 AWS_PROFILE=openstd, CI는 보호 변수의 Access Key.
  backend "s3" {
    bucket       = "openstd-s15p31d111-tfstate"
    key          = "experiment/terraform.tfstate"
    region       = "ap-northeast-2"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region              = "ap-northeast-2"
  allowed_account_ids = [var.account_id]

  default_tags {
    tags = merge({
      Project   = "openkache"
      ManagedBy = var.integration_mode ? "terraform-integration" : "terraform-experiment"
    }, var.integration_mode ? { ExperimentRun = var.experiment_run } : {})
  }
}
