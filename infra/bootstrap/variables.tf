variable "account_id" {
  description = "OpenKache AWS 계정 번호(12자리). 다른 계정에서 실행되는 것을 막는다."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "account_id는 12자리 숫자여야 한다."
  }
}

variable "aws_profile" {
  description = "AWS CLI named profile. default 프로필은 이전 프로젝트 계정이므로 쓰지 않는다."
  type        = string
  default     = "openstd"

  validation {
    condition     = var.aws_profile != "default"
    error_message = "default 프로필은 쓰지 않는다."
  }
}

variable "iam_user_name" {
  description = "Terraform을 실행하는 프로젝트 IAM 사용자. 실험용 최소 권한 정책을 여기에 붙인다."
  type        = string
}

variable "bucket_prefix" {
  description = "S3 버킷 이름 앞부분. 버킷 이름은 전 세계에서 유일해야 한다."
  type        = string
  default     = "openstd-s15p31d111"
}

variable "budget_email" {
  description = "월 비용 알림을 받을 이메일."
  type        = string
}

variable "budget_limit_usd" {
  description = "월 예산(USD). 27만 원을 1,500원/USD로 환산한 값."
  type        = number
  default     = 180
}

variable "budget_alert_percent" {
  description = "실제 비용이 예산의 몇 %에 도달하면 메일을 보낼지."
  type        = number
  default     = 50
}
