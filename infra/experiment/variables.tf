variable "account_id" {
  description = "OpenKache AWS 계정 번호(12자리)."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "account_id는 12자리 숫자여야 한다."
  }
}

variable "iam_user_name" {
  description = "Terraform을 실행하는 프로젝트 IAM 사용자."
  type        = string
  default     = "OpenStd"
}

variable "storage_engine" {
  description = "cache-server KV 저장소. nvme면 instance store를 포맷해 쓴다(docs/storage-nvme.md)."
  type        = string
  default     = "memory"

  validation {
    condition     = contains(["memory", "nvme"], var.storage_engine)
    error_message = "storage_engine은 memory 또는 nvme여야 한다."
  }
}

variable "node_count" {
  description = "cache-server 노드 수. 교보재 조건상 최대 8대(부하 생성 1대는 별도)."
  type        = number
  default     = 1

  validation {
    condition     = var.node_count >= 1 && var.node_count <= 8
    error_message = "node_count는 1-8이어야 한다(교보재 조건: cache-server 8대 이하)."
  }
}

variable "instance_type" {
  description = "교보재 조건으로 m6gd.medium만 쓴다."
  type        = string
  default     = "m6gd.medium"

  validation {
    condition     = var.instance_type == "m6gd.medium"
    error_message = "m6gd.medium만 쓸 수 있다."
  }
}

variable "max_lifetime_minutes" {
  description = "생성 후 이 시간이 지나면 EventBridge Scheduler가 인스턴스를 정지한다."
  type        = number
  default     = 60

  validation {
    condition     = var.max_lifetime_minutes >= 15 && var.max_lifetime_minutes <= 180
    error_message = "max_lifetime_minutes는 15-180이어야 한다."
  }
}

variable "source_commit" {
  description = "노드가 빌드할 commit. scripts/up.sh가 bundles/<commit>.bundle로 올린다."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.source_commit))
    error_message = "source_commit은 40자리 commit 해시여야 한다."
  }
}
