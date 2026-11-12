variable "aws_region" {
  description = "AWS region for the isolated experiment VPC."
  type        = string
  default     = "ap-northeast-2"
}

variable "experiment_id" {
  description = "Unique identifier used for tags, S3 prefixes, and the campaign lock."
  type        = string
  default     = "openkache-fd-ec2"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,47}$", var.experiment_id))
    error_message = "experiment_id must be 3-48 lowercase letters, digits, or hyphens."
  }
}

variable "instance_type" {
  description = "M6gd instance type used for every node; the experiment data path uses its local NVMe instance store."
  type        = string
  default     = "m6gd.medium"
}

variable "instance_architecture" {
  description = "CPU architecture used to select the matching Amazon Linux 2023 AMI."
  type        = string
  default     = "arm64"

  validation {
    condition     = contains(["arm64", "x86_64"], var.instance_architecture)
    error_message = "instance_architecture must be arm64 or x86_64."
  }
}

variable "node_count" {
  description = "Failure-detector cluster size. Five matches the production-sized validation topology."
  type        = number
  default     = 5

  validation {
    condition     = var.node_count >= 5 && var.node_count <= 8 && floor(var.node_count) == var.node_count
    error_message = "node_count must be an integer between 5 and 8."
  }
}

variable "repetitions" {
  description = "Runs per detector and fault for the initial EC2 campaign."
  type        = number
  default     = 10

  validation {
    condition     = var.repetitions >= 1 && var.repetitions <= 30
    error_message = "repetitions must be between 1 and 30."
  }
}

variable "artifact_jar" {
  description = "Optional JAR path. Empty uses failure-detector-lab/build/libs from this repository."
  type        = string
  default     = ""
}

variable "root_volume_gib" {
  type    = number
  default = 20
}

variable "auto_stop_on_completion" {
  description = "Stop all experiment instances after final validation and S3 upload."
  type        = bool
  default     = true
}

variable "allowed_admin_cidr" {
  description = "Optional SSH source CIDR. Keep empty to manage only through SSM."
  type        = string
  default     = ""
}
