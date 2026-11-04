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
  description = "Use the same non-burstable x86_64 instance type for all eight nodes."
  type        = string
  default     = "c7i.large"
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

variable "data_volume_gib" {
  description = "Dedicated gp3 volume used only by the I/O contention scenario."
  type        = number
  default     = 20
}

variable "auto_stop_on_completion" {
  description = "Stop all eight instances after final validation and S3 upload."
  type        = bool
  default     = true
}

variable "allowed_admin_cidr" {
  description = "Optional SSH source CIDR. Keep empty to manage only through SSM."
  type        = string
  default     = ""
}
