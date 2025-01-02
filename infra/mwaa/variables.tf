variable "mwaa_version" {
  description = "MWAA 버전"
  type        = string
  default     = "2.10.3"
}

variable "project" {
  description = "프로젝트 이름"
  type        = string
  default     = "CatchMeMyCapital"
}

variable "environment" {
  description = "배포 환경(Dev, Prod)"
  type        = string
  default     = "Dev"
}


variable "vpc_cidr" {
  description = "MWAA 환경이 구축될 VPC의 CIDR 블록"
  type        = string
  default     = "10.1.0.0/16"
}

variable "region" {
  description = "MWAA 환경 리전 이름"
  type        = string
  default     = "ap-northeast-2"
}


variable "tags" {
  description = "자원 Tag 설정"
  type        = map(string)
  default = {
    "Owner"     = "TeamDetaeil"
    "ManagedBy" = "Terraform"
  }
}

locals {
  mwaa_name = "${var.project}-${var.environment}"
  all_tags = merge(var.tags, {
    "Region" : var.region,
    "Environment" : var.environment
  })
}

variable "aws_conn_login" {
  description = "aws iam id"
  type        = string
  default     = ""
  sensitive   = true
}

variable "aws_conn_pw" {
  description = "aws connection secret"
  type        = string
  default     = ""
  sensitive   = true
}
