variable "mwaa_version" {
  description = "(Required) MWAA 버전"
  type        = string
  default     = "2.10.3"
}

variable "name" {
  description = "(Required) MWAA 환경 이름"
  type        = string
  default     = "MWAA_Environment"
}

variable "vpc_cidr" {
  description = "(Required) MWAA 환경이 구축될 VPC의 CIDR 블록"
  type        = string
  default     = "10.1.0.0/16"
}

variable "tags" {
  description = "(Optional) 자원 Tag 설정"
  type        = map(string)
  default     = {}
}

variable "region" {
  description = "(Required) MWAA 환경 리전 이름"
  type        = string
  default     = "ap-northeast-2"
}
