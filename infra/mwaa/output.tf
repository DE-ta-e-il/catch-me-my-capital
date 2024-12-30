output "mwaa_webserver_url" {
  description = "MWAA 웹서버 URL"
  value       = module.mwaa.mwaa_webserver_url
}

output "mwaa_arn" {
  description = "MWAA의 ARN"
  value       = module.mwaa.mwaa_arn
}

output "mwaa_service_role_arn" {
  description = "MWAA의 Service Role"
  value       = module.mwaa.mwaa_service_role_arn
}

output "mwaa_status" {
  description = "MWAA의 상태"
  value       = module.mwaa.mwaa_status
}

output "mwaa_role_arn" {
  description = "MWAA의 IAM Role ARN"
  value       = module.mwaa.mwaa_role_arn
}

output "aws_s3_bucket_name" {
  description = "MWAA 환경 관련 S3 버킷명"
  value       = local.bucket_name
}

output "vpc_id" {
  description = "MWAA VPC ID"
  value       = module.vpc.vpc_id
}

output "subnets" {
  description = "MWAA VPC Private Subnets"
  value       = module.vpc.private_subnets
}
