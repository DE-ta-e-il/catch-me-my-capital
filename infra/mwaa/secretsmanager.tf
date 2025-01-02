resource "aws_secretsmanager_secret" "S3_BUCKET" {
  name = "S3_BUCKET"
}

resource "aws_secretsmanager_secret_version" "S3_BUCKET" {
  secret_id     = aws_secretsmanager_secret.S3_BUCKET.id
  secret_string = ""
}

resource "aws_secretsmanager_secret" "aws_conn_id" {
  name = "aws_conn_id"
}

resource "aws_secretsmanager_secret_version" "aws_conn_id" {
  secret_id = aws_secretsmanager_secret.aws_conn_id.id
  secret_string = jsonencode({
    conn_type = "aws"
    login     = var.aws_conn_login
    password  = var.aws_conn_pw
    extra     = { region_name = var.region }
  })
}
