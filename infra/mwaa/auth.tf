data "aws_iam_policy_document" "mwaa_access_to_secrets" {
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      aws_secretsmanager_secret.aws_conn_id.arn
    ]
  }
}

resource "aws_iam_policy" "mwaa_secrets_policy" {
  name   = "MWAASecretsAccessPolicy"
  policy = data.aws_iam_policy_document.mwaa_access_to_secrets.json
}

resource "aws_iam_role_policy_attachment" "mwaa_policy_attachment" {
  role       = module.mwaa.mwaa_role_arn.name
  policy_arn = aws_iam_policy.mwaa_secrets_policy.arn
}
