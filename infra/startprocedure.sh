source ./addsecret.sh

aws secretsmanager delete-secret \
    --secret-id aws_conn_id \
    --force-delete-without-recovery

aws secretsmanager delete-secret \
    --secret-id S3_BUCKET \
    --force-delete-without-recovery

cd ./mwaa \
&& terraform init \
&& terraform plan \
&& terraform apply -auto-approve
