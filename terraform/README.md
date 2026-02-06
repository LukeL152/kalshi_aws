# Terraform Deployment

This Terraform config launches EC2 workers that run `python3 -m ingest_shards_ec2` and self-terminate after work is done.

## Quick Start

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

## Required Inputs

- `bucket_name`: globally unique S3 bucket name

## Common Inputs

- `worker_count`: number of workers to launch
- `repo_url`: repository workers should clone
- `repo_ref`: git ref workers should checkout
- `shard_prefix`: shard DB prefix (`pending/running/completed`)
- `output_prefix`: parquet output prefix
- `ssh_ingress_cidrs`: CIDRs allowed to SSH; empty disables SSH ingress

## Security Defaults

- S3 public access block is enabled.
- No SSH ingress is opened unless you set `ssh_ingress_cidrs`.
- IAM policy is limited to the configured bucket objects and list operations.

## Notes

- `terraform.tfstate` and `terraform.tfvars` are intentionally git-ignored.
- `user_data.sh.tmpl` is parameterized and has no personal bucket/account values.
