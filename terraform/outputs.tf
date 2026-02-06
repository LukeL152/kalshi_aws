output "worker_instance_ids" {
  value = aws_instance.worker[*].id
}

output "worker_public_ips" {
  value = aws_instance.worker[*].public_ip
}

output "s3_bucket" {
  value = aws_s3_bucket.ingest.bucket
}
