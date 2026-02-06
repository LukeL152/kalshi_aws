variable "project_name" {
  description = "Prefix used for named AWS resources."
  type        = string
  default     = "s3-shard-ingest"
}

variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "S3 bucket used for shard files and output parquet files."
  type        = string
}

variable "worker_count" {
  description = "Number of EC2 workers to run."
  type        = number
  default     = 2
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "t4g.micro"
}

variable "key_name" {
  description = "Optional EC2 key pair name for SSH access."
  type        = string
  default     = null
}

variable "ssh_ingress_cidrs" {
  description = "CIDR blocks allowed to SSH to workers. Leave empty to disable SSH ingress."
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = "Whether worker instances receive public IP addresses."
  type        = bool
  default     = true
}

variable "subnet_ids" {
  description = "Optional subnet IDs for workers. If empty, default VPC subnets are used."
  type        = list(string)
  default     = []
}

variable "repo_url" {
  description = "Git repository URL to clone on worker startup."
  type        = string
  default     = "https://github.com/your-org/s3-shard-ingest-worker.git"
}

variable "repo_ref" {
  description = "Git ref to checkout (branch/tag/sha)."
  type        = string
  default     = "main"
}

variable "shard_prefix" {
  description = "S3 shard prefix (for pending/running/completed shard DB files)."
  type        = string
  default     = "shards/live"
}

variable "output_prefix" {
  description = "S3 prefix for parquet output data."
  type        = string
  default     = "candlesticks_raw"
}

variable "worker_log_prefix" {
  description = "S3 prefix where worker logs are uploaded."
  type        = string
  default     = "worker_logs"
}

variable "max_rps" {
  description = "Maximum request rate per worker."
  type        = number
  default     = 4
}

variable "threads" {
  description = "DuckDB thread count per worker."
  type        = number
  default     = 2
}

variable "memory_limit" {
  description = "DuckDB memory limit per worker."
  type        = string
  default     = "512MB"
}

variable "tags" {
  description = "Additional tags to apply to resources."
  type        = map(string)
  default     = {}
}
