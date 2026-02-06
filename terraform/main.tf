provider "aws" {
  region = var.region
}

locals {
  common_tags = merge(
    {
      Project   = var.project_name
      ManagedBy = "terraform"
    },
    var.tags
  )
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  target_subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.default.ids
}

resource "aws_s3_bucket" "ingest" {
  bucket        = var.bucket_name
  force_destroy = false

  tags = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "ingest" {
  bucket = aws_s3_bucket.ingest.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_role" "worker" {
  name = "${var.project_name}-worker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "worker_s3" {
  name = "${var.project_name}-worker-s3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.ingest.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["*"]
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:CopyObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "${aws_s3_bucket.ingest.arn}/*"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "worker_s3" {
  role       = aws_iam_role.worker.name
  policy_arn = aws_iam_policy.worker_s3.arn
}

resource "aws_iam_instance_profile" "worker" {
  name = "${var.project_name}-worker-profile"
  role = aws_iam_role.worker.name

  tags = local.common_tags
}

resource "aws_security_group" "worker" {
  name        = "${var.project_name}-worker-sg"
  description = "Security group for shard ingest workers"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = var.ssh_ingress_cidrs
    content {
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }
}

resource "aws_instance" "worker" {
  count = var.worker_count

  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.instance_type
  key_name                    = var.key_name
  iam_instance_profile        = aws_iam_instance_profile.worker.name
  vpc_security_group_ids      = [aws_security_group.worker.id]
  subnet_id                   = local.target_subnet_ids[count.index % length(local.target_subnet_ids)]
  associate_public_ip_address = var.assign_public_ip

  instance_initiated_shutdown_behavior = "terminate"

  user_data = templatefile("${path.module}/user_data.sh.tmpl", {
    project_name      = var.project_name
    region            = var.region
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    s3_bucket         = var.bucket_name
    shard_prefix      = var.shard_prefix
    s3_prefix         = var.output_prefix
    worker_log_prefix = var.worker_log_prefix
    max_rps           = tostring(var.max_rps)
    threads           = tostring(var.threads)
    memory_limit      = var.memory_limit
  })

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-worker-${count.index}"
    }
  )
}
