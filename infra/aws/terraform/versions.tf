terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  # Credentials are never configured here. The provider uses the standard AWS
  # credential chain (env vars, shared config/profile, or an IAM role when run from
  # CI) — exactly the same rule the application itself follows for its own AWS SDK
  # usage. Nothing in this repo ever hardcodes an access key.
}
