terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state stored in S3 with DynamoDB locking
  # Update bucket/table names to match your AWS account before first init
  backend "s3" {
    bucket         = "nishant-test-terraform-state"
    key            = "nishant-test/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "nishant-test-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Nishant_Test"
      ManagedBy   = "terraform"
      Repository  = "Nishant-Karri/Nishant_Test"
    }
  }
}
