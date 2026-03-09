terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state stored in S3
  # DynamoDB locking disabled until nishant-test-terraform-locks table is created by first apply
  backend "s3" {
    bucket  = "nishant-test-terraform-state"
    key     = "nishant-test/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
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
