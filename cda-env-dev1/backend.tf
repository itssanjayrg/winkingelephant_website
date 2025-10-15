terraform {
  required_version = "~> 1.9.0"
  required_providers {
    aws = {
      source = "hashicorp/aws"
      #  Lock version to avoid unexpected problems
      version = "5.70.0"
    }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  assume_role {
    role_arn = var.cda_deployment_role
  }
  default_tags {
    tags = {
      assignment-group        = "edw"
      environment             = var.environment
      application-support     = "edw"
      business-application    = "guidewire cda"
      business-criticality    = "business critical"
      device-function         = "edw"
      owner                   = "ryan.downing@encova.com"
      location                = var.region
      budget-cost-center      = "40013"
      budget-line-item-detail = ""
      data-classification     = "highly confidential"
      deployment-method       = "terraform"
    }
  }

}
