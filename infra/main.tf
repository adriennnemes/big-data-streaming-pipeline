terraform {
  required_version = ">= 1.6.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    fivetran = {
      source  = "fivetran/fivetran"
      version = "~> 1.0"
    }
  }
}

provider "confluent" {}
provider "google" {}
provider "fivetran" {}
