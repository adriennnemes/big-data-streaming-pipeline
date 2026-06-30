########################################
# Environment + Cluster
########################################

resource "confluent_environment" "main" {
  display_name = "bigdata_project"
}

resource "confluent_kafka_cluster" "main" {
  display_name = "movies"
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = "us-east1"
  standard {}

  environment {
    id = confluent_environment.main.id
  }
}

########################################
# Admin SA: used by Terraform to manage Kafka resources (topics, etc.)
########################################

resource "confluent_service_account" "admin" {
  display_name = "tf-admin"
  description  = "Terraform admin for managing the Kafka cluster"
}

resource "confluent_role_binding" "admin_cluster_admin" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_api_key" "admin_kafka_api_key" {
  display_name = "tf-admin-kafka"
  description  = "Kafka API key used by Terraform to manage topics"

  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.admin_cluster_admin
  ]
}

########################################
# Kafka Topic
########################################

resource "confluent_kafka_topic" "raw" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name    = "raw"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.admin_kafka_api_key.id
    secret = confluent_api_key.admin_kafka_api_key.secret
  }
}

resource "confluent_kafka_topic" "aggregates" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name    = "aggregates"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.admin_kafka_api_key.id
    secret = confluent_api_key.admin_kafka_api_key.secret
  }
}

resource "confluent_kafka_topic" "alerts" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name    = "alerts"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.admin_kafka_api_key.id
    secret = confluent_api_key.admin_kafka_api_key.secret
  }
}
