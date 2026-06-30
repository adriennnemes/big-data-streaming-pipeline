########################################
# Group
########################################

resource "fivetran_group" "bq" {
  name = "movies_bq"
}

########################################
# Destination
########################################

variable "google_project" {
  type = string
}

variable "google_credentials" {
  type      = string
  sensitive = true
}

resource "fivetran_destination" "bigquery" {
  group_id = fivetran_group.bq.id
  service  = "big_query"

  time_zone_offset = "+1"
  region = "GCP_US_EAST4"

  config {
    project_id        = var.google_project
    secret_key        = var.google_credentials
    data_set_location = google_bigquery_dataset.movies.location
    support_json_type = true
  }

  depends_on = [
    fivetran_group.bq
  ]

  run_setup_tests = true
}

########################################
# Connector
########################################

locals {
  kafka_bootstrap_no_scheme = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")
}

resource "fivetran_connector" "confluent_aggregates" {
  group_id = fivetran_group.bq.id
  service  = "confluent_cloud"

  destination_schema {
    name = google_bigquery_dataset.movies.dataset_id
  } 

  config {
    servers           = toset([local.kafka_bootstrap_no_scheme])
    security_protocol = "SASL"
    api_key           = confluent_api_key.admin_kafka_api_key.id
    api_secret        = confluent_api_key.admin_kafka_api_key.secret

    consumer_group = "movies_bq_consumer"
    message_type   = "Json"
    sync_type      = "Packed"
  }

  depends_on = [
    fivetran_destination.bigquery
  ]

  run_setup_tests = true
}


########################################
# Table definiton
########################################

#resource "fivetran_connector_schema_config" "confluent_schema" {
#  connector_id           = fivetran_connector.confluent_aggregates.id
#  schema_change_handling = "BLOCK_ALL"
#
#  schemas = {
#    "confluent_aggregates" = {
#      enabled = true
#
#      tables = {
#        "aggregates" = {
#          enabled = true
#        }
#      }
#    }
#  }
#
#  depends_on = [fivetran_connector.confluent_aggregates]
#}

########################################
# Start everything
########################################

resource "fivetran_connector_schedule" "confluent_aggregates" {
  connector_id      = fivetran_connector.confluent_aggregates.id
  sync_frequency    = 15
  paused            = false
  pause_after_trial = false

  # depends_on = [fivetran_connector_schema_config.confluent_schema]
}

