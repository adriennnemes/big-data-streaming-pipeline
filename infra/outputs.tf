output "kafka_bootstrap_servers" {
  value = confluent_kafka_cluster.main.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  value = confluent_kafka_cluster.main.rest_endpoint
}

output "kafka_topic_raw" {
  value = confluent_kafka_topic.raw.topic_name
}

output "kafka_topic_aggregates" {
  value = confluent_kafka_topic.aggregates.topic_name
}

output "kafka_topic_alerts" {
  value = confluent_kafka_topic.alerts.topic_name
}

output "kafka_admin_api_key" {
  value = confluent_api_key.admin_kafka_api_key.id
}

output "kafka_admin_api_secret" {
  value     = confluent_api_key.admin_kafka_api_key.secret
  sensitive = true
}
