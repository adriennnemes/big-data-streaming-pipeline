resource "google_bigquery_dataset" "movies" {
  dataset_id = "movies"
  location   = "US"
  default_partition_expiration_ms = 2592000000
  default_table_expiration_ms = 2592000000

  delete_contents_on_destroy = true
}