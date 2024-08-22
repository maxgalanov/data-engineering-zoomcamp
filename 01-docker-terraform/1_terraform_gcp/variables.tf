variable "credentials" {
  description = "My GCP Credentials"
  default     = "./keys/my-creds.json"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "bq_location" {
  description = "BigQuery location"
  default     = "US"
}