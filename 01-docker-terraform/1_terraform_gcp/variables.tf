variable "credentials" {
  description = "My GCP Credentials"
  default     = "~/.google/credentials/google_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "friendly-idea-433003-e4"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "gbq_dwh_trips_data_all"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "gcs_data_lake_friendly-idea-433003-e4"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}