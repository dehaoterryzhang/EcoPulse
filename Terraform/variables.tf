variable "project" {
  description = "Project"
  default     = "animated-scope-447904-d6"
}

variable "region" {
  description = "Region"
  default     = "us-west1"
}

variable "location" {
  description = "GCS Bucket and BQ Location"
  default     = "us-west1"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "animated-scope-447904-d6-ecopulse-bucket"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ecopulse_bq_dw"
}