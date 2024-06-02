terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.30.0"
    }
  }
}

provider "google" {
    credentials = "/workspaces/NYC-Accidents/terraform/keys/gcp.creds.json"
    project= "nyc-accidents-423921"
    region= "us-central1"
}

resource "google_storage_bucket" "nyc_accidents" {
  name          = "nyc-accidents-bucket_2024"
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "nyc_accidents_bq" {
  dataset_id = "Nyc_Accidents"
  project    = "nyc-accidents-423921"
  location   = "US"
}
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  project    = "nyc-accidents-423921"
  location   = "US"
}

#   lifecycle_rule {
#     condition {
#       age = 3
#     }
#     action {
#       type = "Delete"
#     }
#   }

#   lifecycle_rule {
#     condition {
#       age = 1
#     }
#     action {
#       type = "AbortIncompleteMultipartUpload"
#     }
#   }
