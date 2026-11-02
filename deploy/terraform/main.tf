provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# 1. VPC & NETWORKING
# ------------------------------------------------------------------------------
resource "google_compute_network" "vpc" {
  name                    = "${var.environment}-event-platform-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.environment}-event-platform-subnet"
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.40.0.0/20"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.50.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.60.0.0/20"
  }

  private_ip_google_access = true
}

# ------------------------------------------------------------------------------
# 2. GKE CLUSTER FOR CONNECTRPC SERVICES & CONSUMERS
# ------------------------------------------------------------------------------
resource "google_container_cluster" "primary" {
  name     = "${var.environment}-event-platform-gke"
  location = var.region

  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.1.0/28"
  }
}

resource "google_container_node_pool" "primary_nodes" {
  name     = "${var.environment}-event-node-pool"
  location = var.region
  cluster  = google_container_cluster.primary.name

  autoscaling {
    min_node_count = 2
    max_node_count = 8
  }

  node_config {
    machine_type = "e2-standard-4"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    labels = {
      environment = var.environment
      role        = "event-consumer"
    }
  }
}

# ------------------------------------------------------------------------------
# 3. TRANSACTIONAL POSTGRESQL (OUTBOX & EVENT STORE)
# ------------------------------------------------------------------------------
resource "google_sql_database_instance" "postgres" {
  name             = "${var.environment}-event-platform-db"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier            = "db-custom-2-7680"
    disk_type       = "PD_SSD"
    disk_autoresize = true

    database_flags {
      name  = "cloudsql.logical_decoding"
      value = "on"
    }

    ip_configuration {
      ipv4_enabled = false
    }
  }

  deletion_protection = false
}

# ------------------------------------------------------------------------------
# 4. OBJECT STORAGE (MINIO / S3 COMPATIBLE EVENT ARCHIVE)
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "event_archive" {
  name          = "${var.project_id}-${var.environment}-event-archive"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}
