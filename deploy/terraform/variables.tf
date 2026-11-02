variable "project_id" {
  type        = string
  description = "GCP Project ID"
  default     = "system-architect-dev"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Deployment environment (dev, staging, prod)"
}
