variable "region" {
  type    = string
  default = "ap-south-1"
}

# Restrict provider to expected account
variable "account_id" {
  type = string
}

# "none" keeps just the base up
variable "streamer" {
  type    = string
  default = "none"

  validation {
    condition     = contains(["walshadow", "peerdb", "pg", "none"], var.streamer)
    error_message = "streamer must be walshadow, peerdb, pg or none."
  }
}

variable "clickhouse" {
  type    = bool
  default = true

  validation {
    condition     = var.clickhouse || !contains(["walshadow", "peerdb"], var.streamer)
    error_message = "CDC streamers (walshadow, peerdb) need the clickhouse node."
  }
}

# The bench driver runs here, beside the stack — its round trip lands in every
# latency sample, so a workstation run measures the operator's link instead.
variable "bench_runner" {
  type    = bool
  default = true
}

variable "instance_type" {
  type    = string
  default = "c8i.2xlarge"
}

# empty picks the lowest-sorted AZ offering instance_type
variable "az" {
  type    = string
  default = ""
}

# operator /32 for SG ingress; empty auto-detects via checkip.amazonaws.com
variable "my_ip" {
  type    = string
  default = ""
}
