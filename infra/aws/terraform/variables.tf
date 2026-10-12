variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "chronos"
}

variable "environment" {
  type    = string
  default = "demo"
}

# --- Cost-conscious demo mode toggle ---
# true  (default): single-AZ RDS, no Multi-AZ, smallest instance sizes, ECS tasks in
#                  public subnets with no NAT Gateway (NAT Gateway alone runs ~$32/mo
#                  plus data charges even when idle) — acceptable for a demo that
#                  gets torn down, NOT for production.
# false: still deliberately conservative defaults (this is a portfolio project, not
#        a production deployment guide) but documents where you'd harden it —
#        private subnets + NAT, Multi-AZ RDS, deletion protection. See
#        infra/aws/DEPLOYMENT.md "Hardening for real production" section.
variable "demo_mode" {
  type    = bool
  default = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_name" {
  type    = string
  default = "chronos"
}

variable "db_username" {
  type    = string
  default = "chronos"
}

variable "container_image" {
  description = "ECR image URI (with tag) shared by all five services — they differ only by container command, same as docker-compose.yml locally."
  type        = string
}

variable "api_desired_count" {
  type    = number
  default = 1
}

variable "worker_desired_count" {
  type    = number
  default = 1
}

variable "dispatcher_desired_count" {
  type    = number
  default = 1
}

variable "outbox_publisher_desired_count" {
  type    = number
  default = 1
}

variable "reconciliation_desired_count" {
  type    = number
  default = 1
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "alarm_notification_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms. Leave empty to create alarms with no action (visible in console, no notification) — set this once you have somewhere for them to go."
  type        = string
  default     = ""
}
