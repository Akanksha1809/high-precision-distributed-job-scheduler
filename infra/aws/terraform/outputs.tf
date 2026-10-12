output "api_url" {
  value       = "http://${aws_lb.api.dns_name}"
  description = "Public URL for the Chronos API (HTTP only in demo mode — put a real TLS cert via ACM + an HTTPS listener before using this for anything real)"
}

output "ecr_repository_url" {
  value = aws_ecr_repository.chronos.repository_url
}

output "main_queue_url" {
  value = aws_sqs_queue.main.id
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}

output "rds_endpoint" {
  value     = aws_db_instance.main.address
  sensitive = false
}

output "db_credentials_secret_arn" {
  value = aws_secretsmanager_secret.db_credentials.arn
}
