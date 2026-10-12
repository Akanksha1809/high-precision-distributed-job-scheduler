# One log group per service — matches the structured JSON logs each service already
# emits locally (LOG_FORMAT=json in production, vs console for local dev; see
# app/observability/logging.py), so CloudWatch Logs Insights can query them as
# structured fields directly, no parsing needed.

locals {
  log_group_services = ["api", "dispatcher", "outbox-publisher", "worker", "reconciliation"]
}

resource "aws_cloudwatch_log_group" "service" {
  for_each          = toset(local.log_group_services)
  name              = "/ecs/${var.project_name}-${var.environment}/${each.key}"
  retention_in_days = var.log_retention_days
}

# --- Alarms ---
# Demo-mode observability trade-off: Prometheus/Grafana (already built for local
# dev — see docker-compose.yml) aren't deployed to AWS here, because running them
# in the cloud is itself an extra always-on cost (another Fargate task or EC2
# instance) that a cost-conscious demo shouldn't default to. CloudWatch's native
# metrics + log-based metric filters cover the same operationally-critical signals
# without that extra cost. If you DO want Prometheus/Grafana in AWS, run them as a
# sixth ECS service pointed at the same metrics endpoints — no application code
# changes needed, since every service already exposes /metrics.

resource "aws_cloudwatch_log_metric_filter" "dead_lettered" {
  name           = "${var.project_name}-${var.environment}-dead-lettered"
  log_group_name = aws_cloudwatch_log_group.service["worker"].name
  pattern        = "{ $.event = \"reconciliation_recovered_abandoned_running\" || $.event = \"outbox_publish_exhausted\" }"

  metric_transformation {
    name      = "ChronosDeadLetterEvents"
    namespace = "Chronos/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "dead_letter_events" {
  alarm_name          = "${var.project_name}-${var.environment}-dead-letter-events"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.dead_lettered.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.dead_lettered.metric_transformation[0].namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "A job was routed to the dead-letter queue or an abandoned-running job was recovered — inspect via GET /jobs?status=DEAD_LETTER"
  alarm_actions       = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${var.project_name}-${var.environment}-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }
  alarm_description  = "Messages are sitting in the DLQ — operator inspection needed"
  alarm_actions      = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []
  treat_missing_data = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "main_queue_backlog" {
  alarm_name          = "${var.project_name}-${var.environment}-queue-backlog"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 1000
  dimensions = {
    QueueName = aws_sqs_queue.main.name
  }
  alarm_description  = "Queue backlog sustained above 1000 messages for 15 minutes — workers may be under-provisioned"
  alarm_actions      = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []
  treat_missing_data = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.project_name}-${var.environment}-rds-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.identifier
  }
  alarm_description  = "RDS CPU sustained above 80% — matches the bottleneck pattern documented in BENCHMARKS.md"
  alarm_actions      = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []
  treat_missing_data = "notBreaching"
}
