# Two roles per ECS task, same pattern AWS always recommends:
#   - execution role: what ECS itself needs to START the task (pull image, write
#     bootstrap logs, read secrets to inject as env vars). The application code
#     never sees these permissions.
#   - task role: what the APPLICATION CODE running inside the container can do via
#     the AWS SDK (boto3) at runtime — assumed automatically via the container's
#     credential provider chain, which is exactly why app/queue/sqs.py never
#     hardcodes a key: in AWS, boto3 picks this role up from the ECS metadata
#     endpoint with zero code changes from the LocalStack-targeting local setup.
#
# Every policy below is scoped to specific resource ARNs. No "Resource": "*".

data "aws_caller_identity" "current" {}

# --- Execution role (shared by every task definition) ---

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.project_name}-${var.environment}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    sid       = "ReadDbSecretOnly"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.db_credentials.arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "read-db-secret"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

# --- Task role: shared by dispatcher/outbox-publisher/worker/reconciliation ---
# (the API doesn't touch the queue at all — see app/api/main.py — so it gets no
# SQS permissions; least privilege means the API role genuinely can't send or
# receive a message even if its code tried to.)

resource "aws_iam_role" "queue_consumer_task" {
  name               = "${var.project_name}-${var.environment}-queue-consumer-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

data "aws_iam_policy_document" "queue_consumer" {
  statement {
    sid = "MainQueueProduceConsume"
    actions = [
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.main.arn]
  }
  statement {
    sid = "DlqProduceAndInspect"
    actions = [
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
      "sqs:ReceiveMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "queue_consumer" {
  name   = "queue-access"
  role   = aws_iam_role.queue_consumer_task.id
  policy = data.aws_iam_policy_document.queue_consumer.json
}

# --- Task role: API (no queue access) ---

resource "aws_iam_role" "api_task" {
  name               = "${var.project_name}-${var.environment}-api-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}
