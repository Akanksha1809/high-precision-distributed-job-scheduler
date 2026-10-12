resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

locals {
  common_env = [
    { name = "APP_ENV", value = var.environment },
    { name = "LOG_LEVEL", value = "INFO" },
    { name = "LOG_FORMAT", value = "json" },
  ]

  queue_env = [
    { name = "QUEUE_BACKEND", value = "sqs" },
    { name = "AWS_REGION", value = var.aws_region },
    # AWS_ENDPOINT_URL is deliberately NOT set here (unlike docker-compose.yml's
    # LocalStack config) — leaving it unset makes boto3 resolve the real AWS SQS
    # endpoint. Same application code, different environment, zero code changes.
    { name = "SQS_QUEUE_URL", value = aws_sqs_queue.main.id },
    { name = "SQS_DLQ_URL", value = aws_sqs_queue.dlq.id },
  ]

  db_secret_ref = [
    { name = "DATABASE_URL", valueFrom = "${aws_secretsmanager_secret.db_credentials.arn}:database_url::" },
  ]

  # Per-service config: container command + which task role it gets (API never
  # gets queue permissions; the other four share the queue_consumer role) + whether
  # it's the one service behind the load balancer.
  services = {
    api = {
      command    = ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
      task_role  = aws_iam_role.api_task.arn
      extra_env  = []
      port       = 8000
      behind_alb = true
      cpu        = 512
      memory     = 1024
      desired    = var.api_desired_count
    }
    dispatcher = {
      command    = ["python", "-m", "app.scheduler"]
      task_role  = aws_iam_role.queue_consumer_task.arn # writes outbox rows only; no queue perms needed but shares the role for simplicity
      extra_env  = []
      port       = 9100
      behind_alb = false
      cpu        = 256
      memory     = 512
      desired    = var.dispatcher_desired_count
    }
    outbox-publisher = {
      command    = ["python", "-m", "app.publisher"]
      task_role  = aws_iam_role.queue_consumer_task.arn
      extra_env  = local.queue_env
      port       = 9100
      behind_alb = false
      cpu        = 256
      memory     = 512
      desired    = var.outbox_publisher_desired_count
    }
    worker = {
      command    = ["python", "-m", "app.workers"]
      task_role  = aws_iam_role.queue_consumer_task.arn
      extra_env  = local.queue_env
      port       = 9100
      behind_alb = false
      cpu        = 512
      memory     = 1024
      desired    = var.worker_desired_count
    }
    reconciliation = {
      command    = ["python", "-m", "app.reconciliation"]
      task_role  = aws_iam_role.queue_consumer_task.arn
      extra_env  = local.queue_env
      port       = 9100
      behind_alb = false
      cpu        = 256
      memory     = 512
      desired    = var.reconciliation_desired_count
    }
  }
}

resource "aws_ecs_task_definition" "service" {
  for_each                 = local.services
  family                   = "${var.project_name}-${var.environment}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = each.value.task_role

  container_definitions = jsonencode([{
    name      = each.key
    image     = var.container_image
    command   = each.value.command
    essential = true
    portMappings = [{
      containerPort = each.value.port
      protocol      = "tcp"
    }]
    environment = concat(local.common_env, each.value.extra_env)
    secrets     = local.db_secret_ref
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.service[each.key].name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = each.key
      }
    }
  }])
}

resource "aws_ecs_service" "service" {
  for_each        = local.services
  name            = "${var.project_name}-${var.environment}-${each.key}"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.service[each.key].arn
  desired_count   = each.value.desired
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = var.demo_mode # demo mode only — see network.tf header comment
  }

  dynamic "load_balancer" {
    for_each = each.value.behind_alb ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.api[0].arn
      container_name   = each.key
      container_port   = each.value.port
    }
  }

  depends_on = [aws_lb_listener.api]
}

# --- Load balancer (API only) ---

resource "aws_lb" "api" {
  name               = "${var.project_name}-${var.environment}-api"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "api" {
  count       = 1
  name        = "${var.project_name}-${var.environment}-api"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/health"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
    timeout             = 5
  }
}

resource "aws_lb_listener" "api" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api[0].arn
  }
}
