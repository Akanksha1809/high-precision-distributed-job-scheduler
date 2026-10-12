# Standard queue, not FIFO — see DESIGN_DECISIONS.md for the throughput/ordering
# trade-off (FIFO caps around 3000 msg/s with batching, far below the project's
# throughput target, and the application doesn't need strict cross-job ordering;
# a worker re-validates authoritative state before executing, which is what makes
# standard-queue reordering and at-least-once duplicates safe).

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-${var.environment}-jobs-dlq"
  message_retention_seconds = 1209600 # 14 days — give operators time to notice and inspect
}

resource "aws_sqs_queue" "main" {
  name                       = "${var.project_name}-${var.environment}-jobs-main"
  visibility_timeout_seconds = 60     # should exceed the app's own job_lease_buffer_seconds margin
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 10     # long polling

  # This is a defense-in-depth backstop, not the primary DLQ mechanism. The
  # application routes exhausted-retry jobs to the DLQ itself, at the application
  # level (app/workers/outcomes.py), after its own configurable max_retries budget
  # — that's the primary path, and it's what the DEAD_LETTER job status reflects.
  # This SQS-native redrive policy only fires if a message is redelivered far more
  # times than the app's own retry budget would ever allow (e.g. a bug that crashes
  # the worker before it even reaches the claim step, every single time) — a much
  # higher threshold than max_retries, specifically so it almost never fires under
  # normal operation and only catches genuinely pathological cases.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 20
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq_source" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}
