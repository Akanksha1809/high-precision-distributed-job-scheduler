"""One-shot bootstrap: create the main and dead-letter SQS queues against whatever
endpoint is configured (LocalStack locally, real AWS in production — this script is
safe to run against either, and create_queue is idempotent if they already exist).
Run via `python -m scripts.init_queues` before any dispatcher/publisher/worker
container starts.
"""
import boto3

from app.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    client = boto3.client(
        "sqs", region_name=settings.aws_region, endpoint_url=settings.aws_endpoint_url
    )

    main_name = settings.sqs_queue_url.rsplit("/", 1)[-1] if settings.sqs_queue_url else "chronos-jobs-main"
    dlq_name = settings.sqs_dlq_url.rsplit("/", 1)[-1] if settings.sqs_dlq_url else "chronos-jobs-dlq"

    main_resp = client.create_queue(QueueName=main_name)
    dlq_resp = client.create_queue(QueueName=dlq_name)
    print(f"main queue ready: {main_resp['QueueUrl']}")
    print(f"dlq queue ready:  {dlq_resp['QueueUrl']}")


if __name__ == "__main__":
    main()
