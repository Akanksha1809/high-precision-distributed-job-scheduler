from app.handlers.base import HandlerFn
from app.handlers.samples import always_fail, benchmark_task, process_data, simulate_notification

HANDLER_REGISTRY: dict[str, HandlerFn] = {
    "demo.notify": simulate_notification,
    "demo.process_data": process_data,
    "demo.always_fail": always_fail,
    "bench.task": benchmark_task,
}


def is_registered(task_type: str) -> bool:
    return task_type in HANDLER_REGISTRY


def get_handler(task_type: str) -> HandlerFn:
    try:
        return HANDLER_REGISTRY[task_type]
    except KeyError as exc:
        raise KeyError(f"No handler registered for task_type={task_type!r}") from exc
