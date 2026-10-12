import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import (
    AttemptResponse,
    JobCreateRequest,
    JobListResponse,
    JobRescheduleRequest,
    JobResponse,
)
from app.config.settings import get_settings
from app.db import repository
from app.db.session import get_session
from app.observability.metrics import (
    jobs_cancelled_total,
    jobs_created_total,
    jobs_idempotent_hits_total,
    jobs_rescheduled_total,
)

router = APIRouter(tags=["jobs"])


@router.post("/jobs", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    body: JobCreateRequest, session: AsyncSession = Depends(get_session)
) -> JobResponse:
    settings = get_settings()
    try:
        job, created = await repository.create_job(
            session,
            task_type=body.task_type,
            payload=body.payload,
            scheduled_at=body.scheduled_at,
            idempotency_key=body.idempotency_key,
            max_retries=body.max_retries if body.max_retries is not None else settings.job_max_retries,
            timeout_seconds=body.timeout_seconds
            if body.timeout_seconds is not None
            else settings.job_default_timeout_seconds,
        )
    except repository.IdempotencyConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    if created:
        jobs_created_total.labels(task_type=body.task_type).inc()
    else:
        jobs_idempotent_hits_total.inc()
    return JobResponse.model_validate(job)


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)) -> JobResponse:
    try:
        job = await repository.get_job(session, job_id)
    except repository.JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return JobResponse.model_validate(job)


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    status_filter: str | None = Query(default=None, alias="status"),
    task_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> JobListResponse:
    jobs, next_cursor = await repository.list_jobs(
        session, status=status_filter, task_type=task_type, limit=limit, cursor=cursor
    )
    return JobListResponse(
        items=[JobResponse.model_validate(j) for j in jobs], next_cursor=next_cursor, limit=limit
    )


@router.post("/jobs/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> JobResponse:
    try:
        job = await repository.cancel_job(session, job_id)
    except repository.JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except repository.IllegalStateForOperationError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    jobs_cancelled_total.inc()
    return JobResponse.model_validate(job)


@router.post("/jobs/{job_id}/reschedule", response_model=JobResponse)
async def reschedule_job(
    job_id: uuid.UUID,
    body: JobRescheduleRequest,
    session: AsyncSession = Depends(get_session),
) -> JobResponse:
    try:
        job = await repository.reschedule_job(session, job_id, body.scheduled_at)
    except repository.JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except repository.IllegalStateForOperationError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    jobs_rescheduled_total.inc()
    return JobResponse.model_validate(job)


@router.get("/jobs/{job_id}/attempts", response_model=list[AttemptResponse])
async def list_attempts(
    job_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> list[AttemptResponse]:
    try:
        await repository.get_job(session, job_id)  # 404 if the job itself doesn't exist
    except repository.JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    attempts = await repository.list_attempts(session, job_id)
    return [AttemptResponse.model_validate(a) for a in attempts]
