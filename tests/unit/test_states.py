import pytest

from app.models.states import (
    CANCELLABLE_STATES,
    RESCHEDULABLE_STATES,
    JobStatus,
    can_transition,
)


@pytest.mark.parametrize(
    "frm,to",
    [
        (JobStatus.SCHEDULED, JobStatus.DISPATCH_PENDING),
        (JobStatus.SCHEDULED, JobStatus.CANCELLED),
        (JobStatus.DISPATCH_PENDING, JobStatus.QUEUED),
        (JobStatus.DISPATCH_PENDING, JobStatus.SCHEDULED),
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.SCHEDULED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.RETRY_PENDING),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.DEAD_LETTER),
        (JobStatus.RETRY_PENDING, JobStatus.SCHEDULED),
        (JobStatus.RETRY_PENDING, JobStatus.CANCELLED),
    ],
)
def test_legal_transitions(frm, to):
    assert can_transition(frm, to)


@pytest.mark.parametrize(
    "frm,to",
    [
        (JobStatus.SCHEDULED, JobStatus.RUNNING),
        (JobStatus.SCHEDULED, JobStatus.SUCCEEDED),
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.CANCELLED),  # best-effort: no cancel mid-run
        (JobStatus.SUCCEEDED, JobStatus.SCHEDULED),
        (JobStatus.FAILED, JobStatus.RUNNING),
        (JobStatus.CANCELLED, JobStatus.SCHEDULED),
        (JobStatus.DEAD_LETTER, JobStatus.RETRY_PENDING),
    ],
)
def test_illegal_transitions(frm, to):
    assert not can_transition(frm, to)


def test_terminal_states_have_no_outgoing_transitions():
    for terminal in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED):
        for candidate in JobStatus:
            assert not can_transition(terminal, candidate)


def test_running_is_not_cancellable():
    assert JobStatus.RUNNING not in CANCELLABLE_STATES


def test_in_flight_states_are_not_reschedulable():
    for s in (JobStatus.DISPATCH_PENDING, JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED):
        assert s not in RESCHEDULABLE_STATES
