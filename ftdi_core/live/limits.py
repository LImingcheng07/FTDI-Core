"""CaptureLimits (docs/LIVE_CAPTURE.md §3), shared by the capture engine and the verifier."""
from __future__ import annotations

from dataclasses import dataclass, fields


@dataclass(frozen=True, kw_only=True)
class CaptureLimits:
    queue_size: int = 10000
    max_pending: int = 100000
    gap_wait_ms: int = 200
    gap_timeout_s: float = 10.0
    max_rest_pages_per_gap: int = 10
    rest_min_interval_s: float = 0.35
    max_reconnects: int = 5
    ws_read_timeout_s: float = 30.0
    max_message_bytes: int = 1048576
    max_rest_body_bytes: int = 2097152
    clock_interval_s: float = 60.0
    fsync_every: int = 256

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.type in (int, 'int'):
                ok = type(value) is int and (value >= 0 if field.name in ('gap_wait_ms', 'max_reconnects') else value >= 1)
            else:
                ok = type(value) in (int, float) and value >= 0 and (field.name == 'rest_min_interval_s' or value > 0)
            if not ok:
                raise ValueError(f'CaptureLimits.{field.name} has an invalid value: {value!r}')


def limits_problem(value, public_network):
    """None if ``value`` (a manifest/start ``limits`` dict) is a valid CaptureLimits for that mode."""
    if not isinstance(value, dict) or set(value) != {f.name for f in fields(CaptureLimits)}:
        return 'limits fields'
    try:
        limits = CaptureLimits(**value)
    except (TypeError, ValueError) as exc:
        return f'limits: {exc}'
    if public_network and limits.rest_min_interval_s < 0.1:
        return 'limits: rest_min_interval_s below 0.1 in public mode'
    return None
