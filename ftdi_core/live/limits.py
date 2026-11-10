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


PUBLIC_MIN_CLOCK_INTERVAL_S = 1.0   # at most one server-time request per second against the public venue


PATH_MAX_PROBES = 20               # probe connections per capture, at most
PATH_PUBLIC_MIN_WINDOW_S = 2.0      # against the public venue a probe lasts at least this long
PATH_MIN_SAMPLES = 5                # unqueued messages per connection needed to compare
PATH_MARGIN_MS = 1.0                # a probe replaces the active connection only if faster by more than this


def path_selection_problem(probes, window_s, public_network):
    """None if (probes, window_s) are valid path-selection settings for that mode (probes 0 = off)."""
    if type(probes) is not int or not 0 <= probes <= PATH_MAX_PROBES:
        return f'path probes must be an integer from 0 to {PATH_MAX_PROBES}'
    if type(window_s) not in (int, float) or not 0.1 <= window_s <= 600:
        return 'path probe window must be from 0.1 to 600 seconds'
    if public_network and probes and window_s < PATH_PUBLIC_MIN_WINDOW_S:
        return f'path probe window below {PATH_PUBLIC_MIN_WINDOW_S} s in public mode'
    return None


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
    if public_network and limits.clock_interval_s < PUBLIC_MIN_CLOCK_INTERVAL_S:
        return f'limits: clock_interval_s below {PUBLIC_MIN_CLOCK_INTERVAL_S} in public mode'
    return None
