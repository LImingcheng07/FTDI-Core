"""Wall clock for live capture evidence.

CPython before 3.13 implements ``time.time()`` on Windows with ``GetSystemTimeAsFileTime()``, which advances in
system-timer ticks (nominally 15.625 ms). Read stamps and clock samples taken with it are quantised by up to a tick;
the 2026-11-08 real-network pilot showed runs of reads offset by 3.5-5.6 ms against the interval clock for exactly
this reason (docs/REAL_LIVE_PILOT_RESULTS.md). On Windows the capture therefore uses
``GetSystemTimePreciseAsFileTime()`` (100 ns units, Windows 8+) through ctypes; elsewhere ``time.time_ns()``.
"""
from __future__ import annotations

import ctypes
import sys
import time

_EPOCH_AS_FILETIME = 116444736000000000   # 100 ns intervals between 1601-01-01 and 1970-01-01


def _windows_precise():
    try:
        function = ctypes.WinDLL('kernel32').GetSystemTimePreciseAsFileTime
    except (OSError, AttributeError):
        return None
    function.argtypes = [ctypes.POINTER(ctypes.c_ulonglong)]
    function.restype = None

    def precise_time_ns():
        value = ctypes.c_ulonglong()   # per call: the reader and the writer thread both take stamps
        function(ctypes.byref(value))
        return (value.value - _EPOCH_AS_FILETIME) * 100
    return precise_time_ns


_PRECISE = _windows_precise() if sys.platform == 'win32' else None

if _PRECISE is not None:
    wall_time_ns = _PRECISE
    WALL_CLOCK = {'implementation': 'GetSystemTimePreciseAsFileTime()', 'resolution_ns': 100}
else:
    wall_time_ns = time.time_ns
    WALL_CLOCK = {'implementation': time.get_clock_info('time').implementation,
                  'resolution_ns': round(time.get_clock_info('time').resolution * 1e9)}

PAIR_LIMIT_NS = 100_000   # both interval reads around the wall reading within 0.1 ms
PAIR_ATTEMPTS = 8


def stamp_pair(at_perf_ns=None):
    """(wall_ns, perf_ns) of one instant.

    The ctypes call releases the GIL and the thread may have to wait to get it back, so an interval stamp taken
    after the wall stamp can be late by milliseconds (review of v0.4.1). The interval clock is therefore read on
    both sides of the wall clock, the attempt repeated (up to PAIR_ATTEMPTS) until both interval reads lie within
    PAIR_LIMIT_NS, and the tightest attempt used. The wall stamp is mapped to ``at_perf_ns`` (default: the middle
    of the bracket), e.g. the interval stamp taken right after a read."""
    best = None
    for _ in range(PAIR_ATTEMPTS):
        before = time.perf_counter_ns()
        wall = wall_time_ns()
        after = time.perf_counter_ns()
        if best is None or after - before < best[2] - best[1]:
            best = (wall, before, after)
        if after - before <= PAIR_LIMIT_NS:
            break
    wall, before, after = best
    middle = (before + after) // 2
    at = middle if at_perf_ns is None else at_perf_ns
    return wall - (middle - at), at


INTERVAL_CLOCK = {'implementation': time.get_clock_info('perf_counter').implementation,
                  'resolution_ns': max(1, round(time.get_clock_info('perf_counter').resolution * 1e9))}
