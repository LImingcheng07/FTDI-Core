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

INTERVAL_CLOCK = {'implementation': time.get_clock_info('perf_counter').implementation,
                  'resolution_ns': max(1, round(time.get_clock_info('perf_counter').resolution * 1e9))}
