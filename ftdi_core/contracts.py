"""FTDI-Core v0.1 contracts. Historical and live sources are never conflated.

These are interface-level invariants, not a claim that the old S07 adapters
already implement every desired V1 feature.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
from typing import Any, Mapping

SCHEMA = 'FTDI-Core-Event-v1'
SOURCE_FIELDS = ('source', 'symbol', 'venue', 'channel')


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def content_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical(payload).encode('utf-8')).hexdigest()


class RecordStatus(str, Enum):
    VALID = 'VALID'
    QUARANTINED_INVALID = 'QUARANTINED_INVALID'
    QUARANTINED_CONFLICT = 'QUARANTINED_CONFLICT'


class StreamIntegrity(str, Enum):
    UNVERIFIED_PREFIX = 'UNVERIFIED_PREFIX'
    CONTIGUOUS = 'CONTIGUOUS'
    WAITING_FOR_MISSING = 'WAITING_FOR_MISSING'
    CONFLICT = 'CONFLICT'


class Provenance(str, Enum):
    HISTORICAL_ARCHIVE = 'HISTORICAL_ARCHIVE'
    LIVE_WS = 'LIVE_WS'
    REST_RECOVERY = 'REST_RECOVERY'
    SYNTHETIC = 'SYNTHETIC'


@dataclass(frozen=True)
class Scope:
    source: str
    symbol: str
    venue: str
    channel: str

    def __post_init__(self) -> None:
        if any(not isinstance(x, str) or not x for x in (self.source, self.symbol, self.venue, self.channel)):
            raise ValueError('Nonempty scope fields required')

    def lane(self) -> str:
        return canonical([self.source, self.symbol, self.venue, self.channel])

    @classmethod
    def from_event(cls, e: Mapping[str, Any]) -> 'Scope':
        return cls(*(e[k] for k in SOURCE_FIELDS))


@dataclass(frozen=True)
class TimestampEvidence:
    exchange_event_ms: int | None
    local_receive_wall_ns: int | None
    local_receive_monotonic_ns: int | None
    db_commit_wall_ns: int | None = None
    source_clock_offset_bound_ms: float | None = None

    def __post_init__(self) -> None:
        for name in ('exchange_event_ms', 'local_receive_wall_ns', 'local_receive_monotonic_ns', 'db_commit_wall_ns'):
            v = getattr(self, name)
            if v is not None and (type(v) is not int or v < 0):
                raise ValueError(f'{name} must be nonnegative integer or None')

    @property
    def has_trusted_one_way_delay(self) -> bool:
        return False  # No source-clock bound/transport calibration in Core v0.1.


def validate_market_event(e: Mapping[str, Any]) -> Scope:
    if not isinstance(e, dict):
        raise ValueError('Event must be a JSON object')
    try:
        scope = Scope.from_event(e)
        seq, ts = e['source_seq'], e['event_time_ms']
        if type(seq) is not int or seq < 0 or type(ts) is not int or ts < 0:
            raise ValueError('source_seq/event_time_ms invalid')
        p, q = Decimal(str(e['price'])), Decimal(str(e['qty']))
        if not p.is_finite() or not q.is_finite() or p <= 0 or q <= 0:
            raise ValueError('price/qty must be finite and positive')
    except (KeyError, InvalidOperation, TypeError) as exc:
        raise ValueError(f'Invalid market event: {exc}') from exc
    return scope


def quality_allows_derived_features(record: RecordStatus, stream: StreamIntegrity,
                                    prefix_authoritatively_verified: bool) -> bool:
    """Conservative candidate policy, not a promise of strategy fitness."""
    return (record is RecordStatus.VALID and stream is StreamIntegrity.CONTIGUOUS
            and prefix_authoritatively_verified)
