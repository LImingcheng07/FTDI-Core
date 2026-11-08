"""Venue record parsing and the Core event mapping for live aggTrades (docs/LIVE_CAPTURE.md §2)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import json
import re

SOURCE = 'binance_spot_ws_aggtrade'
VENUE = 'BINANCE_SPOT'
CHANNEL = 'aggTrades'
SYMBOL_RE = re.compile(r'[A-Z0-9]{2,24}')
DECIMAL_RE = re.compile(r'[0-9]+(\.[0-9]+)?')
EVENT_KEYS = frozenset({'source', 'symbol', 'venue', 'channel', 'source_seq', 'event_time_ms', 'event_time_native',
                        'event_time_native_unit', 'price', 'qty', 'first_trade_id', 'last_trade_id', 'is_buyer_maker',
                        'is_best_match'})


class FormatError(ValueError):
    """A venue message or page that does not have the documented shape."""


def canonical(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def lane(symbol):
    return {'source': SOURCE, 'symbol': symbol, 'venue': VENUE, 'channel': CHANNEL}


def check_symbol(symbol):
    if not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
        raise ValueError('symbol must match [A-Z0-9]{2,24}')
    return symbol


def _reject_constant(name):
    raise FormatError(f'non-finite JSON constant {name}')


def loads(text):
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except FormatError:
        raise
    except (ValueError, RecursionError) as exc:
        raise FormatError(f'not JSON: {exc}') from None


def _nonneg_int(value):
    return type(value) is int and value >= 0


def _positive_decimal(value):
    if not isinstance(value, str) or not DECIMAL_RE.fullmatch(value):
        return False
    try:
        number = Decimal(value)
    except InvalidOperation:
        return False
    return number.is_finite() and number > 0


def _trade_fields(obj, where):
    for key in ('a', 'f', 'l', 'T'):
        if not _nonneg_int(obj.get(key)):
            raise FormatError(f'{where}: field {key} must be a non-negative integer')
    if obj['f'] > obj['l']:
        raise FormatError(f'{where}: first trade id above last trade id')
    for key in ('p', 'q'):
        if not _positive_decimal(obj.get(key)):
            raise FormatError(f'{where}: field {key} must be a positive decimal string')
    for key in ('m', 'M'):
        if type(obj.get(key)) is not bool:
            raise FormatError(f'{where}: field {key} must be a boolean')
    return {key: obj[key] for key in ('a', 'p', 'q', 'f', 'l', 'T', 'm', 'M')}


def parse_ws(text, symbol):
    """One WebSocket text message -> venue record (with the venue event time ``E``)."""
    obj = loads(text)
    if not isinstance(obj, dict):
        raise FormatError('WebSocket message is not a JSON object')
    if obj.get('e') != 'aggTrade':
        raise FormatError('WebSocket message is not an aggTrade')
    if obj.get('s') != symbol:
        raise FormatError('WebSocket message for another symbol')
    if not _nonneg_int(obj.get('E')):
        raise FormatError('WebSocket message: field E must be a non-negative integer')
    record = _trade_fields(obj, 'WebSocket message')
    record['E'] = obj['E']
    return record


def parse_rest_page(body):
    """A REST aggTrades body -> list of venue records (no ordering check here)."""
    records, problem = rest_records_lenient(body)
    if problem:
        raise FormatError(problem)
    return records


def rest_records_lenient(body):
    """(valid records, problem or None): every well-formed record of a body, even if others are malformed.

    Used for evidence: a malformed page must not hide a valid record that contradicts a written event."""
    try:
        obj = loads(body)
    except FormatError as exc:
        return [], str(exc)
    if not isinstance(obj, list):
        return [], 'REST aggTrades body is not a JSON array'
    records, problem = [], None
    for index, item in enumerate(obj):
        try:
            if not isinstance(item, dict):
                raise FormatError(f'REST record {index} is not an object')
            records.append(_trade_fields(item, f'REST record {index}'))
        except FormatError as exc:
            problem = problem or str(exc)
    return records, problem


def ws_record_lenient(text, symbol):
    """The venue record of a WebSocket message, or None if it is not a valid aggTrade of ``symbol``."""
    try:
        return parse_ws(text, symbol)
    except FormatError:
        return None


def core_event(record, symbol):
    return {'source': SOURCE, 'symbol': symbol, 'venue': VENUE, 'channel': CHANNEL, 'source_seq': record['a'],
            'event_time_ms': record['T'], 'event_time_native': record['T'], 'event_time_native_unit': 'ms',
            'price': record['p'], 'qty': record['q'], 'first_trade_id': record['f'], 'last_trade_id': record['l'],
            'is_buyer_maker': record['m'], 'is_best_match': record['M']}


def compare(event, other):
    """'identical', 'text_variant' (equal values, different text) or 'conflict' for two events of the same seq."""
    for key in ('source_seq', 'event_time_ms', 'first_trade_id', 'last_trade_id', 'is_buyer_maker', 'is_best_match'):
        if event[key] != other[key] or type(event[key]) is not type(other[key]):
            return 'conflict'
    if Decimal(event['price']) != Decimal(other['price']) or Decimal(event['qty']) != Decimal(other['qty']):
        return 'conflict'
    if event['price'] != other['price'] or event['qty'] != other['qty']:
        return 'text_variant'
    return 'identical'


def check_event(event, symbol):
    """Raise FormatError unless ``event`` is a Core event of §2 for ``symbol``."""
    if not isinstance(event, dict) or set(event) != EVENT_KEYS:
        raise FormatError('event fields differ from the contract')
    expected = core_event({'a': event['source_seq'], 'p': event['price'], 'q': event['qty'],
                           'f': event['first_trade_id'], 'l': event['last_trade_id'], 'T': event['event_time_ms'],
                           'm': event['is_buyer_maker'], 'M': event['is_best_match']}, symbol)
    if event != expected or any(type(event[k]) is not type(expected[k]) for k in expected):
        raise FormatError('event is not the mapping of a venue record')
    _trade_fields({'a': event['source_seq'], 'p': event['price'], 'q': event['qty'], 'f': event['first_trade_id'],
                   'l': event['last_trade_id'], 'T': event['event_time_ms'], 'm': event['is_buyer_maker'],
                   'M': event['is_best_match']}, 'event')
