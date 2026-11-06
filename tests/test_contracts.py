import unittest
from dataclasses import FrozenInstanceError
from ftdi_core.contracts import (Scope,RecordStatus,StreamIntegrity,TimestampEvidence,
                                 validate_market_event,content_hash,quality_allows_derived_features)
from ftdi_core.adapters.s070041.durable_stream import scope_of,canonical
from ftdi_core.adapters.s070041.external_delivery import event_identity


def evt(seq=1):
    return {'source':'binance_spot_aggtrades','symbol':'BTCUSDT','venue':'BINANCE_SPOT','channel':'aggTrades',
            'source_seq':seq,'event_time_ms':1717200000000,'price':'67701.23','qty':'0.001'}


class ContractsTests(unittest.TestCase):
    def test_scope_legacy_compatibility(self):
        self.assertEqual(Scope.from_event(evt()).lane(),scope_of(evt()))
    def test_scope_requires_identity(self):
        with self.assertRaises(ValueError):Scope('','BTCUSDT','BINANCE_SPOT','aggTrades')
    def test_scope_is_immutable(self):
        with self.assertRaises(FrozenInstanceError):Scope('a','b','c','d').venue='x'
    def test_numeric_semantics(self):
        self.assertEqual(validate_market_event(evt()).symbol,'BTCUSDT')
    def test_seq_bool_rejected(self):
        e=evt();e['source_seq']=True
        with self.assertRaises(ValueError):validate_market_event(e)
    def test_negative_sequence_rejected(self):
        with self.assertRaises(ValueError):validate_market_event(evt(-1))
    def test_nonfinite_price_rejected(self):
        e=evt();e['price']='NaN'
        with self.assertRaises(ValueError):validate_market_event(e)
    def test_zero_qty_rejected(self):
        e=evt();e['qty']='0'
        with self.assertRaises(ValueError):validate_market_event(e)
    def test_payload_hash_backwards_compatible(self):
        import hashlib
        self.assertEqual(content_hash(evt()),hashlib.sha256(canonical(evt()).encode()).hexdigest())
    def test_identity_contract_uses_frozen_version(self):
        x=evt();l=scope_of(x);d=content_hash(x)
        self.assertEqual(len(event_identity(l,x['source_seq'],0,d)),64)
    def test_no_untrusted_feature(self):
        self.assertFalse(quality_allows_derived_features(RecordStatus.VALID,StreamIntegrity.CONTIGUOUS,False))
        self.assertFalse(quality_allows_derived_features(RecordStatus.VALID,StreamIntegrity.WAITING_FOR_MISSING,True))
        self.assertTrue(quality_allows_derived_features(RecordStatus.VALID,StreamIntegrity.CONTIGUOUS,True))
    def test_time_evidence_not_latency_certification(self):
        e=TimestampEvidence(1700000000,1700000000000,1000)
        self.assertFalse(e.has_trusted_one_way_delay)
    def test_negative_wall_ns_rejected(self):
        with self.assertRaises(ValueError):TimestampEvidence(1,-1,2)
