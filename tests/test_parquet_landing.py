import tempfile
import unittest
from pathlib import Path
try:
    import pyarrow.parquet as pq
except ImportError:pq=None
from dpe.parquet_landing import write_parts,raw_rows


@unittest.skipUnless(pq,'optional pinned Parquet dependency not installed')
class ParquetLandingTests(unittest.TestCase):
    def test_chunked_parquet_preserves_exact_payload_types_order_and_source_provenance(self):
        records=[{'incident_id':'A','source_version':1,'text':"O'Brien \u2603"},{'incident_id':'A','source_version':1,'text':'duplicate'},
                 {'incident_id':'B','source_version':2,'invalid_timestamp':None}]
        case={'name':'batch','datasets':{'incident_snapshots':records},'provenance':{'incident_snapshots':[{'batch_id':'old','source_file':'source.jsonl','source_row':n+7} for n in range(3)]}}
        expected=list(raw_rows([case]))
        with tempfile.TemporaryDirectory() as temporary:
            files=write_parts(iter(expected),Path(temporary)/'landing',chunk_rows=2)
            self.assertEqual([f['rows'] for f in files],[2,1])
            actual=[row for f in files for row in pq.read_table(f['path']).to_pylist()]
            self.assertEqual(actual,expected)
            self.assertTrue(all(type(r['ordinal']) is int and type(r['source_row']) is int for r in actual))

    def test_extra_landing_fields_are_refused_before_registration(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError,'exact schema'):
                write_parts([{'unexpected':'data'}],Path(temporary)/'landing')
