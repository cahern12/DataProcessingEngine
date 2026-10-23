import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from dpe.common import DATASETS,write_json
from dpe.synthetic import generate
from dpe.ingestion import load_batch
from dpe.bounded_io import sources,profile
from dpe.rules import evaluate


class BoundedIOTests(unittest.TestCase):
    def test_repeatable_file_views_preserve_oracle_results_counts_and_raw_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);generate(root/'source',count=30)
            initial=root/'source/initial';delta=root/'source/delta'
            a,base,_=load_batch(initial);h,updates,_=load_batch(delta)
            header,records,origins,checked=sources([initial,delta])
            combined={k:base[k]+updates[k] for k in DATASETS}
            for name in DATASETS:
                self.assertEqual(list(records[name]),combined[name]);self.assertEqual(list(records[name]),combined[name])
                self.assertEqual(len(records[name]),len(combined[name]))
                self.assertEqual(len(list(origins[name])),len(combined[name]))
            self.assertEqual(evaluate(records,header['cutoff'],header['report_start']),evaluate(combined,h['cutoff'],h['report_start']))
            file=initial/a['files'][0]['path'];file.write_text(file.read_text()+'\n')
            with self.assertRaisesRegex(ValueError,'checksum'):sources([initial])

    def test_json_output_stays_byte_identical_without_a_full_serialized_copy(self):
        value={'b':[None,True,'Unicode \u2603',{'a':3}], 'a':2}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'out.json';write_json(path,value)
            self.assertEqual(path.read_text(),json.dumps(value,sort_keys=True,indent=2)+'\n')

    def test_streamed_source_loader_preserves_sql_membership_and_provenance(self):
        from dpe.lakehouse import Client
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);generate(root/'source',count=10)
            h,records,origins,_=sources([root/'source/initial'])
            def emitted(datasets,provenance):
                client=Client.__new__(Client);calls=[];client.sql=lambda query:calls.append(query)
                client.create_sources('test_schema',[{'name':'case','cutoff':h['cutoff'],'report_start':h['report_start'],'datasets':datasets,'provenance':provenance}])
                return calls
            materialized={k:list(v) for k,v in records.items()};provenance={k:list(v) for k,v in origins.items()}
            with profile({'source_ingestion':{'records_per_insert':7}}):
                self.assertEqual(emitted(records,origins),emitted(materialized,provenance))
                with profile({'source_ingestion':{'bounded_io':True,'records_per_insert':3}}):
                    from dpe.bounded_io import enabled
                    self.assertTrue(enabled())
                self.assertFalse(enabled())
