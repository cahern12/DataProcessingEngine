import tempfile
import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dpe.common import write_json,read_json,digest
from dpe.lakehouse import archived_batch,committed_publications,compare_rows,consume,sql_revision
from dpe.synthetic import generate


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.first="a"*64;self.current="b"*64;self.orphan="c"*64
        def publication(identity,parent,snapshot):
            return {"publication_id":identity,"parent_publication_id":parent,
                    "snapshots":{"incident_resolution":{"table":"lake.dpe_test.gold_incident_resolution","snapshot_id":snapshot}}}
        self.initial=publication(self.first,None,10)
        self.latest=publication(self.current,self.first,20)
        write_json(self.root/"publications"/(self.first+".json"),self.initial)
        write_json(self.root/"publications"/(self.current+".json"),self.latest)
        write_json(self.root/"publications"/(self.orphan+".json"),publication(self.orphan,self.current,30))
        write_json(self.root/"active.json",self.latest)

    def tearDown(self):
        self.temp.cleanup()

    def test_prepared_orphan_is_not_a_committed_publication(self):
        self.assertEqual([p["publication_id"] for p in committed_publications(self.root)],[self.current,self.first])
        with self.assertRaisesRegex(ValueError,"not committed"):
            consume(self.root,"incident_resolution",publication_id=self.orphan)

    def test_consumer_can_pin_a_previous_publication(self):
        client=MagicMock();client.sql.return_value=[{"case_id":"__batch__","incident_id":"I1"}]
        with patch("dpe.lakehouse.Client",return_value=client):
            result=consume(self.root,"incident_resolution",publication_id=self.first)
        self.assertEqual(result["publication_id"],self.first)
        self.assertEqual(result["rows"],[{"incident_id":"I1"}])
        self.assertIn("FOR VERSION AS OF 10",client.sql.call_args[0][0])

    def test_publication_ancestry_cycle_is_rejected(self):
        self.initial["parent_publication_id"]=self.current
        write_json(self.root/"publications"/(self.first+".json"),self.initial)
        with self.assertRaisesRegex(ValueError,"ancestry"):
            list(committed_publications(self.root))

    def test_comparison_distinguishes_missing_and_null_fields(self):
        result=compare_rows("incident_resolution",[{"incident_id":"I1","resolved_at":None}],[{"incident_id":"I1"}])
        self.assertEqual(len(result),1)
        self.assertEqual(result[0]["field"],"resolved_at")

    def test_comparison_rejects_duplicate_keys(self):
        with self.assertRaisesRegex(AssertionError,"duplicate output key"):
            compare_rows("incident_resolution",[],[{"incident_id":"I1"},{"incident_id":"I1"}])

    def test_source_revision_is_independent_of_filesystem_creation_order(self):
        revisions=[]
        for number,names in enumerate([['z.properties','a.properties'],['a.properties','z.properties']]):
            root=self.root/str(number)
            (root/'infra/local').mkdir(parents=True)
            project=root/'transformations/dbt';project.mkdir(parents=True)
            (root/'requirements-local.lock').write_text('fixed requirements\n')
            for name in names:(root/'infra/local'/name).write_text(name+'\n')
            with patch('dpe.lakehouse.ROOT',root),patch('dpe.lakehouse.PROJECT',project),patch('dpe.lakehouse.revision',return_value='fixed source'):
                revisions.append(sql_revision())
        self.assertEqual(revisions[0],revisions[1])

    def test_archived_manifest_cannot_change_under_its_original_identity(self):
        generate(self.root/'source',count=1)
        source=self.root/'source/initial'
        archive=self.root/'sources'/digest(source/'manifest.json')
        shutil.copytree(source,archive)
        archived_batch(archive)
        manifest=read_json(archive/'manifest.json');manifest['seed']+=1
        write_json(archive/'manifest.json',manifest)
        with self.assertRaisesRegex(ValueError,'archived manifest identity mismatch'):
            archived_batch(archive)


if __name__=="__main__":
    unittest.main()
