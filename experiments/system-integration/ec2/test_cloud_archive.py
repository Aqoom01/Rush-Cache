import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest
from cloud_archive import digest,guarded_terminate,pack,prefix,upload,verify


class FakeAws:
    def __init__(self,binding):
        self.binding=binding;self.objects={};self.calls=[];self.foreign=False
    def put(self,key,path):
        versions=self.objects.setdefault(key,[]);versions.append(Path(path).read_bytes())
        return dict(VersionId=str(len(versions)))
    def get(self,key,path,version=None):
        versions=self.objects[key];Path(path).write_bytes(versions[int(version)-1] if version else versions[-1])
        return dict(VersionId=version or str(len(versions)))
    def call(self,service,operation,**args):
        self.calls.append((service,operation,args))
        if operation=='get-caller-identity':return dict(Account=self.binding['account'],Arn='arn:aws:iam::'+self.binding['account']+':user/OpenStd')
        if operation=='describe-instances':
            tags=dict(Project='openkache',ManagedBy='terraform-integration',ExperimentRun='other' if self.foreign else self.binding['batch'])
            return dict(Reservations=[dict(Instances=[dict(InstanceId=i,InstanceType='m6gd.medium',Tags=[dict(Key=k,Value=v) for k,v in tags.items()]) for i in self.binding['instanceIds']])])
        if operation=='terminate-instances':return dict(TerminatingInstances=[dict(InstanceId=i) for i in args['instance_ids']])
        raise AssertionError(operation)


class ArchiveGuardTest(unittest.TestCase):
    def fixture(self,path):
        root=path/'results';root.mkdir()
        with (root/'matrix.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['a1','a2','seed']);writer.writeheader();writer.writerow(dict(a1='VR',a2='V07',seed='1'))
        binding=dict(campaign='ci-101',batch='ci-101-job-1',account='123456789012',
                     instanceIds=['i-'+format(i,'017x') for i in range(1,9)],bundleSha256='a'*64,
                     matrixSha256=digest(root/'matrix.csv'),expectedCases=1)
        (root/'binding.json').write_text(json.dumps(binding))
        (root/'batch-results.json').write_text(json.dumps([dict(row=0,selectors=dict(a1='VR',a2='V07',seed='1'),status='PASS')]))
        case=root/'row-000000';(case/'plan').mkdir(parents=True);run=case/'run';run.mkdir()
        (case/'console.log').write_text('experiment log\n');(case/'plan/plan.json').write_text('{}')
        for file in ('cleanup.json','collection.json'):(run/file).write_text('{"complete":true}')
        for file in ('manifest.json','result.json'):(run/file).write_text('{}')
        (run/'RUN_COMPLETE').write_text('collected')
        return root,binding,FakeAws(binding)

    def test_round_trip_reads_versioned_bytes_then_terminates_exact_owned_set(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path)
            upload(root,binding,path/'snapshot',aws)
            report=guarded_terminate(binding,aws)
            self.assertTrue(report['backup']['verified'])
            self.assertEqual(binding['instanceIds'],aws.calls[-1][2]['instance_ids'])

    def test_corrupt_archive_prevents_any_termination(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path)
            pointer=upload(root,binding,path/'snapshot',aws)
            manifest=json.loads(aws.objects[pointer['manifestKey']][-1]);aws.objects[manifest['archive']['key']][-1]=b'truncated'
            with self.assertRaisesRegex(ValueError,'checksum/size'):guarded_terminate(binding,aws)
            self.assertEqual([],aws.calls)

    def test_partial_collection_can_be_uploaded_but_never_deleted(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path)
            (root/'row-000000/run/collection.json').write_text('{"complete":false}')
            upload(root,binding,path/'snapshot',aws)
            with self.assertRaisesRegex(ValueError,'uncollected'):guarded_terminate(binding,aws)
            self.assertEqual([],aws.calls)

    def test_wrong_owner_blocks_deletion_after_valid_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path);upload(root,binding,path/'snapshot',aws);aws.foreign=True
            with self.assertRaisesRegex(ValueError,'not owned'):guarded_terminate(binding,aws)
            self.assertFalse(any(op=='terminate-instances' for _,op,_ in aws.calls))

    def test_old_run_receipt_is_not_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path);upload(root,binding,path/'snapshot',aws)
            wrong=copy.deepcopy(binding);wrong['bundleSha256']='b'*64
            with self.assertRaisesRegex(ValueError,'another run'):guarded_terminate(wrong,aws)
            self.assertEqual([],aws.calls)

    def test_symlink_evidence_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path);(root/'unsafe').symlink_to('/etc/passwd')
            with self.assertRaisesRegex(ValueError,'symlink'):pack(root,binding,path/'snapshot')

    def test_missing_case_input_is_not_a_complete_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);root,binding,aws=self.fixture(path);(root/'row-000000/plan/plan.json').unlink()
            upload(root,binding,path/'snapshot',aws)
            with self.assertRaises(ValueError):verify(binding,aws)


if __name__=='__main__':unittest.main()
