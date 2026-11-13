import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/'experiments/system-integration/ec2'))
import test_cloud_archive as archive_tests
from cloud_archive import upload
spec=importlib.util.spec_from_file_location('integration_diagnose',Path(__file__).with_name('integration_diagnose.py'))
diagnostics=importlib.util.module_from_spec(spec);spec.loader.exec_module(diagnostics)


class BackupDiagnosticsTest(unittest.TestCase):
    def fixture(self,path):
        root,binding,aws=archive_tests.ArchiveGuardTest().fixture(path)
        binding['batch']='ci-101-11';(root/'binding.json').write_text(json.dumps(binding))
        problem={'complete':False,'failures':[{'role':'gateway','path':'a1-authority/ready.json','error':'No such file'}]}
        (root/'row-000000/run/collection.json').write_text(json.dumps(problem))
        source=path/'job.json';source.write_text(json.dumps({'binding':binding}))
        aws.put('bundles/integration/jobs/ci-101/ci-101-11.json',source)
        pointer=upload(root,binding,path/'snapshot',aws)
        return root,binding,aws,pointer

    def test_partial_backup_exposes_original_failure_without_allowing_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);_,binding,aws,_=self.fixture(path)
            with contextlib.redirect_stdout(io.StringIO()):report=diagnostics.diagnose('101','11',binding['account'],path/'diagnostic',aws)
            self.assertFalse(report['collectionVerified']);self.assertFalse(report['collectionComplete'])
            self.assertEqual('gateway',report['cases'][0]['collection']['failures'][0]['role'])
            self.assertIn('incomplete collection.json',report['collectionProblem'])
            self.assertTrue((path/'diagnostic/diagnostics.json').is_file())
            self.assertEqual(['get-caller-identity','describe-instances'],[op for _,op,_ in aws.calls])

    def test_corrupted_partial_archive_is_not_reported_as_verified_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);_,binding,aws,pointer=self.fixture(path)
            manifest=json.loads(aws.objects[pointer['manifestKey']][-1]);aws.objects[manifest['archive']['key']][-1]=b'broken'
            with self.assertRaisesRegex(ValueError,'checksum/size'):
                diagnostics.diagnose('101','11',binding['account'],path/'diagnostic',aws)
            self.assertFalse((path/'diagnostic/diagnostics.json').exists())
            self.assertFalse(any(op in ('start-instances','stop-instances','terminate-instances') for _,op,_ in aws.calls))

    def test_dry_run_needs_no_aws_or_output_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            work=Path(temp)/'untouched'
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('integration_diagnose.py')),
                                   '--source-pipeline','295003','--source-job','952587','--account','123456789012','--work',str(work)],capture_output=True,text=True)
            self.assertEqual(0,result.returncode,result.stderr)
            self.assertFalse(json.loads(result.stdout)['execute']);self.assertFalse(work.exists())


if __name__=='__main__':unittest.main()
