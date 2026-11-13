import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/'experiments/system-integration/ec2'))
import core_pipeline

SCRIPT=Path(__file__).with_name('integration_inputs.sh')
FIELDS=('CORE_PHASE','INTEGRATION_MATRIX','INTEGRATION_PLAN_SPEC','INTEGRATION_PLAN_ARGS')


class CiInputSelectionTest(unittest.TestCase):
    def resolve(self,environment,cwd):
        code='import json,os; print(json.dumps({k:os.environ.get(k) for k in '+repr(FIELDS)+'}))'
        return subprocess.run(['sh','-c','. "$1"; exec "$2" -c "$3"','input-test',str(SCRIPT),sys.executable,code],
                              cwd=cwd,env=dict(os.environ,**environment),capture_output=True,text=True)

    def test_web_prefilled_one_row_cannot_replace_four_pilot_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);core_pipeline.prepare(root/'core-plan')
            preflight='experiments/system-integration/ec2/matrices/cloud-preflight.csv'
            environment=dict(EXPERIMENT_SUITE='integrated-core',CI_JOB_NAME='core-pilot',
                             INTEGRATION_MATRIX=str(REPO/preflight),INTEGRATION_PLAN_SPEC='core-plan/pilot.json',
                             INTEGRATION_PLAN_ARGS='["--requests","600","--rate","100"]',CORE_PHASE='manual')
            with (REPO/preflight).open() as stream:self.assertEqual(1,len(list(csv.DictReader(stream))))
            result=self.resolve(environment,root);self.assertEqual(0,result.returncode,result.stderr)
            selected=json.loads(result.stdout)
            with (root/selected['INTEGRATION_MATRIX']).open() as stream:rows=list(csv.DictReader(stream))
            spec=json.loads((root/selected['INTEGRATION_PLAN_SPEC']).read_text())
            self.assertEqual(4,len(rows));self.assertEqual(4,len(spec['perRowArguments']))
            self.assertEqual('[]',selected['INTEGRATION_PLAN_ARGS']);self.assertEqual('pilot',selected['CORE_PHASE'])
            self.assertEqual({50,150,500,1500},{int(a[a.index('--rate')+1]) for a in spec['perRowArguments']})

    def test_all_later_phases_ignore_stale_web_paths_and_phase(self):
        for phase in ('screen','working-set','balance','copy','failure','final'):
            with self.subTest(phase=phase),tempfile.TemporaryDirectory() as temp:
                result=self.resolve(dict(EXPERIMENT_SUITE='integrated-core',CI_JOB_NAME='core-'+phase+': [12]',
                                         CORE_PHASE='pilot',INTEGRATION_MATRIX='old.csv',INTEGRATION_PLAN_SPEC='old.json',INTEGRATION_PLAN_ARGS='["--rate","1"]'),temp)
                self.assertEqual(0,result.returncode,result.stderr);selected=json.loads(result.stdout)
                self.assertEqual('core-plan/'+phase+'.csv',selected['INTEGRATION_MATRIX'])
                self.assertEqual('core-plan/'+phase+'.json',selected['INTEGRATION_PLAN_SPEC'])
                self.assertEqual(phase,selected['CORE_PHASE']);self.assertEqual('[]',selected['INTEGRATION_PLAN_ARGS'])

    def test_manual_preflight_values_remain_available(self):
        with tempfile.TemporaryDirectory() as temp:
            values=dict(CORE_PHASE='manual',INTEGRATION_MATRIX='custom.csv',INTEGRATION_PLAN_SPEC='',INTEGRATION_PLAN_ARGS='["--rate","100"]')
            result=self.resolve(dict(EXPERIMENT_SUITE='integrated',CI_JOB_NAME='integration-cloud-batch: [0]',**values),temp)
            self.assertEqual(0,result.returncode,result.stderr);self.assertEqual(values,json.loads(result.stdout))

    def test_unknown_core_cloud_job_cannot_dispatch_a_different_phase(self):
        with tempfile.TemporaryDirectory() as temp:
            result=self.resolve(dict(EXPERIMENT_SUITE='integrated-core',CI_JOB_NAME='core-calibrate'),temp)
            self.assertNotEqual(0,result.returncode);self.assertIn('Unknown core cloud job',result.stderr)


if __name__=='__main__':unittest.main()
