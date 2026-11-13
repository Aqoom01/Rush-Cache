import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import subprocess
import sys
from unittest.mock import patch
REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/'experiments/system-integration/ec2'))
from cloud_archive import Aws
import test_cloud_archive as archive_tests

SOURCE=Path(__file__).with_name('integration_cloud.py')
spec=importlib.util.spec_from_file_location('integration_cloud',SOURCE);cloud=importlib.util.module_from_spec(spec);spec.loader.exec_module(cloud)


class WorkflowTest(unittest.TestCase):
    def test_matrix_spec_mismatch_fails_before_aws_or_state_creation(self):
        import os
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);bundle=path/'bundle.zip';bundle.write_bytes(b'test bundle')
            matrix=path/'matrix.csv';matrix.write_text('a1,a2\nRAFT,O00\n')
            specification=path/'spec.json';specification.write_text(json.dumps({'perRowArguments':[[],[],[],[]]}))
            argv=[str(SOURCE),'--campaign','ci-101','--batch','ci-101-11','--account','123456789012',
                  '--commit','a'*40,'--bundle',str(bundle),'--matrix',str(matrix),'--plan-spec',str(specification),
                  '--work',str(path/'work'),'--execute']
            created=__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
            environment=dict(GITLAB_CI='true',CI_COMMIT_REF_PROTECTED='true',CI_PIPELINE_ID='101',CI_JOB_ID='11',CI_PIPELINE_CREATED_AT=created)
            with patch.dict(os.environ,environment),patch.object(sys,'argv',argv),patch.object(cloud,'Aws') as aws,patch.object(cloud,'terraform') as tf,patch.object(cloud,'finalize') as finalize:
                with self.assertRaisesRegex(ValueError,'per-row argument count differs.*rows=1.*rows=4'):cloud.main()
                aws.assert_not_called();tf.assert_not_called();finalize.assert_not_called()
            self.assertFalse((path/'work').exists())

    def test_pilot_matrix_and_per_row_workload_are_sliced_together(self):
        import core_pipeline
        with tempfile.TemporaryDirectory() as temp:
            plan=Path(temp);core_pipeline.prepare(plan);spec=json.loads((plan/'pilot.json').read_text())
            args=argparse.Namespace(matrix=str(plan/'pilot.csv'),plan_spec=str(plan/'pilot.json'),start=1,limit=2)
            rows,per_row,matrix_bytes,count=cloud.batch_inputs(args,spec)
            self.assertEqual(4,count);self.assertEqual(4,len(per_row));self.assertEqual(2,len(rows))
            self.assertEqual(len(matrix_bytes.decode().splitlines()),3)
            self.assertTrue(all('--rate' in a for a in per_row[args.start:args.start+args.limit]))

    def test_new_allocations_stop_within_the_24h_campaign_budget(self):
        created='2026-11-13T00:00:00Z';start=__import__('datetime').datetime.fromisoformat(created.replace('Z','+00:00')).timestamp()
        self.assertTrue(cloud.campaign_has_window(created,start+20*3600))
        self.assertFalse(cloud.campaign_has_window(created,start+21*3600))
        self.assertFalse(cloud.campaign_has_window(created,start+24*3600))

    def test_common_workload_cannot_copy_secrets_or_change_managed_paths(self):
        cloud.validate_plan_arguments(['--requests','600','--override','fd.heartbeat.ms=200'])
        for values in [['--history','/home/kacheexp/.ssh/id_ed25519'],['--override','native.data.root=/tmp/other'],['--override','scaling.history.file=/etc/passwd']]:
            with self.assertRaises(ValueError):cloud.validate_plan_arguments(values)

    def test_destroy_plan_requires_original_address_and_id(self):
        binding=dict(batch='job',instanceIds=['i-owned'])
        ownership=dict(batch='job',resources=[dict(address='aws_instance.node[0]',type='aws_instance',id='i-owned')])
        plan=dict(resource_changes=[dict(mode='managed',address='aws_instance.node[0]',type='aws_instance',change=dict(actions=['delete'],before=dict(id='i-owned')))])
        cloud.validate_destroy(plan,ownership,binding)
        plan['resource_changes'][0]['change']['before']['id']='i-foreign'
        with self.assertRaisesRegex(ValueError,'not in the created'):cloud.validate_destroy(plan,ownership,binding)

    def test_dry_run_makes_no_work_directory_or_aws_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);bundle=path/'bundle.zip';bundle.write_bytes(b'dry run input')
            command=[sys.executable,str(SOURCE),'--campaign','ci-1','--batch','ci-1-2','--account','123456789012',
                     '--commit','a'*40,'--bundle',str(bundle),'--matrix',str(path/'not-needed.csv'),'--work',str(path/'work')]
            result=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(0,result.returncode,result.stderr)
            self.assertFalse(json.loads(result.stdout)['execute']);self.assertFalse((path/'work').exists())

    def test_stop_also_refuses_foreign_tags(self):
        with tempfile.TemporaryDirectory() as temp:
            _,binding,aws=archive_tests.ArchiveGuardTest().fixture(Path(temp));aws.foreign=True
            with self.assertRaisesRegex(ValueError,'foreign instance'):cloud.stop_owned(aws,binding['batch'],binding['account'])
            self.assertFalse(any(op=='stop-instances' for _,op,_ in aws.calls))

    def test_topology_is_six_replicas_separate_gateway_and_load(self):
        ids=['i'+str(i) for i in range(8)];ips=['10.0.0.'+str(i+1) for i in range(8)]
        topology=cloud.make_topology(ids,ips)
        self.assertEqual(ips[:6],topology['replica_hosts'])
        self.assertEqual(ips[6],topology['gateway_host'])
        self.assertEqual(ips[7],topology['load_host'])
        with self.assertRaises(ValueError):cloud.make_topology(ids,ips[:7]+[ips[0]])

    def test_s3_new_version_cannot_satisfy_old_expected_version(self):
        response=argparse.Namespace(returncode=0,stdout='{"VersionId":"changed"}',stderr='')
        with patch('cloud_archive.subprocess.run',return_value=response):
            with self.assertRaisesRegex(ValueError,'version changed'):Aws().get('key',Path('/tmp/unused'),'expected')

    def test_backup_failure_never_reaches_terraform_destroy(self):
        with tempfile.TemporaryDirectory() as temp:
            work=Path(temp);_,binding,_=archive_tests.ArchiveGuardTest().fixture(work)
            (work/'binding.json').write_text(json.dumps(binding))
            args=argparse.Namespace(batch=binding['batch'],campaign=binding['campaign'],account=binding['account'])
            with patch.object(cloud,'guarded_terminate',side_effect=ValueError('bad checksum')),patch.object(cloud,'stop_owned',return_value={'deleted':False}) as stop,patch.object(cloud,'terraform') as tf:
                self.assertFalse(cloud.finalize(args,work,object()))
                tf.assert_not_called();stop.assert_called_once()
                self.assertTrue((work/'cleanup-blocked.json').exists())

    def test_foreign_state_resource_cannot_be_destroyed(self):
        with tempfile.TemporaryDirectory() as temp:
            work=Path(temp);_,binding,_=archive_tests.ArchiveGuardTest().fixture(work);(work/'binding.json').write_text(json.dumps(binding))
            (work/'resource-ownership.json').write_text(json.dumps(dict(batch=binding['batch'],resources=[])))
            args=argparse.Namespace(batch=binding['batch'],campaign=binding['campaign'],account=binding['account'],commit='a'*40)
            commands=[]
            def tf(*values):
                commands.append(values[2:])
                return json.dumps({'resource_changes':[{'mode':'managed','type':'aws_s3_bucket','change':{'actions':['delete']}}]}) if 'show' in values else ''
            with patch.object(cloud,'guarded_terminate',return_value={'backup':{'verified':True}}),patch.object(cloud,'stop_owned',return_value={'deleted':False}),patch.object(cloud,'terraform',side_effect=tf),patch.object(cloud,'run'):
                self.assertFalse(cloud.finalize(args,work,object()))
                self.assertFalse(any('apply' in command for command in commands))


if __name__=='__main__':unittest.main()
