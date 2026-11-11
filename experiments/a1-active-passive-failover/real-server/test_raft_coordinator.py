"""Offline coordinator contracts: no real AWS calls or host systemd changes."""
import copy
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from raft_ec2_job import prepare,validate,verify_package,sha,SOURCES,coordinator_policy,prefix
from raft_ec2_campaign import bootstrap,start,status,collect
from raft_ec2_worker import Publisher,run_job,instance_role_environment


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);dist=self.root/'fixture.tar';dist.write_bytes(b'distribution')
        self.args=SimpleNamespace(out=self.root/'prepared',dist=dist,account_id='123456789012',
            region='ap-northeast-2',bucket='experiment-results',instances='i-aaa,i-bbb,i-ccc',coordinator=None,
            data_root='/mnt/openkache-data',remote_python='/usr/bin/python3',max_hours=10,
            study='validation',repeat=1,core_repeat=1,detectors=['SWIM_LIFEGUARD'],scenarios=['healthy'])
        self.job=prepare(self.args)
        self.client_args=SimpleNamespace(job=self.args.out,aws_bin='aws',profile='laptop-only',live=False)

    def worker_root(self):
        root=self.root/'remote';root.mkdir();(root/'source').mkdir()
        for name in ('job.json','dist.tar','source.tar'):shutil.copyfile(self.args.out/name,root/name)
        with tarfile.open(root/'source.tar') as archive:
            for member in archive.getmembers():
                (root/'source'/member.name).write_bytes(archive.extractfile(member).read())
        return root

    def aws(self):
        aws=Mock();aws.path.side_effect=str
        nodes=[dict(InstanceId=i,State={'Name':'running'},InstanceType='m6gd.medium',VpcId='vpc-one',
                    Placement={'AvailabilityZone':'ap-northeast-2a'},Tags=[{'Value':'OpenKache experiment'}],
                    IamInstanceProfile={'Arn':'instance-role'}) for i in self.job['instances']]
        def call(*args):
            if args[0]=='sts':return {'Account':self.job['accountId']}
            if args[:2]==('ec2','describe-instances'):return {'Reservations':[{'Instances':nodes}]}
            return {}
        aws.call.side_effect=call;aws.shell.return_value='ActiveState=active'
        return aws

    def test_package_is_complete_importable_and_contains_no_credentials(self):
        self.assertNotIn('profile',json.dumps(self.job));self.assertNotIn('laptop-only',json.dumps(self.job))
        with tarfile.open(self.args.out/'source.tar') as archive:
            self.assertEqual(set(SOURCES),set(archive.getnames()))
            self.assertTrue(all(m.isfile() and '/' not in m.name for m in archive.getmembers()))
        root=self.worker_root()
        subprocess.run([sys.executable,'-c','import raft_ec2_worker, run_raft_campaign'],cwd=root/'source',check=True)
        self.assertEqual(self.job,verify_package(self.args.out))

    def test_invalid_paths_coordinator_and_tampering_are_rejected(self):
        for key,value in [('python','/usr/bin/python3;shutdown'),('dataRoot','/tmp/../etc'),('coordinator','i-ddd')]:
            job=copy.deepcopy(self.job);job[key]=value
            with self.assertRaises(ValueError):validate(job)
        (self.args.out/'dist.tar').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'changed'):verify_package(self.args.out)

    def test_job_settings_cannot_be_silently_changed_after_prepare(self):
        changed=copy.deepcopy(self.job);changed['repeat']=2
        (self.args.out/'job.json').write_text(json.dumps(changed))
        with self.assertRaisesRegex(ValueError,'prepared job changed'):verify_package(self.args.out)

    def test_policy_scopes_stop_and_ssm_to_three_nodes_without_delete_or_termination(self):
        policy=coordinator_policy(self.job);text=json.dumps(policy)
        self.assertIn(self.job['campaign'],text)
        self.assertNotIn('DeleteObject',text);self.assertNotIn('TerminateInstances',text)
        stops=next(s for s in policy['Statement'] if s['Action']==['ec2:StopInstances'])
        self.assertEqual(3,len(stops['Resource']));self.assertTrue(all('*' not in r for r in stops['Resource']))
        statement=next(s for s in policy['Statement'] if s['Action']==['ssm:SendCommand'])
        self.assertEqual(4,len(statement['Resource']))
        self.assertTrue(all(i in text for i in self.job['instances']))

    def test_start_submits_short_service_launch_and_never_laptop_profile_to_node(self):
        aws=self.aws()
        with patch('raft_ec2_campaign.AWS',return_value=aws):receipt=start(self.client_args)
        self.assertEqual('service-accepted',receipt['state']);script=aws.shell.call_args.args[1]
        self.assertIn('systemd-run --collect --service-type=exec',script)
        self.assertIn('Restart=no',script);self.assertIn('RuntimeMaxSec=36000',script)
        for forbidden in ('--scope','--wait','--pty','--pipe','laptop-only','AWS_ACCESS_KEY_ID'):
            self.assertNotIn(forbidden,script)
        self.assertEqual(3,sum(c.args[:2]==('s3','cp') for c in aws.call.call_args_list))
        self.assertEqual(120,aws.shell.call_args.kwargs['timeout'])
        subprocess.run(['bash','-n'],input=script,text=True,check=True)

    def test_wrong_account_fails_before_upload_or_service_start(self):
        aws=self.aws();aws.call.side_effect=None;aws.call.return_value={'Account':'999999999999'}
        with patch('raft_ec2_campaign.AWS',return_value=aws):
            with self.assertRaisesRegex(ValueError,'account'):start(self.client_args)
        aws.shell.assert_not_called();self.assertEqual(1,aws.call.call_count)

    def test_completed_archive_can_retry_auto_stop_with_helpers_already_stopped(self):
        aws=self.aws();original=aws.call.side_effect
        nodes=original('ec2','describe-instances')['Reservations'][0]['Instances']
        for node in nodes:
            if node['InstanceId']!=self.job['coordinator']:node['State']['Name']='stopped'
        def call(*args):
            if args[:2]==('s3','cp') and str(args[2]).startswith('s3://'):
                value=dict(complete=True,planned=1,passed=1) if args[2].endswith('/summary.json') else dict(state='complete',prefix=prefix(self.job))
                Path(args[3]).write_text(json.dumps(value));return {}
            return original(*args)
        aws.call.side_effect=call
        with patch('raft_ec2_campaign.AWS',return_value=aws):value=start(self.client_args,resume=True)
        self.assertEqual('service-accepted',value['state']);self.assertIn('--resume',aws.shell.call_args.args[1])

    def test_foreground_ec2_execution_requires_explicit_debug_flag(self):
        from raft_ec2_job import HERE
        out=self.root/'must-not-exist'
        command=[sys.executable,str(HERE/'run_raft_campaign.py'),'--out',str(out),'--instances','i-aaa,i-bbb,i-ccc',
                 '--bucket','experiment-results','--dist',str(self.args.dist),'--aws-bin','/nonexistent/forbidden-aws']
        result=subprocess.run(command,capture_output=True,text=True)
        self.assertNotEqual(0,result.returncode);self.assertIn('detached EC2 execution',result.stderr)
        self.assertFalse(out.exists())

    def test_active_service_duplicate_start_does_not_invoke_systemd_run(self):
        root=self.worker_root();bin_dir=self.root/'bin';bin_dir.mkdir()
        (bin_dir/'systemctl').write_text('#!/bin/sh\nexit 0\n')
        (bin_dir/'systemd-run').write_text('#!/bin/sh\ntouch '+str(self.root/'DUPLICATE')+'\n')
        for p in bin_dir.iterdir():p.chmod(0o755)
        with patch('raft_ec2_campaign.remote_root',return_value=str(root)):
            script=bootstrap(self.job,sha(root/'job.json'))
        subprocess.run(['bash','-se'],input=script,text=True,check=True,capture_output=True,
                       env={**os.environ,'PATH':str(bin_dir)+':'+os.environ['PATH']})
        self.assertFalse((self.root/'DUPLICATE').exists())

    def test_worker_lock_prevents_duplicate_campaign(self):
        root=self.worker_root()
        with (root/'coordinator.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError,'already running'):
                run_job(root/'job.json',aws=Mock())

    def test_unclean_active_case_is_not_automatically_resumed(self):
        root=self.worker_root();(root/'results').mkdir()
        (root/'worker-state.json').write_text(json.dumps({'activeCase':{'case':'interrupted'}}))
        aws=Mock()
        with self.assertRaisesRegex(RuntimeError,'fault cleanup'):
            run_job(root/'job.json',resume=True,aws=aws)
        aws.call.assert_not_called()

    def test_worker_checks_source_integrity_before_running(self):
        root=self.worker_root();(root/'source'/'run_raft_campaign.py').write_text('changed')
        with self.assertRaisesRegex(ValueError,'source changed'):run_job(root/'job.json',aws=Mock())

    def test_progress_persists_checkpoints_and_clears_only_finished_case(self):
        root=self.worker_root();aws=Mock();publisher=Publisher(root,self.job,aws)
        publisher.progress('case-started',{},dict(case='case-one',attempt='attempt-1'))
        self.assertEqual('case-one',json.loads((root/'worker-state.json').read_text())['activeCase']['case'])
        case=root/'results'/'case-one';case.mkdir(parents=True);(case/'result.json').write_text('{}')
        publisher.progress('case-finished',{},dict(case='case-one',attempt='attempt-1'))
        self.assertIsNone(json.loads((root/'worker-state.json').read_text())['activeCase'])
        self.assertTrue(any('/checkpoints/case-one/result.json' in str(c) for c in aws.call.call_args_list))

    def test_status_and_collect_do_not_launch_ssm(self):
        aws=self.aws()
        def download(*args):
            if args[0]=='s3' and args[2].endswith('/control/status.json'):
                Path(args[3]).write_text(json.dumps(dict(state='running',heartbeatEpochNs=time.time_ns()-120_000_000_000)))
            return {}
        aws.call.side_effect=download
        self.client_args.out=self.root/'download'
        with patch('raft_ec2_campaign.AWS',return_value=aws):
            self.assertTrue(status(self.client_args)['heartbeatStale']);collect(self.client_args)
        aws.shell.assert_not_called()
        self.assertTrue(all(c.args[:2]==('s3','cp') for c in aws.call.call_args_list))
        self.assertFalse(any('--delete' in c.args for c in aws.call.call_args_list))

    def test_live_status_can_inspect_failed_start_before_any_s3_heartbeat(self):
        aws=self.aws();original=aws.call.side_effect;self.client_args.live=True
        def missing(*args):
            if args[0]=='s3':raise RuntimeError('404 NoSuchKey')
            return original(*args)
        aws.call.side_effect=missing
        with patch('raft_ec2_campaign.AWS',return_value=aws):value=status(self.client_args)
        self.assertEqual('not-published',value['state']);self.assertIn('service',value)
        aws.shell.assert_called_once()

    def test_instance_role_does_not_inherit_laptop_credentials(self):
        with patch.dict(os.environ,{'AWS_PROFILE':'laptop','AWS_ACCESS_KEY_ID':'dummy','AWS_SESSION_TOKEN':'dummy'}):
            instance_role_environment()
            self.assertNotIn('AWS_PROFILE',os.environ);self.assertNotIn('AWS_ACCESS_KEY_ID',os.environ)
            self.assertEqual('/dev/null',os.environ['AWS_SHARED_CREDENTIALS_FILE'])

    def test_final_status_upload_failure_cannot_claim_completion(self):
        root=self.worker_root();aws=Mock();aws.path.side_effect=str
        def call(*args):
            if '--dry-run' in args:raise RuntimeError('DryRunOperation')
            if args[:2]==('s3','cp') and str(args[2]).endswith('status-upload.json'):
                if json.loads(Path(args[2]).read_text())['state']=='completed':raise RuntimeError('S3 unavailable')
            return {}
        aws.call.side_effect=call
        def campaign(args):
            args.out.mkdir();(args.out/'archive-status.json').write_text('{"state":"complete"}')
        with patch('raft_ec2_worker.run',side_effect=campaign),patch.dict(os.environ):
            self.assertEqual(1,run_job(root/'job.json',aws=aws))
        self.assertEqual('publication-failed',json.loads((root/'worker-state.json').read_text())['state'])

    def test_auto_stop_failure_preserves_experiment_success_and_reports_shutdown_error(self):
        root=self.worker_root();aws=Mock()
        def campaign(args):
            args.out.mkdir();(args.out/'archive-status.json').write_text('{"state":"complete"}')
        with patch('raft_ec2_worker.run',side_effect=campaign),patch.dict(os.environ),\
             patch('raft_ec2_worker.check_stop_permission'),\
             patch('raft_ec2_worker.stop_after_success',side_effect=RuntimeError('stop denied')):
            self.assertEqual(1,run_job(root/'job.json',aws=aws))
        state=json.loads((root/'worker-state.json').read_text())
        self.assertEqual('completed',state['state']);self.assertEqual('failed',state['autoStop']['phase'])

    def test_failed_archive_never_reaches_auto_stop(self):
        root=self.worker_root();aws=Mock()
        def campaign(args):
            args.out.mkdir();(args.out/'archive-status.json').write_text('{"state":"failed"}')
        with patch('raft_ec2_worker.run',side_effect=campaign),patch.dict(os.environ),\
             patch('raft_ec2_worker.check_stop_permission'),patch('raft_ec2_worker.stop_after_success') as shutdown:
            self.assertEqual(1,run_job(root/'job.json',aws=aws));shutdown.assert_not_called()

    def test_live_status_verifies_stopped_nodes_without_sending_ssm_to_stopped_coordinator(self):
        aws=self.aws();original=aws.call.side_effect;self.client_args.live=True
        nodes=original('ec2','describe-instances')['Reservations'][0]['Instances']
        for node in nodes:node['State']['Name']='stopped'
        def download(*args):
            if args[0]=='s3':
                Path(args[3]).write_text(json.dumps(dict(state='completed',heartbeatEpochNs=time.time_ns())))
                return {}
            return original(*args)
        aws.call.side_effect=download
        with patch('raft_ec2_campaign.AWS',return_value=aws):value=status(self.client_args)
        self.assertTrue(value['autoStopObservedComplete']);aws.shell.assert_not_called()


if __name__=='__main__':unittest.main()
