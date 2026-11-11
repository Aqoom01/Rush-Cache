"""Completion gates and shutdown ordering using simulated AWS only."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from raft_ec2_job import prepare,atomic_json,prefix,coordinator_policy
from raft_campaign_plan import matrix
from raft_ec2_stop import check_stop_permission,stop_after_success


class AutoStopTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        dist=self.root/'dist';dist.write_bytes(b'dist')
        self.job=prepare(SimpleNamespace(out=self.root/'package',dist=dist,account_id='123456789012',region='ap-northeast-2',
            bucket='test-results',instances='i-aaa,i-bbb,i-ccc',coordinator=None,data_root='/mnt/openkache-data',
            remote_python='/usr/bin/python3',max_hours=10,study='validation',repeat=1,core_repeat=1,
            detectors=['SWIM_LIFEGUARD'],scenarios=['healthy']))
        self.out=self.root/'results';self.out.mkdir()
        job=self.job;plan=matrix(job['repeat'],job['detectors'],job['scenarios'],job['study'],job['coreRepeat'])
        atomic_json(self.out/'manifest.json',dict(backend='ssm',campaign=job['campaign'],matrix=plan,instances=job['instances']))
        atomic_json(self.out/'summary.json',dict(complete=True,planned=1,passed=1,cases=[dict(case=plan[0]['case'],status='passed')]))
        for name in ('runs.csv','conditions.csv','comparison-summary.csv','report.md','analysis-provenance.json'):
            (self.out/name).write_text('evidence')
        atomic_json(self.out/'archive-status.json',dict(state='complete',prefix=prefix(job),
                     files=sorted(p.name for p in self.out.iterdir())))
        self.remote=self.root/'s3';shutil.copytree(self.out,self.remote)
        self.states={i:'running' for i in job['instances']};self.trace=[];self.fail_coordinator=False
        self.worker_stops_finish=True;self.wrong_tag=False;self.aws=Mock();self.aws.path.side_effect=str
        def call(*args):
            if args[:2]==('s3','cp'):
                if str(args[2]).startswith('s3://'):shutil.copyfile(self.remote/str(args[2]).rsplit('/',1)[-1],args[3])
                elif str(args[3]).endswith('/control/auto-stop.json'):
                    self.trace.append(('publish',json.loads(Path(args[2]).read_text())['phase']))
                return {}
            if args[0]=='s3api':
                name=args[args.index('--key')+1].split('/',1)[1]
                return dict(ContentLength=(self.remote/name).stat().st_size)
            if args[0]=='sts':return {'Account':job['accountId']}
            if args[:2]==('ec2','describe-instances'):
                ids=list(args[3:])
                return {'Reservations':[{'Instances':[dict(InstanceId=i,InstanceType='m6gd.medium',RootDeviceType='ebs',
                    State={'Name':self.states[i]},Tags=[{'Value':'unrelated' if self.wrong_tag else 'OpenKache experiment'}]) for i in ids]}]}
            if args[:2]==('ec2','stop-instances'):
                if '--dry-run' in args:raise RuntimeError('DryRunOperation')
                ids=list(args[3:]);self.trace.append(('stop',ids))
                if self.fail_coordinator and ids==[job['coordinator']]:raise RuntimeError('stop denied')
                for i in ids:self.states[i]='stopped' if self.worker_stops_finish and i!=job['coordinator'] else 'stopping'
                return dict(StoppingInstances=[dict(InstanceId=i,CurrentState={'Name':'stopping'}) for i in ids])
            raise AssertionError('unexpected simulated AWS action '+str(args))
        self.aws.call.side_effect=call;self.publisher=Mock()

    def run_stop(self,**kwargs):stop_after_success(self.job,self.out,self.aws,self.publisher,poll_seconds=0,**kwargs)
    def stop_calls(self):return [x[1] for x in self.trace if x[0]=='stop']

    def test_default_auto_stop_scopes_policy_and_keep_running_removes_stop_permission(self):
        self.assertTrue(self.job['stopOnSuccess'])
        job=copy.deepcopy(self.job);job['stopOnSuccess']=False
        self.assertNotIn('StopInstances',json.dumps(coordinator_policy(job)))
        stop_after_success(job,self.out,self.aws,self.publisher);self.aws.call.assert_not_called()

    def test_dry_run_accepts_only_permission_confirmation_and_does_not_stop(self):
        check_stop_permission(self.job,self.aws);self.assertFalse(self.stop_calls())
        self.assertIn('--dry-run',self.aws.call.call_args.args)
        self.aws.call.side_effect=RuntimeError('UnauthorizedOperation')
        with self.assertRaisesRegex(RuntimeError,'permission preflight'):check_stop_permission(self.job,self.aws)

    def test_workers_stop_before_coordinator_and_intent_is_saved_before_self_stop(self):
        self.run_stop()
        self.assertEqual([['i-aaa','i-bbb'],['i-ccc']],self.stop_calls())
        self.assertLess(self.trace.index(('publish','coordinator-stop-submitting')),self.trace.index(('stop',['i-ccc'])))
        phase=json.loads((self.root/'auto-stop.json').read_text())
        self.assertEqual('coordinator-stop-accepted',phase['phase']);self.assertFalse(phase['coordinatorStopConfirmed'])
        self.assertFalse(any('--force' in c.args or '--skip-os-shutdown' in c.args for c in self.aws.call.call_args_list))

    def test_incomplete_or_failed_run_never_stops_nodes(self):
        path=self.out/'summary.json';value=json.loads(path.read_text());value['complete']=False;atomic_json(path,value)
        with self.assertRaisesRegex(RuntimeError,'every planned'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_complete_flag_cannot_hide_missing_planned_case(self):
        path=self.out/'summary.json';value=json.loads(path.read_text());value['cases']=[];atomic_json(path,value)
        with self.assertRaisesRegex(RuntimeError,'every planned'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_unverified_archive_never_stops_nodes(self):
        path=self.out/'archive-status.json';value=json.loads(path.read_text());value['state']='failed';atomic_json(path,value)
        with self.assertRaisesRegex(RuntimeError,'matching archive'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_s3_readback_mismatch_never_stops_nodes(self):
        (self.remote/'summary.json').write_text('{}')
        with self.assertRaisesRegex(RuntimeError,'readback differs'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_missing_s3_artifact_never_stops_nodes(self):
        (self.remote/'report.md').unlink()
        with self.assertRaises(FileNotFoundError):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_wrong_account_or_unrelated_node_is_not_stopped(self):
        self.wrong_tag=True
        with self.assertRaisesRegex(RuntimeError,'identity/root disk/tag'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_wrong_aws_account_is_not_stopped(self):
        original=self.aws.call.side_effect
        def call(*args):return {'Account':'999999999999'} if args[0]=='sts' else original(*args)
        self.aws.call.side_effect=call
        with self.assertRaisesRegex(RuntimeError,'account mismatch'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_local_backend_evidence_cannot_authorize_ec2_stop(self):
        path=self.out/'manifest.json';value=json.loads(path.read_text());value['backend']='local';atomic_json(path,value)
        with self.assertRaisesRegex(RuntimeError,'manifest does not match'):self.run_stop()
        self.assertFalse(self.stop_calls())

    def test_timeout_stopping_workers_retains_coordinator(self):
        self.worker_stops_finish=False
        with patch('raft_ec2_stop.time.monotonic',side_effect=[0,2]):
            with self.assertRaisesRegex(TimeoutError,'coordinator retained'):self.run_stop(timeout=1)
        self.assertEqual([['i-aaa','i-bbb']],self.stop_calls());self.assertEqual('running',self.states['i-ccc'])

    def test_retry_with_workers_already_stopped_only_requests_coordinator_stop(self):
        self.states.update({'i-aaa':'stopped','i-bbb':'stopped'})
        self.run_stop();self.assertEqual([['i-ccc']],self.stop_calls())

    def test_coordinator_stop_denial_does_not_claim_shutdown_succeeded(self):
        self.fail_coordinator=True
        with self.assertRaisesRegex(RuntimeError,'stop denied'):self.run_stop()
        self.assertEqual('running',self.states['i-ccc'])
        self.assertEqual('coordinator-stop-submitting',json.loads((self.root/'auto-stop.json').read_text())['phase'])


if __name__=='__main__':unittest.main()
