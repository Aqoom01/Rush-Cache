"""Offline coverage for the matrix, fault ownership checks and restoration paths."""
import json
from pathlib import Path
import signal
import tempfile
import unittest
import hashlib
from types import SimpleNamespace
from unittest.mock import patch

from raft_campaign_plan import matrix,SCENARIOS,CORE_SCENARIOS,DETECTORS
from raft_campaign_fault import execute
from run_raft_campaign import run,FILES,HERE,SSM
from raft_measurements import raft_journal,leadership_counts,normal_traffic
from summarize_raft_campaign import summarize,median_interval


class CampaignTests(unittest.TestCase):
    def test_full_matrix_has_126_distinct_cases_and_all_required_faults(self):
        plan=matrix()
        self.assertEqual(126,len(plan));self.assertEqual(126,len({s['case'] for s in plan}))
        self.assertEqual(set(SCENARIOS),{s['scenario'] for s in plan})
        self.assertTrue(all(s['storage']=='nvme' for s in plan if s['kind'] in ('io-stall','fio')))

    def setup_target(self,root):
        (root/'pb.pid').write_text('999999')
        return b'java\0io.openkache.server.replication.PbServerMain\0'+str(root/'node.properties').encode()+b'\0'

    def test_wrong_process_identity_prevents_fault(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);self.setup_target(root)
            with patch('raft_campaign_fault.Path.read_bytes',return_value=b'other-service'),patch('raft_campaign_fault.os.kill') as kill:
                with self.assertRaisesRegex(RuntimeError,'identity'):
                    execute(root,{'kind':'crash','seconds':0},['a','b','c'],0)
                kill.assert_not_called()

    def test_cpu_pause_always_continues_process_after_interruption(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);identity=self.setup_target(root)
            with patch('raft_campaign_fault.Path.read_bytes',return_value=identity),patch('raft_campaign_fault.os.kill') as kill,patch('raft_campaign_fault.time.sleep',side_effect=InterruptedError('cancel')):
                with self.assertRaises(InterruptedError):
                    execute(root,{'kind':'cpu-pause','seconds':2},['a','b','c'],0)
                self.assertEqual([(999999,signal.SIGSTOP),(999999,signal.SIGCONT)],[c.args for c in kill.call_args_list])
            self.assertEqual('released',json.loads((root/'fault-events.json').read_text())[-1]['event'])

    def test_cpu_contention_restores_affinity_and_terminates_workers_after_interruption(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);identity=self.setup_target(root)
            worker=Mock();worker.poll.return_value=None
            with patch('raft_campaign_fault.Path.read_bytes',return_value=identity),\
                 patch('raft_campaign_fault.Path.iterdir',return_value=[Path('/proc/999999/task/999999')]),\
                 patch('raft_campaign_fault.os.sched_getaffinity',return_value={1,2}),\
                 patch('raft_campaign_fault.os.sched_setaffinity') as affinity,\
                 patch('raft_campaign_fault.subprocess.Popen',return_value=worker),\
                 patch('raft_campaign_fault.time.sleep',side_effect=InterruptedError('cancel')):
                with self.assertRaises(InterruptedError):
                    execute(root,{'kind':'cpu-contention','seconds':2,'workers':1},['a','b','c'],0)
                self.assertEqual([(999999,{1}),(999999,{1,2})],[c.args for c in affinity.call_args_list])
                worker.terminate.assert_called_once();worker.wait.assert_called_once()
            self.assertEqual('released',json.loads((root/'fault-events.json').read_text())[-1]['event'])

    def test_partition_removes_only_its_exact_rules(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);identity=self.setup_target(root);commands=[]
            def command(argv,**kwargs):
                commands.append(argv);return ''
            with patch('raft_campaign_fault.Path.read_bytes',return_value=identity),patch('raft_campaign_fault.subprocess.check_output',side_effect=command):
                execute(root,{'kind':'partition','seconds':0},['10.1.1.1','10.1.1.2','10.1.1.3'],0)
            inserted=[c[3:] for c in commands if c[:3]==('iptables','-w','-I')]
            deleted=[c[3:] for c in commands if c[:3]==('iptables','-w','-D')]
            self.assertEqual(8,len(inserted));self.assertEqual(inserted,list(reversed(deleted)))
            self.assertTrue(all('7000' not in c for c in inserted))

    def test_local_network_fault_is_marked_as_model_and_cleans_marker(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);identity=self.setup_target(root)
            with patch('raft_campaign_fault.Path.read_bytes',return_value=identity):
                rows=execute(root,{'kind':'loss','seconds':0,'scope':'udp'},['a','b','c'],0,local_model=True)
            self.assertFalse((root/'io-fault.properties').exists())
            self.assertTrue(any(e['event']=='local-network-model' for e in rows))

    def test_completed_resume_does_not_even_initialize_backend(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);lib=root/'lib';lib.mkdir();(lib/'fixture.jar').write_bytes(b'fixture')
            out=root/'run';out.mkdir();plan=matrix(1,['SWIM'],['healthy'])
            names=(*FILES,'raft_campaign_plan.py','run_raft_campaign.py','summarize_raft_campaign.py','local_raft_fd_validation.py','ec2_ssm.py','plot_raft_campaign.py','requirements-raft-plots.txt')
            manifest=dict(matrix=plan,backend='local',scriptsSha256={n:hashlib.sha256((HERE/n).read_bytes()).hexdigest() for n in names},
                          distSha256=hashlib.sha256(b'fixture').hexdigest())
            (out/'manifest.json').write_text(json.dumps(manifest))
            case=out/plan[0]['case'];case.mkdir()
            (case/'result.json').write_text(json.dumps(dict(ok=True,schema='raft-scenario-1',spec=plan[0],preservedSuccesses=1,verifiedCopies=3)))
            args=SimpleNamespace(local=True,lib=lib,out=out,repeat=1,detectors=['SWIM'],scenarios=['healthy'],resume=True,plan_only=False)
            with patch('run_raft_campaign.Local') as backend:
                run(args);backend.assert_not_called()
            self.assertTrue(json.loads((out/'summary.json').read_text())['complete'])
            self.assertTrue((out/'report.md').exists())

    def test_preflight_failure_still_writes_pending_report(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);lib=root/'lib';lib.mkdir();(lib/'fixture.jar').write_bytes(b'fixture')
            args=SimpleNamespace(local=True,lib=lib,out=root/'run',repeat=1,detectors=['SWIM'],scenarios=['healthy'],resume=False,plan_only=False)
            with patch('run_raft_campaign.Local',side_effect=RuntimeError('preflight unavailable')):
                with self.assertRaisesRegex(RuntimeError,'preflight unavailable'):run(args)
            summary=json.loads((args.out/'summary.json').read_text())
            self.assertFalse(summary['complete']);self.assertEqual('pending',summary['cases'][0]['status'])
            self.assertTrue((args.out/'report.md').exists())

    def test_progress_upload_error_before_setup_retains_failure_and_finishes_checkpoint(self):
        events=[]
        def callback(event,manifest,details):
            events.append(event)
            if event=='case-started':raise RuntimeError('checkpoint upload unavailable')
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);lib=root/'lib';lib.mkdir();(lib/'fixture.jar').write_bytes(b'fixture')
            args=SimpleNamespace(local=True,lib=lib,out=root/'run',repeat=1,detectors=['SWIM'],scenarios=['healthy'],
                                 resume=False,plan_only=False,progress=callback)
            with patch('run_raft_campaign.Local') as factory:
                with self.assertRaisesRegex(RuntimeError,'failed/pending'):run(args)
                factory.return_value.setup.assert_not_called();factory.return_value.start.assert_not_called()
            self.assertIn('case-finished',events)
            summary=json.loads((args.out/'summary.json').read_text())
            self.assertEqual('failed',summary['cases'][0]['status'])

    def test_completed_ec2_resume_republishes_without_ssm_deploy_or_jvm_start(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);dist=root/'dist.tar';dist.write_bytes(b'fixture')
            out=root/'run';out.mkdir();plan=matrix(1,['SWIM'],['healthy'])
            names=(*FILES,'raft_campaign_plan.py','run_raft_campaign.py','summarize_raft_campaign.py',
                   'local_raft_fd_validation.py','ec2_ssm.py','plot_raft_campaign.py','requirements-raft-plots.txt')
            manifest=dict(matrix=plan,backend='ssm',resultPrefix='s3://test/campaign',
                          scriptsSha256={n:hashlib.sha256((HERE/n).read_bytes()).hexdigest() for n in names},
                          distSha256=hashlib.sha256(b'fixture').hexdigest())
            (out/'manifest.json').write_text(json.dumps(manifest))
            case=out/plan[0]['case'];case.mkdir()
            (case/'result.json').write_text(json.dumps(dict(ok=True,schema='raft-scenario-1',spec=plan[0],preservedSuccesses=1,verifiedCopies=3)))
            args=SimpleNamespace(local=False,dist=dist,out=out,repeat=1,detectors=['SWIM'],scenarios=['healthy'],
                                 resume=True,plan_only=False,aws_bin='aws',profile='test',region='ap-northeast-2')
            with patch('run_raft_campaign.SSM.__init__',side_effect=AssertionError('must not deploy')),\
                 patch('run_raft_campaign.SSM.publish') as publish,patch('run_raft_campaign.AWS') as aws:
                run(args)
                publish.assert_called_once_with(out,full=True)
                aws.return_value.shell.assert_not_called();aws.return_value.call.assert_not_called()

    def test_presentation_plan_balances_order_and_increases_prespecified_core_repetitions(self):
        plan=matrix(study='presentation')
        self.assertEqual(252,len(plan))
        for scenario in SCENARIOS:
            rows=[s for s in plan if s['scenario']==scenario]
            self.assertEqual((10 if scenario in CORE_SCENARIOS else 3)*3,len(rows))
            for repeat in (1,2,3):
                order=[s['detector'] for s in rows if s['repeat']==repeat]
                self.assertEqual(list(DETECTORS[repeat-1:]+DETECTORS[:repeat-1]),order)
        healthy=[s for s in plan if s['scenario']=='healthy']
        self.assertTrue(all((s['normalSeconds'],s['warmupSeconds'],s['normalRate'])==(60,10,50) for s in healthy))

    def test_journal_recovers_events_missed_by_polling_and_distinguishes_reelection(self):
        events=[dict(term=2,kind='STARTED',atNs=150,origin='TIMEOUT'),
                dict(term=2,kind='BECAME_LEADER',atNs=160,origin='TIMEOUT'),
                dict(term=3,kind='BECAME_LEADER',atNs=180,origin='TIMEOUT')]
        samples=[dict(node=1,status=dict(raftElectionEvents=events)),dict(node=1,status=dict(raftElectionEvents=events))]
        journal=raft_journal(samples,1,{'1':dict(beginNs=1000,endNs=1010,nodeNanoTime=100)},[1020,1030])
        self.assertEqual(3,len(journal));self.assertTrue(all(e['faultPhase']=='after' for e in journal))
        self.assertEqual(dict(leadershipAcquisitions=2,leaderIdentityChanges=1,sameLeaderReelections=1),leadership_counts(journal,0))
        samples[0]['status']['raftElectionEventsDropped']=1
        with self.assertRaisesRegex(RuntimeError,'overflow'):raft_journal(samples,1,{},[0,0])

    def test_same_term_two_leaders_is_a_safety_failure(self):
        with self.assertRaisesRegex(AssertionError,'two leaders'):
            leadership_counts([dict(kind='BECAME_LEADER',term=2,node=1),dict(kind='BECAME_LEADER',term=2,node=2)],0)

    def test_paced_normal_measurement_records_failed_requests_and_missed_slots(self):
        clock=[0.0]
        class Client:
            calls=0
            def set(self,*args,**kwargs):
                self.calls+=1;clock[0]+=.1
                return dict(status=3 if self.calls==1 else 0,epoch=1,detail='')
            def status(self,node): return dict(sentBytes=self.calls*100,fdSentBytes=int(clock[0]*10))
        result=dict(acknowledged={});client=Client()
        with patch('raft_measurements.time.monotonic',side_effect=lambda:clock[0]),\
             patch('raft_measurements.time.monotonic_ns',side_effect=lambda:int(clock[0]*1e9)),\
             patch('raft_measurements.time.sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)):
            normal_traffic(client,0,1,b'v',dict(normalSeconds=1,normalRate=50,warmupSeconds=0),result)
        self.assertGreater(result['normalMissedSlots'],0)
        self.assertLess(result['normalAchievedSuccessRate'],50)
        self.assertLess(result['normalSuccessRate'],1)
        self.assertEqual(result['normalRequests']-1,result['normalSuccesses'])
        self.assertEqual(result['normalSuccesses'],len(result['normalSamplesNs']))
        self.assertFalse(result['normalMinimumSamplesMet'])

    def test_reports_keep_failed_and_pending_and_never_turn_missing_metrics_into_zero(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);plan=matrix(1,scenarios=['crash'])
            (root/'manifest.json').write_text(json.dumps(dict(matrix=plan,backend='local')))
            for spec in plan[:2]:
                case=root/spec['case'];case.mkdir()
                (case/'result.json').write_text(json.dumps(dict(ok=spec==plan[0],schema='raft-scenario-1',spec=spec,
                      error='failure' if spec==plan[1] else '',preservedSuccesses=2,verifiedCopies=2,recoveryUpperNs=1000000,
                      falseLeaderDeadEvents=0)))
            self.assertFalse(summarize(root))
            import csv
            with (root/'runs.csv').open() as file: rows=list(csv.DictReader(file))
            self.assertEqual(['passed','failed','pending'],[r['status'] for r in rows])
            self.assertEqual('',rows[-1]['normalP50Ms'])
            self.assertEqual('',rows[0]['falseLeaderDeadEvents'])
            self.assertTrue((root/'analysis-provenance.json').exists())
            with (root/'comparison-summary.csv').open() as file: comparisons=list(csv.DictReader(file))
            self.assertTrue(all(r['pairs']=='0' for r in comparisons))
        self.assertEqual((None,None),median_interval([1,2,3]))
        self.assertEqual((2,2),median_interval([2]*10))

    def test_s3_publish_copies_summary_and_figures_without_deleting_or_uploading_cache(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);(root/'manifest.json').write_text('{}');(root/'report.md').write_text('report')
            (root/'figures').mkdir();(root/'figures'/'recovery.png').write_bytes(b'png')
            (root/'figures'/'unrelated.tmp').write_text('no')
            backend=SSM.__new__(SSM);backend.s3='s3://test/campaign';backend.aws=Mock()
            backend.aws.path.side_effect=str
            backend.publish(root,full=True)
            calls=[call.args for call in backend.aws.call.call_args_list]
            self.assertEqual(4,len(calls));self.assertTrue(all(c[:2]==('s3','cp') for c in calls))
            self.assertTrue(any(c[3].endswith('/figures/recovery.png') for c in calls))
            self.assertTrue(calls[-1][3].endswith('/archive-status.json'))
            self.assertEqual('complete',json.loads((root/'archive-status.json').read_text())['state'])

    def test_s3_publication_failure_does_not_claim_archive_complete(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as path:
            root=Path(path);(root/'report.md').write_text('report')
            backend=SSM.__new__(SSM);backend.s3='s3://test/campaign';backend.aws=Mock()
            backend.aws.path.side_effect=str;backend.aws.call.side_effect=RuntimeError('upload failed')
            with self.assertRaisesRegex(RuntimeError,'upload failed'):backend.publish(root,full=True)
            self.assertEqual('failed',json.loads((root/'archive-status.json').read_text())['state'])


if __name__=='__main__':unittest.main()
