import csv
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import core_pipeline as core
import headless_worker
from cloud_archive import verify
import test_cloud_archive as archive_tests


def fake_record(row,rate=500,fraction=1.,latency=1000000):
    offered=rate*60
    result=dict(seed=int(row['seed']),configuredRate=rate,offered=offered,ok=int(offered*fraction),miss=0,failed=0,
                successP99FromScheduleNanos=latency,fdLiveProcessDownReports=0,correctnessValid=True,migrationValid=True,
                coverageValid=True,jointComponentsValid=True,fdRuntimeValid=True)
    return dict(selectors=row,status='PASS',result=result)


class CorePipelineTest(unittest.TestCase):
    def test_verified_receipts_drive_all_stages_and_final_report_without_cloud_calls(self):
        import shutil,sys
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp);plan=base/'plan';core.prepare(plan);all_receipts=base/'all'
            def receipts(phase):
                folder=base/phase;batch=folder/('job-'+phase);batch.mkdir(parents=True)
                spec=json.loads((plan/(phase+'.json')).read_text())
                with (plan/(phase+'.csv')).open() as stream:rows=list(csv.DictReader(stream))
                results=[]
                for index,r in enumerate(rows):
                    args=spec['planArguments']+spec['perRowArguments'][index]
                    rate=int(args[args.index('--rate')+1]);fraction=.9 if phase=='pilot' and rate>=500 else 1.
                    record=fake_record(r,rate,fraction);record['row']=index;results.append(record)
                for r in results:r['gateway']=dict(completedMigrations=1)
                receipt=dict(verified=True,binding=dict(batch='job-'+phase),manifestKey='mock/'+phase,phase=phase,experimentResults=results)
                (batch/'backup-verified.json').write_text(json.dumps(receipt));(batch/'cleanup-complete.json').write_text('{"terraformDestroyed":true}')
                shutil.copytree(batch,all_receipts/batch.name)
                return folder
            core.calibrate(plan,core.read_receipts(receipts('pilot'),4))
            core.choose_initial(plan,core.read_receipts(receipts('screen'),63))
            core.followups(plan,core.read_receipts(receipts('working-set'),18))
            for phase in ('balance','copy','failure'):receipts(phase)
            component_records=[r for r in core.read_receipts(all_receipts) if r['phase'] in ('balance','copy','failure')]
            core.finalize_selection(plan,component_records);receipts('final')
            with patch.object(sys,'argv',['core_pipeline.py','report','--plan',str(plan),'--receipts',str(all_receipts)]),patch.object(core,'Aws',side_effect=AssertionError('unexpected AWS call')):
                core.main()
            report=json.loads((plan/'report.json').read_text())
            self.assertTrue(report['complete']);self.assertEqual(159,report['collectedCases']);self.assertEqual(2,len(report['finalComparisons']))

    def test_final_validation_combines_measured_b_copy_and_fd_choices(self):
        with tempfile.TemporaryDirectory() as temp:
            plan=Path(temp);candidates=[dict(a1='VR',a2='V07',read_selection='ADAPTIVE'),dict(a1='CHAIN',a2='V05',read_selection='ADAPTIVE')]
            core.save(plan/'selection.json',dict(migrationCandidates=candidates))
            core.save(plan/'rates.json',dict(moderate=100,high=300));records=[]
            profiles=[('ORDER','NONE','NONE'),('ORDER','MIDPOINT','GREEDY'),('ORDER','HEAT','GREEDY'),('ORDER','HEAT','SLICER'),('RANGE','HEAT','GREEDY'),('RING_ALLOC','NONE','NONE')]
            for c in candidates:
                for seed in (2,3):
                    for d,s,p in profiles:
                        r=fake_record(core.row(c,seed,distribution=d,split=s,placement=p),latency=1000000 if p=='SLICER' else 10000000);r['phase']='balance';records.append(r)
                    for copy in ('STOP','PRE','POST'):
                        r=fake_record(core.row(c,seed,copy=copy),latency=1000000 if copy=='POST' else 10000000);r.update(phase='copy',gateway=dict(completedMigrations=1));records.append(r)
                    for fd in ('F3','REDIS_CLUSTER','PHI_GOSSIP','IGNITE_TCP_RING','AEROSPIKE_MESH'):
                        r=fake_record(core.row(c,seed,fd=fd),fraction=.99 if fd=='AEROSPIKE_MESH' else .8);r['phase']='failure';r['result']['failed']=5
                        if fd=='PHI_GOSSIP':r['result'].update(ok=r['result']['offered'],fdLiveProcessDownReports=2)
                        records.append(r)
            core.finalize_selection(plan,records)
            selected=json.loads((plan/'selection.json').read_text())['finalJointSettings'][1:]
            for setting in selected:self.assertEqual(('SLICER','POST','AEROSPIKE_MESH'),(setting['placement'],setting['copy'],setting['fd']))
            with (plan/'final.csv').open() as stream:rows=list(csv.DictReader(stream))
            self.assertEqual(18,len(rows));self.assertTrue(all(r['scenario']=='SCALE_CYCLE' and r['key_mode']=='ORDERED' for r in rows))

    def test_pilot_selects_rates_and_full_design_has_155_plus_four_cases(self):
        with tempfile.TemporaryDirectory() as temp:
            plan=Path(temp);core.prepare(plan)
            pilot=core.row(dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE'))
            core.calibrate(plan,[fake_record(pilot,rate,fraction) for rate,fraction in [(50,1.),(150,1.),(500,.9),(1500,.5)]])
            rates=json.loads((plan/'rates.json').read_text());self.assertEqual((150,500),(rates['moderate'],rates['high']))
            with (plan/'screen.csv').open() as stream:screen=list(csv.DictReader(stream))
            self.assertEqual(63,len(screen))
            records=[fake_record(r,latency=(1 if r['a2']=='V07' else 10)*1000000) for r in screen]
            core.choose_initial(plan,records)
            with (plan/'working-set.csv').open() as stream:large=list(csv.DictReader(stream))
            core.followups(plan,[fake_record(r,latency=(1 if r['a2']=='V05' else 20)*1000000) for r in large])
            counts={}
            for name in ('pilot','screen','working-set','balance','copy','failure','final'):
                with (plan/(name+'.csv')).open() as stream:rows=list(csv.DictReader(stream))
                spec=json.loads((plan/(name+'.json')).read_text());self.assertEqual(len(rows),len(spec['perRowArguments']))
                for r in rows:core.row_plan_arguments(r)
                counts[name]=len(rows)
            self.assertEqual(dict(pilot=4,screen=63,**{'working-set':18},balance=24,copy=12,failure=20,final=18),counts)
            self.assertEqual(159,sum(counts.values()))
            selected=json.loads((plan/'selection.json').read_text());self.assertTrue(any(r['a2']=='V05' for r in selected['migrationCandidates']))

    def test_unverified_or_unfinished_batches_cannot_advance_selection(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);batch=root/'job';batch.mkdir()
            (batch/'backup-verified.json').write_text(json.dumps(dict(verified=True,binding=dict(batch='job'),experimentResults=[])))
            with self.assertRaises(FileNotFoundError):core.read_receipts(root,63)
            (batch/'cleanup-complete.json').write_text('{"terraformDestroyed":false}')
            with self.assertRaises(ValueError):core.read_receipts(root,63)
            (batch/'cleanup-complete.json').write_text('{"terraformDestroyed":true}')
            with self.assertRaises(ValueError):core.read_receipts(root,63)

    def test_invalid_results_never_create_a_candidate(self):
        r=fake_record(core.row(dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE')))
        r['result']['correctnessValid']=False
        with self.assertRaises(ValueError):core.aggregate([r],1)

    def test_worker_preserves_all_rows_and_uploads_a_verifiable_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);_,binding,aws=archive_tests.ArchiveGuardTest().fixture(root)
            rows=[dict(a1='VR',a2='V07',seed='1'),dict(a1='VR',a2='V07',seed='2')]
            output=io.StringIO(newline='');writer=csv.DictWriter(output,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
            binding['expectedCases']=2;binding['matrixSha256']=hashlib.sha256(output.getvalue().encode()).hexdigest()
            job=dict(binding=binding,rows=rows,topology={},planArguments=[],caseTimeoutSeconds=600,
                     perRowArguments=[['--rate','50'],['--rate','150']])
            class Process:
                def __init__(self,command,**kwargs):
                    out=Path(command[command.index('--output')+1]);matrix=Path(command[command.index('--matrix')+1])
                    with matrix.open() as stream:one=list(csv.DictReader(stream))[0]
                    args=[a.removeprefix('--plan-arg=') for a in command if a.startswith('--plan-arg=')]
                    rate=int(args[args.index('--rate')+1]);case=out/'row-000000';(case/'plan').mkdir(parents=True);run=case/'run';run.mkdir()
                    (case/'console.log').write_text('mock controller');(case/'plan/plan.json').write_text('{}')
                    for file in ('collection.json','cleanup.json'):(run/file).write_text('{"complete":true}')
                    (run/'manifest.json').write_text('{}');(run/'RUN_COMPLETE').write_text('collected')
                    record=fake_record(one,rate);record.update(row=0,cleanup=dict(complete=True),exitCode=0)
                    (run/'result.json').write_text(json.dumps(record['result']));(out/'batch-results.json').write_text(json.dumps([record]))
                    self.returncode=0
                def poll(self):return 0
            with patch.object(headless_worker.subprocess,'Popen',Process),patch.object(headless_worker,'Aws',return_value=aws),patch.object(headless_worker.signal,'signal'):
                self.assertEqual(0,headless_worker.execute(job,root/'job.json',base=root/'worker'))
            receipt=verify(binding,aws)
            self.assertEqual([50,150],[r['result']['configuredRate'] for r in receipt['experimentResults']])


if __name__=='__main__':unittest.main()
