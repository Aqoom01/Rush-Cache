import csv
import itertools
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from analyze_core_focus import summarize
from core_resources import cpu_ticks,rss_kib
from core_followups import pick_candidate
import integration


class CoreFocusTest(unittest.TestCase):
    def test_candidate_tolerance_does_not_pick_a_large_rejection_tradeoff(self):
        rows=[dict(name='baseline',successFractionMedian=1.,successP99MsMedian=100),
              dict(name='close-fast',successFractionMedian=.995,successP99MsMedian=20),
              dict(name='reject-fast',successFractionMedian=.8,successP99MsMedian=1)]
        self.assertEqual('close-fast',pick_candidate(rows)['name'])

    def test_proc_metrics_parse_comm_with_spaces_and_missing_rss(self):
        fields=['S']+['0']*20
        fields[11]='103';fields[12]='27'
        self.assertEqual(130,cpu_ticks('123 (a java worker) '+' '.join(fields)))
        self.assertEqual(1024,rss_kib('Name:\tjava\nVmRSS:\t1024 kB\n'))
        self.assertIsNone(rss_kib('Name:\tjava\n'))

    def fixture(self,root,all_methods=False):
        methods=list(itertools.product(integration.A1_PROTOCOLS,integration.A2_PROTOCOLS)) if all_methods else [('RAFT','O00')]
        matrix=[];runs=[]
        for a1,a2 in methods:
            for read in (('ADAPTIVE','ROUND_ROBIN') if a2=='V06' else ('ADAPTIVE',)):
                for seed in (1,2):
                    selector=dict(a1=a1,a2=a2,read_selection=read,seed=str(seed))
                    matrix.append(selector)
                    result=dict(seed=seed,offered=100,ok=100,miss=0,failed=0,notAdmitted=0,
                                terminalGoodputIncludingDrain=100,successP99FromScheduleNanos=10000000,
                                requestTapeSha256='tape-'+str(seed),fdLiveProcessDownReports=0,
                                correctnessValid=True,migrationValid=True,coverageValid=True,jointComponentsValid=True,fdRuntimeValid=True)
                    runs.append(dict(selectors=selector,status='PASS',cleanup=dict(complete=True),result=result))
        with (root/'a1-a2-screen.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(matrix[0]));writer.writeheader();writer.writerows(matrix)
        (root/'a1-a2-screen').mkdir()
        path=root/'a1-a2-screen/batch-results.json';path.write_text(json.dumps(runs))
        return path,runs

    def test_invalid_or_incomplete_repetition_is_not_ranked(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path,runs=self.fixture(root)
            runs[1]['result']['correctnessValid']=False;path.write_text(json.dumps(runs))
            report=summarize(root)
            self.assertEqual([],report['provisionalOrder'])
            self.assertEqual(1,report['configurations'][0]['valid'])
            path.write_text(json.dumps(runs[:1]))
            self.assertEqual(0,summarize(root)['pairedComplete'])

    def test_different_request_tapes_in_same_seed_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path,runs=self.fixture(root,True)
            runs[2]['result']['requestTapeSha256']='different';path.write_text(json.dumps(runs))
            with self.assertRaisesRegex(AssertionError,'Different workload tape'):
                summarize(root)

    def test_generated_followups_have_supported_rows_and_new_seeds(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);self.fixture(root,True);output=root/'followups'
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('core_followups.py')),
                '--screen-root',str(root),'--bundle',str(root/'unused-bundle'),'--output',str(output)],capture_output=True,text=True)
            self.assertEqual(0,result.returncode,result.stderr)
            jobs=json.loads((output/'jobs.json').read_text())
            self.assertEqual([27,9,9,18,36,30],[j['runs'] for j in jobs])
            for job in jobs:
                with (output/job['matrix']).open(newline='') as stream:
                    rows=list(csv.DictReader(stream))
                self.assertEqual(job['runs'],len(rows))
                for row in rows:
                    integration.row_plan_arguments(row)
                    self.assertIn(row['seed'],('3','4','5'))
            selected=json.loads((output/'selection.json').read_text())['stressAndMigrationCandidates']
            self.assertTrue(any(c['a1'] in ('RAFT','VR') for c in selected))


if __name__=='__main__':
    unittest.main()
