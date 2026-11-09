"""Protect experiment evidence: reject incomplete/incorrect cohorts and ID collisions."""
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from final_bench import WorkerClient, summarize
from run_final_validation import matrix
from analyze_final_validation import validate_result, analyze


class FinalValidationTest(unittest.TestCase):
    def test_plan_balanced_and_repeatable(self):
        cases = matrix()
        self.assertEqual(cases, matrix())
        self.assertEqual(len(cases), 250)
        self.assertEqual(sum(c['scenario'] == 'crash' for c in cases), 40)
        self.assertEqual(sum(c['scenario'] == 'write' for c in cases), 120)
        self.assertEqual(sum(c['scenario'] == 'read' for c in cases), 90)
        self.assertEqual(len({c['caseId'] for c in cases}), len(cases))

    def test_concurrent_clients_have_distinct_wire_identity(self):
        bodies = []
        for identity in (100, 101):
            c = WorkerClient(['unused'] * 3, identity)
            c.request = lambda node, opcode, body: bodies.append(body)
            c.set(0, 1, b'k', b'v')
        self.assertNotEqual(struct.unpack('!Q', bodies[0][:8]), struct.unpack('!Q', bodies[1][:8]))

    def test_retry_cost_not_discarded(self):
        r = summarize([{'latencyNs': 100, 'attempts': [{'status': 6}, {'status': 0}]}], 200)
        self.assertEqual(r['p99Ns'], 100)
        self.assertEqual(r['attemptStatusCounts'], {'6': 1, '0': 1})
        self.assertEqual(r['logicalOk'], 1)

    def test_truncated_write_and_duplicate_keys_rejected(self):
        case = dict(scenario='write', protocol='PB', value=64, count=2)
        r = {'schema': 'a1-final-validation-1', 'ok': True, 'config': case,
             'measurement': {'logicalOk': 2, 'elapsedNs': 100, 'samplesNs': [10, 20]},
             'verifiedCopies': 6, 'records': [{'key': 'a', 'attempts': [{'status': 0}]}, {'key': 'b', 'attempts': [{'status': 0}]}]}
        validate_result(case, r)
        for field, value in [('verifiedCopies', 5), ('records', [r['records'][0]] * 2), ('ok', False)]:
            bad = copy.deepcopy(r)
            bad[field] = value
            with self.assertRaises(RuntimeError):
                validate_result(case, bad)

    def test_timed_read_requires_duration_and_every_worker(self):
        case = dict(scenario='read', seconds=1)
        r = {'schema': 'a1-final-validation-1', 'ok': True, 'config': case,
             'measurement': {'logicalOk': 2, 'elapsedNs': 1_000_000_000, 'samplesNs': [1, 2]},
             'perClientCounts': [1, 1]}
        validate_result(case, r)
        r['perClientCounts'] = [2, 0]
        with self.assertRaises(RuntimeError):
            validate_result(case, r)
        r['perClientCounts'] = [1, 1]
        r['measurement']['elapsedNs'] = 10
        with self.assertRaises(RuntimeError):
            validate_result(case, r)

    def test_crash_rejects_false_preservation_and_wrong_fault_interval(self):
        case = {'scenario': 'crash'}
        records = [{'operation': i, 'node': 0 if i < 32 else 1,
                    'epoch': 0 if i < 32 else 1, 'endNs': i + 100,
                    'attempts': [{'startNs': 100, 'endNs': 132}]} for i in range(49)]
        r = {'schema': 'a1-final-validation-1', 'ok': True, 'config': case,
             'records': records, 'verifiedCopies': 98, 'preservedSuccesses': 49,
             'originalLeader': 0, 'statesInitial': [{'epoch': 0}],
             'fault': {'sentNs': 110, 'callBeginNs': 111, 'callReturnNs': 112, 'firstNewLeaderOkNs': 132},
             'failoverNs': 21}
        validate_result(case, r)
        bad = copy.deepcopy(r)
        bad['verifiedCopies'] = 96
        with self.assertRaises(RuntimeError):
            validate_result(case, bad)
        bad = copy.deepcopy(r)
        bad['fault']['callBeginNs'] = 99
        with self.assertRaises(RuntimeError):
            validate_result(case, bad)

    def test_analyzer_writes_csv_and_refuses_corruption(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case = dict(caseId='case-0000', scenario='read', protocol='CHAIN',
                        value=64, clients=1, repeat=1, seconds=1)
            r = {'schema': 'a1-final-validation-1', 'ok': True, 'config': case,
                 'measurement': {'logicalOk': 1, 'elapsedNs': 1_000_000_000,
                                 'samplesNs': [10], 'p99Ns': 10, 'okOpsPerSecond': 1},
                 'perClientCounts': [1]}
            raw = root / 'case-0000.json'
            raw.write_text(json.dumps(r))
            case['resultSha256'] = hashlib.sha256(raw.read_bytes()).hexdigest()
            (root / 'executed-dist.tar').write_bytes(b'fixture')
            manifest = {'executed': True, 'complete': True, 'cleanup': [{'ok': True}] * 3,
                        'distSha256': hashlib.sha256(b'fixture').hexdigest(),
                        'scriptsSha256': {}, 'cases': [case]}
            (root / 'manifest.json').write_text(json.dumps(manifest))
            analyze(root, root / 'summary')
            self.assertIn('p99Ms_median', (root / 'summary' / 'summary.csv').read_text())
            raw.write_text('{}')
            with self.assertRaisesRegex(RuntimeError, 'hash mismatch'):
                analyze(root, root / 'corrupt')


if __name__ == '__main__':
    unittest.main()
