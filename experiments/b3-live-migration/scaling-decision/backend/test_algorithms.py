"""논문 핵심 계산·실행 경계·원본 검증의 회귀 테스트."""
from dataclasses import asdict, replace
from pathlib import Path
import copy
import itertools
import json
import math
import tempfile
import unittest
import numpy as np
from threadpoolctl import threadpool_limits

from cluster import Config, Cluster, Unit, Plan, METHODS, trace, history, validate, moved_bytes, queue_sample, digest
from algorithms import Models, Scads, Estore, Accordion, Spar, Tiramola, Pax, Slicer, solve_placement, dynamic_schedule, arima_forecast
from experiment import execute, configuration, validate_record, write_json, rebuild, SCHEMA, GENERATOR


class AlgorithmsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = replace(Config(), windows=32, forecast_horizon=16)
        cls.models = Models(cls.config)

    def snapshot(self, method='scads', hot=False):
        c = self.config
        cluster = Cluster(c, method)
        r = np.ones(c.keys) * 3
        if hot:
            r[:4] = 40
        return dict(t=3, active=cluster.active, units=cluster.units, reads=r, writes=np.zeros(c.keys),
                    cpu={n: .5 for n in cluster.active}, p99=.05, total=int(sum(r)), completed=int(sum(r)),
                    hints=tuple(cluster.hints), config=c)

    def test_calibration_held_out_loads(self):
        low, _ = queue_sample(30, .1, 713, self.config)
        high, _ = queue_sample(130, .1, 713, self.config)
        self.assertLess(low, self.config.slo)
        self.assertGreater(high, self.config.slo)
        self.assertTrue(self.models.fits(27, 3))
        self.assertFalse(self.models.fits(117, 13))

    def test_milp_optimum_matches_exhaustive_assignments(self):
        c = replace(self.config, keys=4)
        units = tuple(Unit((i,), (0 if i < 3 else 1,)) for i in range(4))
        reads, writes = np.ones(4) * 30, np.zeros(4)
        placed, status = solve_placement(units, (0, 1), reads, writes, c, .60)
        brute = min(moved_bytes(units, tuple(Unit((i,), (assignment[i],)) for i in range(4)), c)
                    for assignment in itertools.product((0, 1), repeat=4)
                    if all(assignment.count(n) * .30 <= .60 + 1e-9 for n in (0, 1)))
        self.assertEqual(status, 0)
        self.assertEqual(moved_bytes(units, placed, c), brute)
        impossible, status = solve_placement(units, (0, 1), reads, writes, c, .50)
        self.assertIsNone(impossible)
        self.assertEqual(status, 2)

    def test_scads_uses_replication_for_indivisible_hot_bin(self):
        s = self.snapshot(hot=True)
        plan = Scads(self.models, []).decide(s)
        validate(plan, self.config)
        self.assertGreater(plan.detail['replication_steps'], 0)
        self.assertTrue(any(len(u.holders) > 1 for u in plan.units))

    def test_estore_refines_hot_tuple_without_losing_keys(self):
        s = self.snapshot('estore', hot=True)
        s['cpu'][0] = .95
        plan = Estore(self.models, []).decide(s)
        validate(plan, self.config)
        self.assertTrue(any(len(u.keys) == 1 for u in plan.units))
        self.assertEqual(plan.detail['hot_keys'], [0, 1, 2, 3])

    def test_dynamic_program_provisions_before_forecast_peak(self):
        predictions = [80.] * 4 + [260.] * 12
        cost, schedule = dynamic_schedule(predictions, 2, 100, self.config)
        self.assertTrue(math.isfinite(cost))
        self.assertEqual(schedule[0][0], 0)
        self.assertGreater(schedule[0][2], 2)
        cost, schedule = dynamic_schedule([1000.] * 16, 2, 100, self.config)
        self.assertTrue(math.isinf(cost))
        self.assertFalse(schedule)

    def test_spar_learns_periodic_pattern_from_past_only(self):
        c = self.config
        past = [200 + 80 * math.sin(2 * math.pi * t / c.period)
                for t in range(-c.period * c.training_periods, 0)]
        model = Spar(past, c)
        predicted = np.array(model.predict(past)) / 1.1
        expected = np.array([200 + 80 * math.sin(2 * math.pi * t / c.period) for t in range(c.forecast_horizon)])
        self.assertLess(float(np.max(abs(predicted - expected))), .001)

    def test_arima_constant_history_and_finite_ramp(self):
        estimate, fit = arima_forecast([100.] * 64, 2)
        self.assertAlmostEqual(estimate, 100.)
        estimate, fit = arima_forecast(list(range(64)), 2)
        self.assertGreater(estimate, 63)
        self.assertEqual(fit['order'], [1, 1, 1])

    def test_tiramola_updates_state_action_values(self):
        policy = Tiramola(self.models, [])
        plan = policy.decide(self.snapshot('tiramola'))
        validate(plan, self.config)
        self.assertGreater(plan.detail['updates'], 0)
        self.assertEqual(len(policy.data), len(self.models.reward_data) + 1)
        self.assertGreater(float(np.max(abs(policy.q))), 0)
        self.assertLessEqual(abs(len(plan.active) - 4), 1)

    def test_pax_acquires_hot_replica_and_keeps_coverage(self):
        s = self.snapshot('pax')
        s['reads'][:] = 0
        s['reads'][:4] = 100
        s['total'] = 400
        policy = Pax(self.models, [400.] * 288)
        plan = policy.decide(s)
        validate(plan, self.config, replica=True)
        self.assertIn(1, plan.active)
        self.assertEqual(plan.units, s['units'])

    def test_slicer_keeps_node_count_and_split_owner(self):
        s = self.snapshot('slicer', hot=True)
        plan = Slicer(self.models, []).decide(s)
        validate(plan, self.config)
        self.assertEqual(plan.active, s['active'])
        self.assertGreater(len(plan.units), len(s['units']))
        old = {k: u.holders for u in s['units'] for k in u.keys}
        self.assertTrue(all(old[k] == u.holders for u in plan.units for k in u.keys))

    def test_node_not_ready_before_copy_and_no_overlapping_actions(self):
        c = self.config
        cluster = Cluster(c, 'elastman')
        units = list(cluster.units)
        units[0] = Unit(units[0].keys, (4,))
        plan = Plan((0, 1, 2, 3, 4), tuple(units), 'test', {})
        cluster.accept(plan, 1)
        cluster.advance(2)
        self.assertNotIn(4, cluster.active)
        with self.assertRaises(AssertionError):
            cluster.accept(plan, 2)
        cluster.advance(3)
        self.assertIn(4, cluster.active)
        self.assertEqual(cluster.transferred, 4 * c.key_bytes)

    def test_scale_in_waits_for_inflight_requests_to_drain(self):
        c = self.config
        cluster = Cluster(c, 'elastman')
        cluster.free[3] = 8.
        units = tuple(Unit(u.keys, (0,) if u.holders == (3,) else u.holders) for u in cluster.units)
        cluster.accept(Plan((0, 1, 2), units, 'drain', {}), 1)
        cluster.advance(3)
        self.assertNotIn(3, cluster.active)
        self.assertIn(3, cluster.pending['old'])
        self.assertIsNone(cluster.actions[0]['completed_window'])
        cluster.advance(8)
        self.assertNotIn(3, cluster.active)
        self.assertIsNone(cluster.pending)

    def test_drain_redirects_new_requests_before_node_termination(self):
        c = self.config
        cluster = Cluster(c, 'elastman')
        cluster.free[3] = 8.
        units = tuple(Unit(u.keys, (0,) if u.holders == (3,) else u.holders) for u in cluster.units)
        cluster.accept(Plan((0, 1, 2), units, 'drain', {}), 1)
        cluster.advance(3)
        observation = cluster.observe(3, [(3.1, 12, False)])
        self.assertEqual(cluster.free[3], 8.)
        self.assertGreater(cluster.free[0], 3.1)
        self.assertEqual(cluster.rows[-1]['ready_nodes'], 3)
        self.assertEqual(cluster.rows[-1]['allocated_nodes'], 4)
        cluster.advance(8)
        self.assertIsNone(cluster.pending)

    def test_all_eight_execute_same_external_trace_deterministically(self):
        hashes = set()
        for method in METHODS:
            one = execute(method, 'hot_range', 1, self.config, self.models)
            two = execute(method, 'hot_range', 1, self.config, self.models)
            self.assertEqual(one, two, method)
            hashes.add(one['workload_hash'])
            self.assertEqual(one['metrics']['requests'], one['metrics']['completed'] + one['metrics']['outstanding'])
        self.assertEqual(len(hashes), 1)

    def test_original_validation_rejects_tamper_and_missing_matrix(self):
        c, _ = configuration('quick')
        events, past = trace('spike', 1, c), history('spike', 1, c)
        record = execute('elastman', 'spike', 1, c, self.models, events, past)
        record.update(resultSchemaVersion=SCHEMA, profile='quick', generator=GENERATOR,
                      timeDomain='virtual', dataPlane='modeled-transfer-no-KV',
                      implementationScope='paper-derived-mechanism-prototype-not-original-system-reproduction')
        record['evidence_hash'] = digest(record)
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / 'inputs').mkdir()
            (folder / 'results').mkdir()
            write_json(folder / 'inputs/spike-1.json', dict(generator=GENERATOR, events=events, history=past))
            self.assertEqual(validate_record(record, 'quick', folder / 'inputs'), record)
            changed = copy.deepcopy(record)
            changed['metrics']['node_windows'] += 1
            with self.assertRaises(AssertionError):
                validate_record(changed, 'quick', folder / 'inputs')
            changed['evidence_hash'] = digest({k: v for k, v in changed.items() if k != 'evidence_hash'})
            with self.assertRaises(AssertionError):
                validate_record(changed, 'quick', folder / 'inputs')
            with self.assertRaises(AssertionError):
                rebuild(folder, 'quick')


if __name__ == '__main__':
    with threadpool_limits(limits=1):
        unittest.main(verbosity=2)
