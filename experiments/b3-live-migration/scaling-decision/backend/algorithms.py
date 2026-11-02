"""논문별 핵심 결정 알고리즘. 외부 시스템 의존 기능은 README의 변경점 표를 따른다."""
from __future__ import annotations

import math
from functools import lru_cache
import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp, least_squares
from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans

from cluster import Unit, Plan, balanced, loads, unit_work, moved_bytes, queue_sample, digest


class Models:
    def __init__(self, config):
        self.config = config
        rows = []
        for ratio in (0., .1, .5, .9, 1.):
            for rate in range(20, 121, 10):
                for seed in range(3):
                    p99, throughput = queue_sample(rate, ratio, 9000 + seed, config)
                    rows.append([rate * (1 - ratio), rate * ratio, p99, throughput])
        self.rows = rows
        self.classifier = LogisticRegression(C=1000., random_state=0, max_iter=1000)
        x = np.array([r[:2] for r in rows]) / 100
        y = np.array([r[2] > config.slo for r in rows], dtype=int)
        self.classifier.fit(x, y)
        self.training_hash = digest(rows)
        self.capacity = self.limit(config.write_ratio)
        self.reward_data = []
        # 별도 uniform 부하 sweep. 평가 workload나 미래 관측을 사용하지 않는다.
        for nodes in range(config.min_nodes, config.max_nodes + 1):
            for total in range(40, 801, 40):
                for seed in range(3):
                    p99, throughput = queue_sample(total / nodes, config.write_ratio, 12000 + seed, config)
                    self.reward_data.append([nodes, float(total), p99, throughput * nodes])
        self.reward_hash = digest(self.reward_data)

    def limit(self, ratio):
        coef = self.classifier.coef_[0]
        denominator = coef[0] * (1 - ratio) + coef[1] * ratio
        return float(np.clip(-100 * self.classifier.intercept_[0] / max(1e-6, denominator), 15, 100))

    def fits(self, reads, writes):
        return bool(self.classifier.predict(np.array([[reads, writes]]) / 100)[0] == 0)

    def record(self):
        return dict(calibration_hash=self.training_hash, reward_training_hash=self.reward_hash,
                    calibration_rows=len(self.rows), reward_rows=len(self.reward_data),
                    logistic_coefficients=self.classifier.coef_[0].tolist(),
                    logistic_intercept=float(self.classifier.intercept_[0]), capacity=self.capacity)


def single_count(snapshot, count, reason, detail=None):
    c = snapshot['config']
    count = max(c.min_nodes, min(c.max_nodes, count))
    active = tuple(range(count))
    if active == snapshot['active']:
        return Plan(active, snapshot['units'], reason, detail or {})
    units = balanced(snapshot['units'], active, snapshot['reads'], snapshot['writes'], c)
    return Plan(active, units, reason, detail or {})


class Scads:
    """FAST'11 Algorithm 1: hottest-bin, best-fit, replica, consolidation."""
    def __init__(self, models, past):
        self.models = models

    def decide(self, s):
        c, r, w = s['config'], s['reads'], s['writes']
        units, active = list(s['units']), list(s['active'])
        original = list(units)
        def rates(node, items):
            reads = writes = 0.
            for u in items:
                if node in u.holders:
                    reads += sum(r[k] for k in u.keys) / len(u.holders)
                    writes += sum(w[k] for k in u.keys)
            return reads, writes
        def fits(node, items):
            return self.models.fits(*rates(node, items))
        replications = 0
        for _ in range(64):
            bad = [n for n in active if not fits(n, units)]
            if not bad:
                break
            source = max(bad, key=lambda n: (sum(rates(n, units)), -n))
            candidates = sorted((u for u in units if source in u.holders),
                                key=lambda u: (-unit_work(u, r, w, config=c), u.keys))
            if not candidates:
                break
            u = candidates[0]
            others = [n for n in active if n not in u.holders]
            replacement = None
            if not self.models.fits(sum(r[k] for k in u.keys), sum(w[k] for k in u.keys)):
                # 원문의 큰 bin 경로: 감당 가능한 수의 빈 서버에 전용 복제본을 배치한다.
                required = next((number for number in range(2, c.max_nodes + 1)
                                 if self.models.fits(sum(r[k] for k in u.keys) / number,
                                                     sum(w[k] for k in u.keys))), None)
                empty = [n for n in active if not any(n in item.holders and item != u for item in units)]
                unused = sorted(set(range(c.max_nodes)) - set(active))
                if required is not None and len(empty) + len(unused) >= required:
                    destinations = (empty + unused)[:required]
                    active.extend(n for n in destinations if n not in active)
                    replacement = Unit(u.keys, tuple(sorted(destinations)))
                    replications += 1
            else:
                for n in sorted(others, key=lambda n: (-sum(rates(n, units)), n)):
                    moved = Unit(u.keys, (n,))
                    trial = [moved if item == u else item for item in units]
                    if fits(n, trial):
                        replacement = moved
                        break
                if replacement is None and len(active) < c.max_nodes:
                    n = min(set(range(c.max_nodes)) - set(active))
                    active.append(n)
                    replacement = Unit(u.keys, (n,))
            if replacement is None:
                break
            units[units.index(u)] = replacement
        # 과부하가 해소된 경우에만 저부하 서버를 완전히 비워 통합한다.
        if all(fits(n, units) for n in active):
            for source in sorted(active, key=lambda n: (sum(rates(n, units)), n)):
                if len(active) <= c.min_nodes:
                    break
                trial = list(units)
                okay = True
                for u in list(trial):
                    if source not in u.holders:
                        continue
                    if len(u.holders) > 1:
                        moved = Unit(u.keys, tuple(n for n in u.holders if n != source))
                        updated = [moved if item == u else item for item in trial]
                        if all(fits(n, updated) for n in moved.holders):
                            trial = updated
                            continue
                        okay = False
                        break
                    target = None
                    for n in sorted((n for n in active if n != source), key=lambda n: (-sum(rates(n, trial)), n)):
                        moved = Unit(u.keys, (n,))
                        updated = [moved if item == u else item for item in trial]
                        if fits(n, updated):
                            target, trial = n, updated
                            break
                    if target is None:
                        okay = False
                        break
                if okay:
                    units = trial
                    active.remove(source)
        return Plan(tuple(sorted(active)), tuple(sorted(units, key=lambda u: u.keys)), 'hottest-bin-best-fit',
                    dict(replication_steps=replications, bins=len(original)))


class Elastman:
    """CAC'13 식 (2)의 PI와 feedforward classifier. 이득은 모델 설정이다."""
    def __init__(self, models, past):
        self.models = models
        self.previous_total = None
        self.u = models.capacity
        self.integral = 0.
        self.filtered = None

    def decide(self, s):
        c = s['config']
        self.filtered = s['p99'] if self.filtered is None else .5 * self.filtered + .5 * s['p99']
        error = (c.slo - self.filtered) / c.slo
        spike = self.previous_total is None or abs(s['total'] - self.previous_total) > max(1, self.previous_total) * .20
        self.previous_total = s['total']
        if spike:
            ratio = sum(s['writes']) / max(1, s['total'])
            self.u = self.models.limit(ratio) * .95
            mode = 'feedforward-logistic'
        elif abs(error) <= .10:
            return Plan(s['active'], s['units'], 'PI-deadband', dict(filtered_p99=self.filtered))
        else:
            self.integral = float(np.clip(self.integral + error, -5, 5))
            self.u = float(np.clip(self.u + 8 * error + self.integral, 20, 95))
            mode = 'feedback-PI'
        return single_count(s, math.ceil(s['total'] / self.u), mode,
                            dict(target_throughput=self.u, filtered_p99=self.filtered, integral=self.integral))


def estore_units(s):
    """hot key 승격·cold key 강등. 작은 데이터셋이므로 top-k=4로 설정한다."""
    r, w = s['reads'], s['writes']
    hot = set(sorted(range(len(r)), key=lambda k: (-r[k] - w[k], k))[:4])
    owner = {k: u.holders[0] for u in s['units'] for k in u.keys}
    units = []
    for start in range(0, len(r), 4):
        cold = []
        for k in range(start, start + 4):
            if k in hot:
                if cold:
                    units.append(Unit(tuple(cold), (owner[cold[0]],)))
                    cold = []
                units.append(Unit((k,), (owner[k],)))
            elif cold and owner[k] != owner[cold[-1]]:
                units.append(Unit(tuple(cold), (owner[cold[0]],)))
                cold = [k]
            else:
                cold.append(k)
        if cold:
            units.append(Unit(tuple(cold), (owner[cold[0]],)))
    return units, hot


class Estore:
    def __init__(self, models, past):
        self.cpu_history = []

    def decide(self, s):
        c = s['config']
        self.cpu_history.append(dict(s['cpu']))
        average = float(np.mean(list(s['cpu'].values())))
        active = list(s['active'])
        if average > c.high and len(active) < c.max_nodes:
            active.append(min(set(range(c.max_nodes)) - set(active)))
        elif average < c.low and len(active) > c.min_nodes:
            remove = min(active, key=lambda n: (s['cpu'][n], n))
            active.remove(remove)
        if active == list(s['active']) and all(c.low <= value <= c.high for value in s['cpu'].values()):
            return Plan(s['active'], s['units'], 'CPU-watermarks-stable', {})
        units, hot = estore_units(s)
        r, w = s['reads'], s['writes']
        units = [Unit(u.keys, (u.holders[0] if u.holders[0] in active else min(active),)) for u in units]
        average_work = sum(r) * c.read_service + sum(w) * c.write_service
        average_work /= len(active)
        epsilon = max(.03, average_work * .10)
        moves = 0
        # Greedy hot 단계 후 Greedy Extended cold 단계.
        for hot_stage in (True, False):
            order = sorted(range(len(units)), key=lambda i: (-unit_work(units[i], r, w, config=c), units[i].keys))
            for i in order:
                u = units[i]
                if (len(u.keys) == 1 and u.keys[0] in hot) != hot_stage:
                    continue
                current = loads(units, active, r, w, c)
                source = u.holders[0]
                target = min(active, key=lambda n: (current[n], n))
                if current[source] > average_work + epsilon and current[target] < average_work - epsilon:
                    weight = unit_work(u, r, w, config=c)
                    if current[target] + weight < current[source]:
                        units[i] = Unit(u.keys, (target,))
                        moves += 1
        return Plan(tuple(sorted(active)), tuple(sorted(units, key=lambda u: u.keys)), 'Greedy-Extended',
                    dict(hot_keys=sorted(hot), local_moves=moves, cpu_mean=average, top_k=4))


def solve_placement(units, active, reads, writes, config, capacity):
    """Accordion의 고정 서버 수 MILP. 목적은 이동 byte, 제약은 부하·메모리·단일 owner."""
    m, n = len(units), len(active)
    objective = np.zeros(m * n)
    matrix, lower, upper = [], [], []
    for i, u in enumerate(units):
        row = np.zeros(m * n)
        row[i * n:(i + 1) * n] = 1
        matrix.append(row); lower.append(1); upper.append(1)
        for j, node in enumerate(active):
            objective[i * n + j] = (0 if node in u.holders else len(u.keys) * config.key_bytes) + 1e-5 * (i + 1) * (j + 1)
    for j in range(n):
        for memory in (False, True):
            row = np.zeros(m * n)
            for i, u in enumerate(units):
                row[i * n + j] = len(u.keys) * config.key_bytes if memory else unit_work(u, reads, writes, config=config)
            matrix.append(row); lower.append(0); upper.append(config.node_bytes if memory else capacity)
    result = milp(objective, integrality=np.ones(m * n), bounds=Bounds(0, 1),
                  constraints=LinearConstraint(np.array(matrix), lower, upper), options=dict(time_limit=2., mip_rel_gap=0.))
    if result.status != 0:
        return None, int(result.status)
    assignment = result.x.reshape((m, n)).argmax(axis=1)
    placed = tuple(Unit(u.keys, (active[int(assignment[i])],)) for i, u in enumerate(units))
    return placed, 0


class Accordion:
    def __init__(self, models, past):
        self.previous = None
        self.models = models

    def decide(self, s):
        c, total = s['config'], s['total']
        current = len(s['active'])
        decreasing = self.previous is not None and total < self.previous
        self.previous = total
        candidates = list(range(current - 1, c.min_nodes - 1, -1)) + [current] if decreasing else list(range(current, c.max_nodes + 1))
        found, statuses = [], []
        ratio = sum(s['writes']) / max(1, total)
        capacity = self.models.limit(ratio) * ((1 - ratio) * c.read_service + ratio * c.write_service)
        for count in candidates:
            active = tuple(range(count))
            units, status = solve_placement(s['units'], active, s['reads'], s['writes'], c, capacity)
            statuses.append(dict(nodes=count, solver_status=status))
            if units is not None:
                found.append((count, units))
                if not decreasing:
                    break
        if not found:
            return Plan(s['active'], s['units'], 'MILP-no-optimal-feasible-plan', dict(solver=statuses))
        count, units = min(found, key=lambda pair: pair[0])
        return Plan(tuple(range(count)), units, 'server-count-MILP', dict(solver=statuses, capacity=capacity, affinity='null', replicas=1))


class Spar:
    """P-Store 식 (8)의 작은 SPAR: n=2 periods, m=3 recent offsets, horizon별 LS."""
    def __init__(self, past, config):
        self.period = config.period
        self.horizon = config.forecast_horizon
        self.weights = {}
        for horizon in range(1, self.horizon + 1):
            x, y = [], []
            for t in range(2 * self.period + 4, len(past) - horizon):
                x.append(self.features(past[:t + 1], horizon))
                y.append(past[t + horizon])
            self.weights[horizon] = np.linalg.lstsq(np.array(x), np.array(y), rcond=None)[0]
        self.training_hash = digest(past)

    def features(self, history, horizon):
        t, period = len(history) - 1, self.period
        seasonal = [history[t + horizon - k * period] for k in (1, 2)]
        offsets = [history[t - j] - np.mean([history[t - j - k * period] for k in (1, 2)]) for j in range(3)]
        return seasonal + offsets

    def predict(self, history):
        return [max(0., float(np.dot(self.weights[h], self.features(history, h)))) * 1.10
                for h in range(1, self.horizon + 1)]


def move_duration(before, after, config):
    if before == after:
        return config.control_windows
    moved = config.keys * config.key_bytes * abs(after - before) / max(before, after)
    duration = math.ceil(moved / config.transfer_bytes) + (config.provision_windows if after > before else 0)
    return max(config.control_windows, math.ceil(duration / config.control_windows) * config.control_windows)


def dynamic_schedule(predictions, initial, capacity, config):
    horizon = len(predictions)
    @lru_cache(None)
    def best(t, before):
        if t >= horizon:
            return 0., ()
        answer = (float('inf'), ())
        for after in range(config.min_nodes, config.max_nodes + 1):
            duration = move_duration(before, after, config)
            if t + duration > horizon:
                continue
            effective = min(before, after) * capacity * (1 - config.migration_penalty if before != after else 1)
            if any(value > effective for value in predictions[t:t + duration - 1]) or predictions[t + duration - 1] > after * capacity:
                continue
            tail_cost, tail = best(t + duration, after)
            cost = max(before, after) * duration + tail_cost
            if cost < answer[0] - 1e-9:
                answer = (cost, ((t, before, after, duration),) + tail)
        return answer
    return best(0, initial)


class Pstore:
    def __init__(self, models, past):
        self.models = models
        self.history = list(past)
        self.spar = Spar(past, models.config)

    def observe(self, total):
        self.history.append(float(total))

    def decide(self, s):
        forecasts = self.spar.predict(self.history)
        cost, schedule = dynamic_schedule(forecasts, len(s['active']), self.models.capacity * .90, s['config'])
        if not schedule:
            return single_count(s, math.ceil(s['total'] / (self.models.capacity * .90)), 'prediction-infeasible-reactive-fallback',
                                dict(prediction=forecasts, training_hash=self.spar.training_hash))
        return single_count(s, schedule[0][2], 'SPAR-dynamic-program',
                            dict(prediction=forecasts, schedule=schedule, node_window_cost=cost, training_hash=self.spar.training_hash))


class Tiramola:
    """CCGrid'13: VM 수 상태, load-slice 대표 관측, Eq(1) Q 업데이트."""
    def __init__(self, models, past):
        self.models = models
        self.data = list(models.reward_data)
        self.q = np.zeros((9, 3))
        self.updates = 0

    def reward(self, nodes, total, slo):
        candidates = np.array([row[1:] for row in self.data if row[0] == nodes and abs(row[1] - total) <= max(20, total * .20)])
        if len(candidates) == 0:
            nearest = sorted((row for row in self.data if row[0] == nodes), key=lambda row: abs(row[1] - total))[:3]
            candidates = np.array([row[1:] for row in nearest])
        # 원문은 k>1일 때 가장 큰 cluster의 centroid를 보상 계산에 사용한다.
        if len(candidates) >= 4:
            normalized = candidates / np.maximum(np.std(candidates, axis=0), 1e-6)
            labels = KMeans(n_clusters=2, n_init=1, random_state=0).fit_predict(normalized)
            largest = int(np.argmax(np.bincount(labels)))
            point = candidates[labels == largest].mean(axis=0)
        else:
            point = candidates.mean(axis=0)
        _, latency, throughput = point
        return float(throughput / 100 - .30 * nodes - 25 * max(0., latency / slo - 1))

    def decide(self, s):
        c, nodes = s['config'], len(s['active'])
        self.data.append([nodes, float(s['total']), float(s['p99']), float(s['completed'])])
        rewards = {n: self.reward(n, s['total'], c.slo) for n in range(c.min_nodes, c.max_nodes + 1)}
        # 결정론적 전이에서 학습된 대표 보상으로 Bellman/Q backup을 수행한다.
        # 실제 VM 전이의 탐색 비용·학습 지연은 이 알고리즘 수준 실험에 포함하지 않는다.
        for _ in range(20):
            previous = self.q.copy()
            for n in rewards:
                for index, delta in enumerate((-1, 0, 1)):
                    destination = n + delta
                    if destination not in rewards:
                        self.q[n, index] = -1e6
                        continue
                    target = rewards[destination] - .05 * abs(delta) + .8 * max(previous[destination])
                    self.q[n, index] += .30 * (target - self.q[n, index])
                    self.updates += 1
        action = max(range(3), key=lambda i: (self.q[nodes, i], i == 1, -i))
        return single_count(s, nodes + action - 1, 'load-slice-Q-backup',
                            dict(q_values=self.q[nodes].tolist(), rewards=rewards, updates=self.updates,
                                 alpha=.30, gamma=.8, transition_penalty=.05))


def arima_forecast(history, steps):
    """ARIMA(1,1,1)의 conditional least-squares 적합. 미래 관측은 받지 않는다."""
    delta = np.diff(np.asarray(history[-64:], dtype=float))
    def residual(parameters):
        phi, theta = parameters
        errors = np.zeros(len(delta))
        for i in range(1, len(delta)):
            errors[i] = delta[i] - phi * delta[i - 1] - theta * errors[i - 1]
        return errors[1:]
    result = least_squares(residual, [0., 0.], bounds=([- .98, -.98], [.98, .98]), max_nfev=60)
    phi, theta = result.x
    error = residual(result.x)[-1]
    change = phi * delta[-1] + theta * error
    value = history[-1]
    for _ in range(steps):
        value += change
        change *= phi
    return max(0., float(value)), dict(phi=float(phi), theta=float(theta), order=[1, 1, 1])


class Pax:
    def __init__(self, models, past):
        self.history = list(past)

    def observe(self, total):
        self.history.append(float(total))

    def decide(self, s):
        c = s['config']
        prediction, model = arima_forecast(self.history, c.provision_windows)
        active = list(s['active'])
        scores = {n: sum(sum(s['reads'][k] + s['writes'][k] for k in u.keys) for u in s['units'] if n in u.holders)
                  for n in range(c.max_nodes)}
        def cpu(nodes):
            work = loads(s['units'], nodes, s['reads'], s['writes'], c)
            return sum(work.values()) / len(nodes) * prediction / max(1, s['total'])
        def valid(nodes):
            return all(any(n in nodes for n in u.holders) for u in s['units'])
        start_cpu = cpu(active)
        if start_cpu > c.high:
            for node in sorted(set(range(c.max_nodes)) - set(active), key=lambda n: (-scores[n], n)):
                active.append(node)
                if cpu(active) <= (c.low + c.high) / 2:
                    break
        elif start_cpu < c.low:
            for node in sorted(active, key=lambda n: (scores[n], n)):
                candidate = [n for n in active if n != node]
                if len(candidate) >= c.min_nodes and valid(candidate) and cpu(candidate) <= (c.low + c.high) / 2:
                    active = candidate
        return Plan(tuple(sorted(active)), s['units'], 'partition-score-ARIMA-average',
                    dict(prediction=prediction, model=model, scores=scores, predicted_cpu=start_cpu, consistency_level=1))


class Slicer:
    """OSDI'16 §4.4: benefit/key-churn greedy, cold merge, hot split, 고정 task 수."""
    def __init__(self, models, past):
        pass

    def decide(self, s):
        c, r, w = s['config'], s['reads'], s['writes']
        units = list(s['units'])
        active = s['active']
        mean_slice = sum(unit_work(u, r, w, config=c) for u in units) / len(units)
        # 같은 owner의 인접 cold slice만 merge한다.
        merged = []
        for u in sorted(units, key=lambda u: u.keys):
            if merged and merged[-1].holders == u.holders and merged[-1].keys[-1] + 1 == u.keys[0]:
                combined = Unit(merged[-1].keys + u.keys, u.holders)
                if unit_work(combined, r, w, config=c) < mean_slice / 2:
                    merged[-1] = combined
                    continue
            merged.append(u)
        units = merged
        used = 0
        budget = max(1, math.floor(.09 * c.keys))
        while used < budget:
            current = loads(units, active, r, w, c)
            hottest = max(active, key=lambda n: (current[n], -n))
            coldest = min(active, key=lambda n: (current[n], n))
            best = None
            for i, u in enumerate(units):
                if u.holders != (hottest,) or used + len(u.keys) > budget:
                    continue
                weight = unit_work(u, r, w, config=c)
                updated = dict(current)
                updated[hottest] -= weight
                updated[coldest] += weight
                benefit = max(current.values()) - max(updated.values())
                ratio = benefit / len(u.keys)
                if ratio > 1e-9 and (best is None or ratio > best[0]):
                    best = ratio, i, coldest
            if best is None:
                break
            _, i, target = best
            u = units[i]
            units[i] = Unit(u.keys, (target,))
            used += len(u.keys)
        # 분할은 현 assignment를 유지하며 다음 결정의 선택지만 늘린다.
        split = []
        mean_slice = sum(unit_work(u, r, w, config=c) for u in units) / len(units)
        for u in units:
            if len(u.keys) > 1 and unit_work(u, r, w, config=c) >= 2 * mean_slice:
                middle = len(u.keys) // 2
                split.extend((Unit(u.keys[:middle], u.holders), Unit(u.keys[middle:], u.holders)))
            else:
                split.append(u)
        return Plan(active, tuple(sorted(split, key=lambda u: u.keys)), 'weighted-move-cold-merge-hot-split',
                    dict(churn_budget_keys=budget, moved_keys=used, redundancy=1))


FACTORIES = dict(scads=Scads, elastman=Elastman, estore=Estore, accordion=Accordion,
                 pstore=Pstore, tiramola=Tiramola, pax=Pax, slicer=Slicer)
