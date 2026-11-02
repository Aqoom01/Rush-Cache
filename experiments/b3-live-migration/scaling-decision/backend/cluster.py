"""선행 알고리즘 비교용 가상 클러스터. 실제 KV/네트워크 구현과 독립적이다."""
from __future__ import annotations

from dataclasses import dataclass, asdict
import hashlib
import heapq
import json
import math
import numpy as np

METHODS = ('scads', 'elastman', 'estore', 'accordion', 'pstore', 'tiramola', 'pax', 'slicer')
SCENARIOS = ('step', 'spike', 'periodic', 'low', 'hot_range', 'moving_hotspot', 'hot_key')
CONTROLS = ('fixed_single', 'fixed_replica', 'fixed_slices')


@dataclass(frozen=True)
class Config:
    windows: int = 96
    keys: int = 32
    initial_nodes: int = 4
    min_nodes: int = 2
    max_nodes: int = 8
    key_bytes: int = 256
    node_bytes: int = 8192
    transfer_bytes: int = 2048
    provision_windows: int = 2
    cooldown_windows: int = 2
    control_windows: int = 4
    read_service: float = .01
    write_service: float = .02
    slo: float = .12
    low: float = .35
    high: float = .75
    target: float = .60
    migration_penalty: float = .15
    period: int = 48
    training_periods: int = 6
    forecast_horizon: int = 24
    write_ratio: float = .10


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def demand(scenario, t, config):
    period = config.period
    if scenario == 'periodic':
        return 230 + 140 * math.sin(2 * math.pi * t / period)
    if scenario == 'step':
        return 180 if t < config.windows // 4 else 420 if t < 3 * config.windows // 4 else 85
    if scenario == 'spike':
        return 600 if config.windows // 3 <= t < config.windows // 3 + 2 else 180
    if scenario == 'low':
        return 180 if t < config.windows // 4 else 65
    return 210


def trace(scenario, seed, config):
    """외부 요청열은 방법과 배치에 의존하지 않는다. 과거 학습 구간은 별도 생성한다."""
    rng = np.random.default_rng(seed)
    events = []
    for t in range(config.windows):
        probabilities = np.ones(config.keys, dtype=float)
        if scenario in ('hot_range', 'moving_hotspot', 'hot_key'):
            probabilities[:] = .30 / (config.keys - (1 if scenario == 'hot_key' else 4))
            start = 0 if scenario != 'moving_hotspot' else (t // 24 * 8) % config.keys
            hot = [0] if scenario == 'hot_key' else [(start + i) % config.keys for i in range(4)]
            probabilities[hot] = .70 / len(hot)
        probabilities /= probabilities.sum()
        count = int(rng.poisson(demand(scenario, t, config)))
        keys = rng.choice(config.keys, count, p=probabilities)
        arrivals = np.sort(rng.uniform(t, t + 1, count))
        writes = rng.random(count) < config.write_ratio
        events.append([(round(float(a), 9), int(k), bool(w)) for a, k, w in zip(arrivals, keys, writes)])
    return events


def history(scenario, seed, config):
    rng = np.random.default_rng(100_000 + seed)
    return [float(rng.poisson(demand(scenario, t, config)))
            for t in range(-config.period * config.training_periods, 0)]


@dataclass(frozen=True)
class Unit:
    keys: tuple[int, ...]
    holders: tuple[int, ...]


@dataclass
class Plan:
    active: tuple[int, ...]
    units: tuple[Unit, ...]
    reason: str
    detail: dict


def unit_work(unit, reads, writes, active=None, config=Config()):
    holders = unit.holders if active is None else tuple(n for n in unit.holders if n in active)
    if not holders:
        raise ValueError('no active replica')
    r = float(sum(reads[k] for k in unit.keys)) / len(holders)
    w = float(sum(writes[k] for k in unit.keys))
    return r * config.read_service + w * config.write_service


def loads(units, active, reads, writes, config):
    result = {n: 0. for n in active}
    for unit in units:
        for n in unit.holders:
            if n in result:
                result[n] += unit_work(unit, reads, writes, active, config)
    return result


def moved_bytes(old, new, config):
    previous = {k: set(u.holders) for u in old for k in u.keys}
    return sum(len(set(u.holders) - previous[k]) * config.key_bytes for u in new for k in u.keys)


def churn(old, new):
    previous = {k: u.holders for u in old for k in u.keys}
    return sum(previous[k] != u.holders for u in new for k in u.keys)


def validate(plan, config, replica=False):
    assert config.min_nodes <= len(plan.active) <= config.max_nodes
    assert len(plan.active) == len(set(plan.active))
    assert all(0 <= n < config.max_nodes for n in plan.active)
    keys = [k for u in plan.units for k in u.keys]
    assert sorted(keys) == list(range(config.keys)), 'missing/duplicate key range'
    memory = {n: 0 for n in range(config.max_nodes)}
    for unit in plan.units:
        assert unit.keys and tuple(sorted(unit.keys)) == unit.keys
        assert unit.holders and len(set(unit.holders)) == len(unit.holders)
        assert any(n in plan.active for n in unit.holders), 'unavailable range'
        assert all(0 <= n < config.max_nodes for n in unit.holders)
        if not replica:
            assert all(n in plan.active for n in unit.holders), 'owner on removed node'
        for n in unit.holders:
            memory[n] += len(unit.keys) * config.key_bytes
    assert max(memory.values()) <= config.node_bytes, 'memory capacity'


class Cluster:
    def __init__(self, config, method):
        self.config = config
        self.replica = method in ('pax', 'fixed_replica')
        self.active = (0, 2, 4, 6) if self.replica else tuple(range(config.initial_nodes))
        self.units = tuple(Unit(tuple(range(i, i + 4)),
                                ((i // 4, (i // 4 + 1) % 8, (i // 4 + 2) % 8) if self.replica else ((i // 4) % 4,)))
                           for i in range(0, config.keys, 4))
        self.free = [0.] * config.max_nodes
        self.completions = []
        self.pending = None
        self.last_complete = -100
        self.hints = [0] * config.max_nodes
        self.rows = []
        self.actions = []
        self.last_p99 = 0.
        self.accepted = self.finished = self.violations = 0
        self.transferred = self.planned = self.reversals = 0
        self.splits = self.merges = self.last_direction = 0
        validate(Plan(self.active, self.units, 'initial', {}), config, self.replica)

    def advance(self, t):
        if self.pending is None:
            return
        p = self.pending
        if t < p['ready']:
            return
        if 'cutover' in p:
            remaining = 0
        elif self.replica:
            for n in p['new']:
                sent = min(self.hints[n], self.config.transfer_bytes)
                self.hints[n] -= sent
                self.transferred += sent
            remaining = sum(self.hints[n] for n in p['new'])
        else:
            sent = min(p['remaining'], self.config.transfer_bytes)
            p['remaining'] -= sent
            self.transferred += sent
            remaining = p['remaining']
        if remaining:
            return
        if 'cutover' not in p:
            old_count = len(self.units)
            self.active, self.units = p['plan'].active, p['plan'].units
            self.splits += max(0, len(self.units) - old_count)
            self.merges += max(0, old_count - len(self.units))
            p['cutover'] = t
            p['event']['cutover_window'] = t
            validate(p['plan'], self.config, self.replica)
        # 새 요청은 이미 새 assignment로 보낸다. 제거 노드는 기존 요청만 drain한다.
        removed = p['old'] - set(p['plan'].active)
        if any(self.free[n] > t + 1e-9 for n in removed):
            return
        p['event']['completed_window'] = t
        p['event']['delay_windows'] = t - p['start']
        self.last_complete = t
        self.pending = None

    def observe(self, t, events):
        config = self.config
        key_units = {k: u for u in self.units for k in u.keys}
        reads = np.zeros(config.keys)
        writes = np.zeros(config.keys)
        offered = {n: 0. for n in self.active}
        initial_backlog = {n: max(0., self.free[n] - t) for n in self.active}
        # 이동이 진행 중이면 서비스에도 고정 비율의 간섭을 준다(실측 값이 아님).
        penalty = config.migration_penalty if self.pending and 'cutover' not in self.pending and t >= self.pending['ready'] else 0.
        late = 0
        for arrival, key, write in events:
            (writes if write else reads)[key] += 1
            unit = key_units[key]
            available = [n for n in unit.holders if n in self.active]
            assert available
            if write:
                destinations = available
                service = config.write_service / (1 - penalty)
                if self.replica:
                    for n in unit.holders:
                        if n not in self.active:
                            self.hints[n] += config.key_bytes
            else:
                destinations = [min(available, key=lambda n: (self.free[n], n))]
                service = config.read_service / (1 - penalty)
            finishes = []
            for n in destinations:
                self.free[n] = max(arrival, self.free[n]) + service
                offered[n] += service
                finishes.append(self.free[n])
            finish = min(finishes)  # 복제 모형의 응답 일관성은 CL=1.
            heapq.heappush(self.completions, (finish, arrival, not write))
            late += finish - arrival > config.slo
        response_latencies, read_latencies = [], []
        while self.completions and self.completions[0][0] <= t + 1:
            end, start, read = heapq.heappop(self.completions)
            response_latencies.append(end - start)
            if read:
                read_latencies.append(end - start)
        if read_latencies:
            self.last_p99 = float(np.quantile(read_latencies, .99))
        self.accepted += len(events)
        self.finished += len(response_latencies)
        self.violations += late
        cpu = {n: min(1., initial_backlog[n] + offered[n]) for n in self.active}
        allocated = len((self.pending['old'] | set(self.pending['plan'].active)) if self.pending else set(self.active))
        row = dict(window=t, requests=len(events), completed=len(response_latencies),
                   deadline_misses=int(late), read_p99=self.last_p99,
                   p99_read_samples=len(read_latencies), ready_nodes=len(self.active), allocated_nodes=allocated,
                   active=list(self.active), unit_count=len(self.units), cpu_mean=float(np.mean(list(cpu.values()))),
                   max_offered_work=max(offered.values()), excess_work=sum(max(0., v - config.target) for v in offered.values()),
                   transferred_bytes=self.transferred, pending=self.pending is not None)
        self.rows.append(row)
        return dict(t=t, active=self.active, units=self.units, reads=reads, writes=writes,
                    cpu=cpu, p99=self.last_p99, total=len(events), completed=len(response_latencies),
                    hints=tuple(self.hints), config=config)

    def accept(self, plan, t):
        if self.pending is not None:
            raise AssertionError('overlapping action')
        validate(plan, self.config, self.replica)
        if plan.active == self.active and plan.units == self.units:
            return
        direction = int(len(plan.active) > len(self.active)) - int(len(plan.active) < len(self.active))
        if direction:
            self.reversals += bool(self.last_direction and direction != self.last_direction)
            self.last_direction = direction
        data = moved_bytes(self.units, plan.units, self.config) if not self.replica else sum(self.hints[n] for n in set(plan.active) - set(self.active))
        event = dict(start_window=t, old_nodes=list(self.active), new_nodes=list(plan.active),
                     action='out' if direction > 0 else 'in' if direction < 0 else 'rebalance',
                     reason=plan.reason, planned_bytes=data, changed_keys=churn(self.units, plan.units),
                     old_units=len(self.units), new_units=len(plan.units), detail=plan.detail,
                     completed_window=None,
                     assignments=[dict(keys=list(u.keys), holders=list(u.holders)) for u in plan.units])
        self.actions.append(event)
        self.planned += data
        new = set(plan.active) - set(self.active)
        self.pending = dict(plan=plan, old=set(self.active), remaining=data, ready=t + (self.config.provision_windows if new else 0),
                            start=t, new=new, event=event)

    def metrics(self):
        return dict(requests=self.accepted, completed=self.finished, outstanding=len(self.completions),
                    deadline_miss_rate=self.violations / max(1, self.accepted),
                    node_windows=sum(r['allocated_nodes'] for r in self.rows),
                    mean_ready_nodes=float(np.mean([r['ready_nodes'] for r in self.rows])),
                    excess_work_area=sum(r['excess_work'] for r in self.rows),
                    model_transfer_bytes=self.transferred, planned_bytes=self.planned,
                    scale_out=sum(a['action'] == 'out' for a in self.actions),
                    scale_in=sum(a['action'] == 'in' for a in self.actions),
                    rebalance=sum(a['action'] == 'rebalance' for a in self.actions),
                    direction_reversals=self.reversals, completed_actions=sum(a['completed_window'] is not None for a in self.actions),
                    pending_actions=int(self.pending is not None), split_units=self.splits, merged_units=self.merges,
                    max_units=max(r['unit_count'] for r in self.rows))


def balanced(units, active, reads, writes, config):
    """선행 방법이 배치 자체를 지정하지 않을 때만 사용하는 공통 LPT 실행 어댑터."""
    used = {n: 0. for n in active}
    result = []
    for unit in sorted(units, key=lambda u: (-unit_work(u, reads, writes, config=config), u.keys)):
        n = min(active, key=lambda n: (used[n], n not in unit.holders, n))
        moved = Unit(unit.keys, (n,))
        result.append(moved)
        used[n] += unit_work(moved, reads, writes, config=config)
    return tuple(sorted(result, key=lambda u: u.keys))


def queue_sample(rate, write_ratio, seed, config, windows=6):
    """서비스 모델의 독립 calibration. 값은 실제 서버 latency가 아니다."""
    rng = np.random.default_rng(seed)
    free = 0.
    latencies = []
    completed = 0
    for t in range(windows):
        count = int(rng.poisson(rate))
        for arrival in np.sort(rng.uniform(t, t + 1, count)):
            write = rng.random() < write_ratio
            free = max(free, float(arrival)) + (config.write_service if write else config.read_service)
            if t >= windows // 2:
                latencies.append(free - arrival)
            completed += free <= windows
    return float(np.quantile(latencies, .99)), completed / windows
