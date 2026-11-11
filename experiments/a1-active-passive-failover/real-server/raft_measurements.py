"""Measurement helpers shared by the benchmark and offline evidence tests."""
import time


def normal_traffic(client, leader, term, value, spec, result):
    """One synchronous client at a fixed offered rate; missed slots never become bursts.

    This is a latency/communication experiment, not a saturation-throughput benchmark.
    Byte counters are application protocol bytes, excluding TCP/IP headers/retransmits.
    """
    rate = spec.get('normalRate', 50)
    seconds = spec.get('normalSeconds', 0)
    warmup = spec.get('warmupSeconds', 0)
    interval = 1 / rate
    operation = 1000
    route = leader
    def phase(duration, count=None):
        nonlocal operation, route, term
        start = time.monotonic(); due = start; rows = []; missed = 0
        while (time.monotonic() < start + duration if count is None else len(rows) < count):
            remaining = due - time.monotonic()
            if remaining > 0: time.sleep(remaining)
            if count is None and time.monotonic() >= start + duration: break
            key = f'normal-{operation}'
            began = time.monotonic_ns()
            row = dict(startNs=began, scheduleLagNs=max(0, began-int(due*1e9)))
            try:
                reply = client.set(route, operation, key.encode(), value, term, deadline=500)
                row['status'] = reply['status']; term = max(term, reply['epoch'])
                if reply['status'] == 0:
                    result['acknowledged'][key] = operation; operation += 1
                elif reply['detail'].startswith('redirect-node='):
                    route = int(reply['detail'].split('=')[1])
                else: route = (route+1) % 3
            except (OSError, EOFError) as error:
                row['error'] = type(error).__name__; route = (route+1) % 3
            row['endNs'] = time.monotonic_ns(); rows.append(row)
            due += interval
            skipped = max(0, int((time.monotonic()-due)//interval))
            due += skipped*interval; missed += skipped
        return rows, missed, time.monotonic()-start
    if warmup: phase(warmup)
    before = [client.status(i) for i in range(3)]
    rows, missed, elapsed = phase(seconds, None if seconds else 32)
    after = [client.status(i) for i in range(3)]
    successful = [r for r in rows if r.get('status') == 0]
    result.update(normalAttempts=rows, normalRequests=len(rows), normalSuccesses=len(successful),
                  normalSuccessRate=len(successful)/len(rows) if rows else None,
                  normalSamplesNs=[r['endNs']-r['startNs'] for r in successful],
                  normalDurationNs=int(elapsed*1e9), normalOfferedRate=rate,
                  normalAchievedSuccessRate=len(successful)/elapsed,
                  normalMissedSlots=missed, normalMinimumSamplesMet=len(successful)>=1000 if seconds else None,
                  normalReplicationBytes=sum(e['sentBytes']-b['sentBytes'] for b,e in zip(before,after)),
                  normalFdBytes=sum(e['fdSentBytes']-b['fdSentBytes'] for b,e in zip(before,after)),
                  normalByteDefinition='application protocol bytes; excludes TCP/IP headers and retransmissions')


def raft_journal(samples, initial_term, calibrations, fault_bounds):
    """Use retained owner-thread events, rather than inferring transitions from polling."""
    events = {}
    for sample in samples:
        state = sample.get('status', {})
        if state.get('raftElectionEventsDropped', 0):
            raise RuntimeError('Raft diagnostic journal overflow; event counts incomplete')
        for event in state.get('raftElectionEvents', []):
            if event['term'] <= initial_term: continue
            c = calibrations[str(sample['node'])]
            bounds = [c[k]+event['atNs']-c['nodeNanoTime'] for k in ('beginNs','endNs')]
            events[(sample['node'], event['term'], event['kind'])] = dict(
                node=sample['node'], term=event['term'], kind=event['kind'], origin=event['origin'],
                atBoundsNs=bounds,
                faultPhase='before' if bounds[1]<fault_bounds[0] else
                           'after' if bounds[0]>=fault_bounds[1] else 'boundary-uncertain',
                latencyBoundsNs=[bounds[0]-fault_bounds[1],bounds[1]-fault_bounds[0]])
    return sorted(events.values(), key=lambda e:(e['term'], e['node'], e['kind']))


def leadership_counts(events, original_leader):
    # A new term elected by the same node is a reelection, not a leader identity change.
    wins=sorted((e for e in events if e['kind']=='BECAME_LEADER'),key=lambda e:(e['term'],e['node']))
    by_term={}
    for event in wins:
        if event['term'] in by_term and by_term[event['term']] != event['node']:
            raise AssertionError('two leaders acquired the same Raft term')
        by_term[event['term']]=event['node']
    current=original_leader; changes=0
    for node in by_term.values():
        changes+=node!=current; current=node
    return dict(leadershipAcquisitions=len(by_term),leaderIdentityChanges=changes,
                sameLeaderReelections=len(by_term)-changes)
