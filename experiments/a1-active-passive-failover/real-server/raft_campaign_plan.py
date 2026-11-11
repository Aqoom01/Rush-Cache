"""Immutable scenario definitions shared by local validation and the EC2 campaign."""
DETECTORS=('FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD')
CORE_SCENARIOS=('healthy','crash','cpu-observer','cpu-contention-observer','delay-udp','delay-both')
FIXED_SETTINGS=dict(raftTimeoutMs=500,heartbeatMs=50,probeIntervalMs=200,
                    directTimeoutMs=50,indirectTimeoutMs=50,suspicionMs=500)
SCENARIOS={
    'healthy':dict(kind='healthy',target='leader',seconds=8),
    'crash':dict(kind='crash',target='leader',seconds=0),
    'loss-udp':dict(kind='loss',target='leader',scope='udp',seconds=8),
    'loss-both':dict(kind='loss',target='leader',scope='both',seconds=8),
    'delay-udp':dict(kind='delay',target='leader',scope='udp',seconds=8),
    'delay-both':dict(kind='delay',target='leader',scope='both',seconds=8),
    'cpu-leader':dict(kind='cpu-pause',target='leader',seconds=2),
    'cpu-observer':dict(kind='cpu-pause',target='observer',seconds=2),
    'cpu-contention-observer':dict(kind='cpu-contention',target='observer',seconds=8,workers=8),
    'ssd-stall-leader':dict(kind='io-stall',target='leader',seconds=2,storage='nvme'),
    'ssd-stall-observer':dict(kind='io-stall',target='observer',seconds=2,storage='nvme'),
    'ssd-contention':dict(kind='fio',target='leader',seconds=8,storage='nvme'),
    'partition':dict(kind='partition',target='leader',scope='both',seconds=8),
    'inflight-crash':dict(kind='crash',target='leader',seconds=0,inflight=True),
}


def matrix(repeats=3,detectors=DETECTORS,scenarios=None,study='validation',core_repeats=10):
    if repeats<1: raise ValueError('repeats must be positive')
    selected=list(SCENARIOS) if scenarios is None else list(scenarios)
    if set(selected)-SCENARIOS.keys() or set(detectors)-set(DETECTORS):
        raise ValueError('unknown detector/scenario')
    if study not in ('validation','presentation') or core_repeats<1: raise ValueError('invalid study')
    counts={s:max(repeats,core_repeats) if study=='presentation' and s in CORE_SCENARIOS else repeats for s in selected}
    plan=[]
    for repeat in range(1,max(counts.values())+1):
        for scenario in selected:
            if repeat>counts[scenario]: continue
            # Rotate order (ABC/BCA/CAB), with the same seed inside each comparison block.
            offset=(repeat-1)%len(detectors)
            for detector in (*detectors[offset:],*detectors[:offset]):
                plan.append(dict(case=f'{scenario}-{detector.lower()}-r{repeat}',scenario=scenario,
                                 detector=detector,repeat=repeat,**SCENARIOS[scenario],
                                 normalSeconds=60 if study=='presentation' and scenario=='healthy' else 0,
                                 warmupSeconds=10 if study=='presentation' and scenario=='healthy' else 0,
                                 normalRate=50))
    return plan
