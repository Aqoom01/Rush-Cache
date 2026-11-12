"""Continuous client/history audit for one Raft scenario, controlled by an external driver."""
import argparse
import json
from pathlib import Path
import subprocess
import threading
import time
import uuid

from election_bench import discover,verify
from pb_bench import Client,quantile
from raft_measurements import normal_traffic,raft_journal,leadership_counts


def run(a):
    root=Path(__file__).resolve().parent;hosts=a.hosts.split(',');ports=tuple(map(int,a.ports.split(',')))
    spec=json.loads(a.spec)
    result=dict(schema='raft-scenario-1',runId=uuid.uuid4().hex,detector=a.detector,spec=spec,
                ok=False,attempts=[],samples=[],acknowledged={},normalSamplesNs=[],clock='controller monotonic',
                storage='nvme' if spec.get('storage')=='nvme' else 'memory')
    client=Client(hosts,client_port=ports[0],peer_port=ports[1]);stop=threading.Event();collector=None;fault_collector=None
    fault_state={};fault_lock=threading.Lock()
    def transfer(direction,name):
        if a.s3:
            source,dest=((str(root/name),a.s3+'/'+name) if direction=='up' else (a.s3+'/'+name,str(root/name)))
            p=subprocess.run(['aws','s3','cp',source,dest,'--region',a.region,'--only-show-errors'],
                             capture_output=True,text=True,timeout=15)
            if p.returncode: raise RuntimeError(p.stderr)
    def ready():
        (root/'ready.json').write_text(json.dumps(dict(leader=a.controller,atNs=time.monotonic_ns())))
        transfer('up','ready.json')
    def fault_events():
        with fault_lock:
            if a.s3: transfer('down','fault-events.json')
            return json.loads((root/'fault-events.json').read_text())
    def collect_fault():
        # S3 control polling must never block the measured request/retry path.
        while not stop.is_set():
            try: fault_state['events']=fault_events()
            except (FileNotFoundError,RuntimeError,json.JSONDecodeError,subprocess.TimeoutExpired): pass
            stop.wait(1)
    def collect():
        diagnostic=Client(hosts,client_port=ports[0],peer_port=ports[1])
        try:
            while not stop.is_set():
                for node in range(3):
                    began=time.monotonic_ns()
                    try:
                        state=diagnostic.status(node)
                        result['samples'].append(dict(node=node,beginNs=began,endNs=time.monotonic_ns(),status=state))
                    except (OSError,EOFError) as error:
                        result['samples'].append(dict(node=node,beginNs=began,endNs=time.monotonic_ns(),error=type(error).__name__))
                stop.wait(.1)
        finally: diagnostic.close()
    try:
        leader,term,states=discover(client,'RAFT')
        if leader!=a.controller: raise RuntimeError('controller is not current leader')
        # Equal settling time for all modes, including the baseline without a UDP detector.
        wait=max(s['failureStartupGraceMs']+400-s['uptimeNs']/1e6 for s in states)
        if wait>0: time.sleep(wait/1000)
        leader,term,states=discover(client,'RAFT')
        if leader!=a.controller: raise RuntimeError('leader changed before experiment')
        result.update(originalLeader=leader,initialTerm=term,statesBefore=states)
        value=b'x'*a.value
        for i in range(30):
            key=f'warm-{i}'
            reply=client.set(leader,100+i,key.encode(),value,term)
            if reply['status']!=0: raise RuntimeError('warmup failed')
            result['acknowledged'][key]=100+i
        result['calibrations']={}
        for node in range(3):
            began=time.monotonic_ns();state=client.status(node)
            result['calibrations'][str(node)]=dict(beginNs=began,endNs=time.monotonic_ns(),nodeNanoTime=state['nanoTime'])
        collector=threading.Thread(target=collect,daemon=True);collector.start()
        result['warmupRequests']=30
        normal_traffic(client,leader,term,value,spec,result)
        verify(client,[0,1,2],list(k.encode() for k in result['acknowledged']),value)
        if spec['kind']!='healthy':
            current_leader,current_term,_=discover(client,'RAFT')
            if (current_leader,current_term)!=(leader,term):
                raise RuntimeError('leader/term changed before fault; retain normal evidence and inspect instability')
        if not spec.get('inflight'): ready()
        fault_collector=threading.Thread(target=collect_fault,daemon=True);fault_collector.start()
        operation=max(result['acknowledged'].values())+1000;route=leader;epoch=term;events=None;first=True;fault_seen_at=None
        deadline=time.monotonic()+a.window
        while time.monotonic()<deadline:
            now=time.monotonic()
            events=fault_state.get('events')
            if events and any(e['event']=='released' for e in events): fault_seen_at=fault_seen_at or now
            key=f'work-{operation}'.encode();attempt=dict(node=route,operation=operation,startNs=time.monotonic_ns())
            if first and spec.get('inflight'):
                def on_sent(*_):
                    ready()
                    # Hold response consumption while the driver terminates the JVM.
                    limit=time.monotonic()+25
                    while time.monotonic()<limit:
                        try:
                            es=fault_events()
                            if any(e['event']=='applied' for e in es): return
                        except (FileNotFoundError,RuntimeError,json.JSONDecodeError): pass
                        time.sleep(.1)
                    raise RuntimeError('inflight fault was not applied')
                client.on_sent=on_sent
            try:
                reply=client.set(route,operation,key,value,epoch,deadline=500)
                attempt.update(status=reply['status'],epoch=reply['epoch'],endNs=time.monotonic_ns())
                epoch=max(epoch,reply['epoch'])
                if reply['status']==0:
                    result['acknowledged'][key.decode()]=operation
                    operation+=1
                elif reply['detail'].startswith('redirect-node='):
                    route=int(reply['detail'].split('=')[1])
                else: route=(route+1)%3
            except (OSError,EOFError) as error:
                attempt.update(error=type(error).__name__,endNs=time.monotonic_ns());route=(route+1)%3
            finally:
                client.on_sent=None;first=False
            result['attempts'].append(attempt)
            if fault_seen_at and now-fault_seen_at>=3 and attempt.get('status')==0: break
            # Bound log/frame size while maintaining ongoing traffic throughout the fault.
            time.sleep(.05)
        events=fault_events();result['faultEvents']=events
        if any(e['event']=='error' for e in events): raise RuntimeError('fault worker failed')
        applied=next(e for e in events if e['event']=='applied')
        released=next(e for e in events if e['event']=='released')
        calibration=result['calibrations'][str(a.target)]
        lower=lambda t:calibration['beginNs']+t-calibration['nodeNanoTime']
        upper=lambda t:calibration['endNs']+t-calibration['nodeNanoTime']
        # Linux Java nanoTime/Python monotonic use CLOCK_MONOTONIC on the same target host.
        result['faultAppliedBoundsNs']=[lower(applied['nanoTime']),upper(applied['nanoTime'])]
        result['faultClockAssumption']='Linux same-host monotonic; status RTT offset interval; relative drift unbounded'
        begin,end=upper(applied['nanoTime']),lower(released['nanoTime'])
        successes=[x for x in result['attempts'] if x.get('status')==0 and x['startNs']>=begin]
        result['firstSuccessAfterFaultNs']=min((x['endNs'] for x in successes),default=None)
        result['recoveryUpperNs']=None if not successes else result['firstSuccessAfterFaultNs']-lower(applied['nanoTime'])
        if spec['kind']=='partition':
            forbidden=[x for x in successes if x['node']==a.target and x['startNs']>=begin+200_000_000 and x['endNs']<end]
            if forbidden: raise AssertionError('minority primary acknowledged a new write during partition')
            result['minoritySuccesses']=0
        # Locate a current leader without requiring the intentionally crashed process to answer.
        survivors=[i for i in range(3) if not(spec['kind']=='crash' and i==a.target)]
        budget=time.monotonic()+20;current=None
        while time.monotonic()<budget:
            for node in survivors:
                try:
                    state=client.status(node)
                    if state['role']=='LEADER' and state['applied']==state['logLength']:
                        current=state;break
                except (OSError,EOFError): pass
            if current: break
            time.sleep(.1)
        if current is None: raise RuntimeError('no recovered ready leader')
        result.update(finalLeader=current['node'],finalTerm=current['epoch'])
        if spec['kind']=='crash' and (current['node']==a.target or current['epoch']<=term):
            raise AssertionError('leader crash did not produce a new term/leader')
        reply=client.set(current['node'],999999,b'post-final',value,current['epoch'])
        if reply['status']!=0: raise RuntimeError('final traffic recovery failed')
        keys=[k.encode() for k in result['acknowledged']]+[b'post-final']
        result['verifiedCopies']=verify(client,survivors,keys,value)
        result['preservedSuccesses']=len(result['acknowledged'])
        if current['epoch']>term:
            result['staleEpochStatus']=client.set(current['node'],999998,b'stale',value,term)['status']
            if result['staleEpochStatus']!=5: raise AssertionError('stale epoch accepted')
        result['statesAfter']=[client.status(i) for i in survivors]
        if spec.get('storage')=='nvme':
            counters=[s['storageCounters'] for s in result['statesAfter']]
            if not all(s['storageEngine']=='nvme' for s in result['statesAfter']): raise AssertionError('NVMe path not enabled')
            if not sum(c.get('storage_read_ops',0) for c in counters): raise AssertionError('no physical storage reads recorded')
            if spec['kind']=='io-stall' and not sum(c.get('injected_io_stall_tasks',0) for c in counters):
                raise AssertionError('I/O completion stall did not affect a storage task')
        # Add final snapshots so a formerly stalled node's retained journal is collected.
        for state in result['statesAfter']:
            result['samples'].append(dict(node=state['node'],beginNs=time.monotonic_ns(),endNs=time.monotonic_ns(),status=state))
        samples=list(result['samples'])
        unique={}
        for sample in samples:
            for event in sample.get('status',{}).get('fdEvents',[]):
                if event['atNs']<result['calibrations'][str(sample['node'])]['nodeNanoTime']: continue
                unique[(sample['node'],event['member'],event['current'],event['incarnation'],event['atNs'])]=event
        if any(len(s.get('status',{}).get('fdEvents',[]))>=2048 for s in samples):
            raise RuntimeError('FD event journal at capacity; counts may be incomplete')
        result['fdDeadEvents']=sum(e['current']=='DEAD' for e in unique.values())
        detection=[]
        for (observer,member,status,incarnation,at_ns),event in unique.items():
            if status!='DEAD' or member!=a.target: continue
            c=result['calibrations'][str(observer)]
            bounds=[c[k]+at_ns-c['nodeNanoTime'] for k in ('beginNs','endNs')]
            if bounds[1]<lower(applied['nanoTime']): continue
            detection.append(dict(observer=observer,member=member,incarnation=incarnation,
                                  latencyBoundsNs=[bounds[0]-upper(applied['nanoTime']),bounds[1]-lower(applied['nanoTime'])]))
        result['detections']=detection
        result['fdDetectionObserved']=bool(detection)
        journal=raft_journal(samples,term,result['calibrations'],result['faultAppliedBoundsNs'])
        result['raftJournal']=journal
        elections=[e for e in journal if e['kind']=='STARTED' and e['faultPhase']=='after']
        result['raftElections']=elections
        result['boundaryUncertainRaftEvents']=sum(e['faultPhase']=='boundary-uncertain' for e in journal)
        result.update(leadership_counts(journal,leader))
        result['electionStarts']=sum(e['kind']=='STARTED' for e in journal)
        result['fallbackElections']=sum(e['origin']=='FALLBACK' for e in elections)
        result['firstRaftElectionLatencyBoundsNs']=[min(e['latencyBoundsNs'][i] for e in elections) for i in (0,1)] if elections else None
        result['falsePositiveEligible']=spec['kind'] in ('healthy','loss','delay','io-stall','fio') or spec['target']=='observer'
        result['electionsWhileLeaderAlive']=result['electionStarts'] if result['falsePositiveEligible'] else None
        result['leaderChangesWhileLeaderAlive']=result['leaderIdentityChanges'] if result['falsePositiveEligible'] else None
        during=[x for x in result['attempts'] if x['startNs']>=begin and x['endNs']<=end]
        result['faultWindowAttempts']=len(during)
        result['faultWindowAttemptSuccessRate']=sum(x.get('status')==0 for x in during)/len(during) if during else None
        result['falsePositiveDefinition']='DEAD transition on a process that is neither crashed, paused nor partitioned'
        # Exclude the intentionally paused observer itself from false-positive counts.
        result['falseDeadEvents']=sum(e['current']=='DEAD' and not(spec['kind']=='cpu-pause' and e['member']==a.target)
                                      for e in unique.values()) if result['falsePositiveEligible'] else None
        result['falseLeaderDeadEvents']=sum(e['current']=='DEAD' and e['member']==leader for e in unique.values()) if result['falsePositiveEligible'] else None
        result['fdAwarenessMax']=max((s.get('status',{}).get('fdAwarenessScore',0) for s in samples if s['node']==a.target),default=0)
        result['fdProbeIntervalMaxNs']=max((s.get('status',{}).get('fdEffectiveProbeIntervalNs',0) for s in samples if s['node']==a.target),default=0)
        observation=max(s['endNs'] for s in result['samples'])-min(s['beginNs'] for s in result['samples'])
        result['observationNs']=observation
        exposure=max(1,3*observation-(int(spec['seconds']*1e9) if spec['kind']=='cpu-pause' else 0))
        result['liveNodeExposureNs']=exposure if result['falsePositiveEligible'] else None
        result['falseDeadPerLiveNodeMinute']=result['falseDeadEvents']*60e9/exposure if result['falsePositiveEligible'] else None
        result['normalP50Ns']=quantile(result['normalSamplesNs'],.5) if result['normalSamplesNs'] else None
        result['normalP99Ns']=quantile(result['normalSamplesNs'],.99) if result['normalSamplesNs'] else None
        result['ok']=True
    except Exception as error:
        result['error']=repr(error);raise
    finally:
        stop.set()
        if collector is not None: collector.join(timeout=8)
        if fault_collector is not None: fault_collector.join(timeout=16)
        client.close()
        (root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        if a.s3: transfer('up','result.json')
    print(json.dumps({k:result.get(k) for k in ('ok','detector','preservedSuccesses','recoveryUpperNs','fdDeadEvents','falseDeadEvents')}))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--hosts',required=True);p.add_argument('--spec',required=True)
    p.add_argument('--controller',type=int,required=True);p.add_argument('--target',type=int,required=True)
    p.add_argument('--detector',required=True);p.add_argument('--ports',default='7000,7001,7002')
    p.add_argument('--window',type=float,default=60);p.add_argument('--value',type=int,default=1024)
    p.add_argument('--s3');p.add_argument('--region',default='ap-northeast-2')
    run(p.parse_args())
