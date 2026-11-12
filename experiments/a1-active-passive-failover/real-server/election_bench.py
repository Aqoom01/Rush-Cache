"""실제 선출 리더 호스트에서 같은 시계로 JVM crash와 첫 새 쓰기 응답을 측정한다."""
import argparse
import json
import os
from pathlib import Path
import signal
import time
import uuid
import pb_control
from pb_bench import Client,quantile


def discover(client,protocol,budget=25):
    end=time.monotonic()+budget
    while time.monotonic()<end:
        try:
            states=[client.status(i) for i in range(3)]
            if protocol=='RAFT':
                leaders=[s for s in states if s['role']=='LEADER' and s['readyNs']>=0 and s['applied']==s['logLength']]
                if len(leaders)==1:
                    leader=leaders[0]
                    if all(s['epoch']==leader['epoch'] and s['leader']==leader['node'] for s in states):
                        return leader['node'],leader['epoch'],states
            elif all(s['epoch']==(1 if protocol=='PB' else 0) and s.get('state','NORMAL')=='NORMAL' for s in states) and all(s['heartbeatSeen'] for s in states[1:]):
                return 0,states[0]['epoch'],states
        except (OSError,EOFError):
            pass
        time.sleep(.05)
    raise RuntimeError('cluster did not reach one ready leader')


def verify(client,nodes,keys,value):
    end=time.monotonic()+3
    while True:
        good=True
        for node in nodes:
            for key in keys:
                r=client.local(node,key)
                good &= r['status']==0 and r['value']==value
        if good:
            return len(nodes)*len(keys)
        if time.monotonic()>=end:
            raise RuntimeError('successful SET value missing from local KV')
        time.sleep(.02)


def run(args):
    client=Client(args.hosts.split(','),client_port=args.client_port,peer_port=args.peer_port)
    result={'schema':'a1-real-election-1','protocol':args.protocol,'runId':uuid.uuid4().hex,
            'utcEpochMs':int(time.time()*1000),'timeDomain':'wall-clock','durationUnit':'ns',
            'valueBytes':args.value,'timeoutMs':args.timeout,'repeat':args.repeat,'electionSeed':args.repeat,
            'electionTimeoutRangeMs':[args.timeout,2*args.timeout] if args.protocol=='RAFT' else None,
            'heartbeatMs':50,'tickMs':10,'normalConcurrency':1,'normalRequests':args.count,'warmupRequests':30,
            'clientPlacement':'elected-original-leader-host','normalSamplesNs':[],'attempts':[],
            'failureModel':'settled idle original leader JVM SIGKILL after both backup values verified'}
    try:
        leader,epoch,states=discover(client,args.protocol)
        assert leader==args.controller,'controller is not on the elected leader host'
        result.update(originalLeader=leader,initialEpoch=epoch,statesInitial=states)
        detector=states[leader].get('failureDetector','FIXED_TIMEOUT')
        result['failureDetector']=detector
        result['failureDetectorConfig']={k:states[leader][k] for k in (
            'failureProbeIntervalMs','failureDirectTimeoutMs','failureIndirectTimeoutMs',
            'failureSuspicionMs','failureStartupGraceMs','failureAwarenessMaxMultiplier',
            'failureSuspicionMaxTimeoutMultiplier','failureMaxConfirmations') if k in states[leader]}
        if detector!='FIXED_TIMEOUT':
            # Do not inject while some detectors are still in startup grace.
            wait_ms=max(s['failureStartupGraceMs']+2*s['failureProbeIntervalMs']-s['uptimeNs']/1e6 for s in states)
            if wait_ms>0:
                time.sleep(wait_ms/1000)
            leader_now,epoch_now,_=discover(client,args.protocol)
            assert (leader_now,epoch_now)==(leader,epoch),'leader changed during healthy startup'
        survivors=[i for i in range(3) if i!=leader]
        value=bytes((i*17+3)%256 for i in range(args.value))
        for i in range(30):
            assert client.set(leader,100+i,f'warm-{i}'.encode(),value,epoch)['status']==0,'warmup failed'
        normal_begin=[client.status(i) for i in range(3)]
        result['normalBytesBegin']=[s['sentBytes'] for s in normal_begin]
        result['normalFdBytesBegin']=[s.get('fdSentBytes',0) for s in normal_begin]
        keys=[];begin=time.perf_counter_ns()
        for i in range(args.count):
            key=f'pre-{i}'.encode(); before=time.perf_counter_ns()
            r=client.set(leader,1000+i,key,value,epoch)
            after=time.perf_counter_ns()
            assert r['status']==0 and r['epoch']==epoch,'normal SET failed / epoch changed'
            keys.append(key); result['normalSamplesNs'].append(after-before)
        end=time.perf_counter_ns()
        result.update(normalElapsedNs=end-begin,normalP50Ns=quantile(result['normalSamplesNs'],.5),
                      normalP99Ns=quantile(result['normalSamplesNs'],.99),observedSingleClientOpsPerSecond=args.count*1e9/(end-begin))
        normal_end=[client.status(i) for i in range(3)]
        result['normalBytesEnd']=[s['sentBytes'] for s in normal_end]
        result['normalFdBytesEnd']=[s.get('fdSentBytes',0) for s in normal_end]
        time.sleep(.2)
        result['preCrashVerifiedCopies']=verify(client,survivors,keys,value)
        result['statesBefore']=[client.status(i) for i in range(3)]
        assert all(s['epoch']==epoch for s in result['statesBefore']),'pre-crash term changed'
        calibrations={}
        for node in survivors:
            before=time.perf_counter_ns(); status=client.status(node); after=time.perf_counter_ns()
            calibrations[node]={'beginNs':before,'endNs':after,'nodeNanoTime':status['nanoTime']}
        result['clockCalibrations']=calibrations
        # pb_control validates the exact main class and config path in this directory.
        pid=int(pb_control.PID.read_text())
        assert pb_control.alive(pid),'dedicated JVM identity check failed'
        result['faultCallBeginNs']=time.perf_counter_ns()
        os.kill(pid,signal.SIGKILL)
        result['faultCallReturnNs']=time.perf_counter_ns()
        route=leader;current_epoch=epoch;budget=time.monotonic()+15
        while time.monotonic()<budget:
            attempt={'node':route,'epoch':current_epoch,'startNs':time.perf_counter_ns()}
            try:
                r=client.set(route,100000,b'post-first',value,current_epoch)
                attempt.update(status=r['status'],responseEpoch=r['epoch'],detail=r['detail'],endNs=time.perf_counter_ns())
                result['attempts'].append(attempt); current_epoch=max(current_epoch,r['epoch'])
                if r['status']==0:
                    assert route in survivors and r['epoch']>epoch,'success was not from a new leader'
                    result.update(newLeader=route,newEpoch=r['epoch'],firstNewPrimaryOkNs=attempt['endNs'])
                    break
                if r['detail'].startswith('redirect-node='):
                    route=int(r['detail'].split('=')[1])
                elif r['status'] not in (5,6,8):
                    route=(route+1)%3
            except (OSError,EOFError) as error:
                attempt.update(status='CLIENT_FAILURE',error=type(error).__name__,endNs=time.perf_counter_ns())
                result['attempts'].append(attempt);route=(route+1)%3
            time.sleep(.01)
        else:
            raise RuntimeError('no successful write from a surviving leader within 15s')
        result['failoverNs']=result['firstNewPrimaryOkNs']-result['faultCallBeginNs']
        result['faultInvocationNs']=result['faultCallReturnNs']-result['faultCallBeginNs']
        # Collect diagnostics after the actual KV audit; OK may precede observation publication.
        result['postCrashVerifiedCopies']=verify(client,survivors,keys+[b'post-first'],value)
        result['statesAfter']=[client.status(i) for i in survivors]
        s=next(s for s in result['statesAfter'] if s['node']==result['newLeader'])
        assert s['epoch']==result['newEpoch'] and s['readyNs']>=s['detectedNs']>=0
        if args.protocol=='RAFT':
            assert s['role']=='LEADER' and s['electionTerm']==s['epoch']
            assert s['detectedNs']>result['statesBefore'][s['node']]['detectedNs'],'old election timing reused'
            result['electionOrigin']=s.get('raftElectionOrigin','TIMEOUT')
            result['electionTrigger']=s.get('raftElectionTrigger','TIMEOUT')
            result['fallbackUsed']=result['electionOrigin']=='FALLBACK'
            if detector!='FIXED_TIMEOUT' and result['electionOrigin']=='SWIM_DEAD':
                assert s['raftFailedLeader']==leader and s['raftFailureSignalTerm']==epoch,'wrong leader/term FD evidence'
                assert 0<=s['raftFdDeadNs']<=s['detectedNs'],'missing or late DEAD evidence'
                result['deadToElectionNs']=s['detectedNs']-s['raftFdDeadNs']
                calibration=calibrations[s['node']]
                result['fdDetectionLatencyBoundsNs']=[
                    calibration[k]+s['raftFdDeadNs']-calibration['nodeNanoTime']-result['faultCallBeginNs']
                    for k in ('beginNs','endNs')]
                result['fdClockAssumption']='RTT calibration interval; relative clock drift during run is not bounded by this estimate'
        result['promotionNs']=s['readyNs']-s['detectedNs']
        result['preservedSuccesses']=len(keys)
        result['staleEpochStatus']=client.set(result['newLeader'],100001,b'stale',value,epoch)['status']
        assert result['staleEpochStatus']==5
        backup=next(i for i in survivors if i!=result['newLeader'])
        r=client.set(backup,100002,b'backup-direct',value,result['newEpoch'])
        result['backupWriteStatus']=r['status']
        assert r['status']==4 and r['detail']==f"redirect-node={result['newLeader']}"
        result['ok']=True
    except Exception as e:
        result.update(ok=False,error=repr(e));raise
    finally:
        client.close();Path(args.out).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ['protocol','ok','valueBytes','timeoutMs','repeat','originalLeader','newLeader','initialEpoch','newEpoch','failoverNs','promotionNs']}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hosts',required=True);p.add_argument('--protocol',choices=['PB','VR','RAFT'],required=True)
    p.add_argument('--discover',action='store_true');p.add_argument('--controller',type=int);p.add_argument('--out')
    p.add_argument('--value',type=int,default=64);p.add_argument('--timeout',type=int,default=500)
    p.add_argument('--repeat',type=int,default=1);p.add_argument('--count',type=int,default=128)
    p.add_argument('--client-port',type=int,default=7000);p.add_argument('--peer-port',type=int,default=7001)
    args=p.parse_args()
    if args.discover:
        client=Client(args.hosts.split(','),client_port=args.client_port,peer_port=args.peer_port)
        try:
            leader,epoch,states=discover(client,args.protocol);print(json.dumps({'leader':leader,'epoch':epoch,'states':states}))
        finally:client.close()
    else:
        run(args)
