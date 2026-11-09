"""Chain 실서버 실험. controller는 crash 대상 호스트에서 실행하며, 정확한 영구 crash를 아는 외부 master 역할(FREEZE/REPAIR/OPEN)을 맡는다.
감지는 실제 failure detector가 아니라 controller의 고정 대기(timeout.ms)다. 모든 시각은 같은 controller의 perf_counter다."""
import argparse
import json
import os
from pathlib import Path
import signal
import struct
import time
import uuid
from pb_bench import Client,quantile
import pb_control

BUSY=6


def freeze_body(epoch,survivors):
    return struct.pack('!BQI',0,epoch,len(survivors))+b''.join(struct.pack('!Q',s) for s in survivors)


def repair_body(epoch):
    return struct.pack('!BQ',1,epoch)


def open_body(epoch,length):
    return struct.pack('!BQI',2,epoch,length)


def control(client,node,body):
    r=client.request(node,38,body,peer=True)
    if r['status']!=0:
        raise RuntimeError(f"master control rejected on node {node}: {r['status']} {r['detail']}")
    return r['detail']


def digest(client,node):
    r=client.request(node,37,b'',peer=True);assert r['status']==0 and len(r['value'])==36;return r['value']


def get(client,node,key,epoch,deadline=1000):
    body=struct.pack('!QQQII',17,999,epoch,deadline,len(key))+key
    return client.request(node,1,body)


def set_retry(client,node,operation,key,value,epoch,budget=3.0):
    """head는 이전 쓰기의 역방향 Ack 전에 BUSY를 돌려준다. 같은 요청을 재시도하고 재시도 횟수·전체 소요를 함께 돌려준다."""
    start=time.perf_counter_ns();end=time.monotonic()+budget;busy=0
    while True:
        r=client.set(node,operation,key,value,epoch)
        if r['status']!=BUSY or time.monotonic()>=end:
            return r,busy,time.perf_counter_ns()-start
        busy+=1;time.sleep(.0005)


def states(client,nodes=(0,1,2)):
    return [client.status(i) for i in nodes]


def discover(client,read_mode,budget=25):
    end=time.monotonic()+budget
    while time.monotonic()<end:
        try:
            s=states(client)
            if all(x['protocol']=='CHAIN' and x['epoch']==1 and x['state']=='RUNNING' and x['chain']==[0,1,2] and x['readMode']==read_mode for x in s):
                return s
        except (OSError,EOFError):
            pass
        time.sleep(.05)
    raise RuntimeError('chain did not reach RUNNING with the expected configuration')


def verify(client,nodes,keys,value,budget=3):
    end=time.monotonic()+budget
    while True:
        good=all(client.local(n,k)['status']==0 and client.local(n,k)['value']==value for n in nodes for k in keys)
        if good:
            return len(nodes)*len(keys)
        if time.monotonic()>=end:
            raise RuntimeError('successful SET value missing from local KV')
        time.sleep(.02)


def failover(client,result,crashed,epoch,detection_ms):
    """정확한 crash를 아는 master 절차. detection 대기 후 FREEZE→REPAIR→digest 일치 확인→OPEN. 각 단계 시각을 기록한다."""
    survivors=[n for n in (0,1,2) if n!=crashed]
    time.sleep(detection_ms/1000)
    result['masterDetectedNs']=time.perf_counter_ns()
    result['freezeReplies']=[control(client,n,freeze_body(epoch,survivors)) for n in survivors]
    result['masterFrozenNs']=time.perf_counter_ns()
    for n in survivors:
        control(client,n,repair_body(epoch))
    result['masterRepairControlNs']=time.perf_counter_ns()
    end=time.monotonic()+10;length=None;polls=0
    while time.monotonic()<end:
        s=states(client,survivors);length=s[0]['logLength'];polls+=1
        if all(x['state']=='REPAIRING' and x['chain']==survivors and x['epoch']==epoch and x['logLength']==x['applied']==x['acknowledged']==length for x in s):
            break
        time.sleep(.005)
    else:
        raise RuntimeError('chain repair did not converge')
    result['masterRepairPolledNs']=time.perf_counter_ns();result['repairPolls']=polls;result['repairLastPoll']=[{k:x[k] for k in ('node','state','logLength','applied','acknowledged','sent')} for x in s]
    digests=[digest(client,n) for n in survivors]
    assert len(set(digests))==1,'survivor logs differ after repair'
    result['masterRepairedNs']=time.perf_counter_ns();result['repairedLength']=length
    for n in survivors:
        control(client,n,open_body(epoch,length))
    result['masterOpenedNs']=time.perf_counter_ns()
    return survivors


def run(args):
    client=Client(args.hosts.split(','))
    crashed={'head':0,'middle':1,'tail':2}[args.position]
    result={'schema':'a1-real-chain-2','protocol':'CHAIN','readMode':args.read_mode,'runId':uuid.uuid4().hex,'utcEpochMs':int(time.time()*1000),
            'timeDomain':'wall-clock','durationUnit':'ns','position':args.position,'crashedNode':crashed,'valueBytes':args.value,
            'timeoutMs':args.timeout,'repeat':args.repeat,'retryMs':args.retry,'warmupRequests':30,'normalRequests':128,'tailReads':128,
            'normalConcurrency':1,'clientPlacement':'crashed-node-host','normalSamplesNs':[],'normalBusyRetries':[],'tailReadSamplesNs':[],
            'masterModel':'controller knows the exact permanent crash; fixed detection wait = timeout.ms; FREEZE/REPAIR/OPEN over peer opcode38',
            'attempts':[]}
    try:
        assert args.controller==crashed,'controller must run on the crash target host'
        result['statesInitial']=discover(client,args.read_mode)
        value=bytes((i*17+3)%256 for i in range(args.value));keys=[]
        for i in range(30):
            key=f'warm-{i}'.encode();keys.append(key)
            r,_,_=set_retry(client,0,100+i,key,value,1);assert r['status']==0,'warmup failed'
        begin=time.perf_counter_ns()
        for i in range(128):
            key=f'pre-{i}'.encode();keys.append(key)
            r,busy,took=set_retry(client,0,1000+i,key,value,1)
            assert r['status']==0 and r['epoch']==1,'normal SET failed'
            result['normalSamplesNs'].append(took);result['normalBusyRetries'].append(busy)
        result['normalElapsedNs']=time.perf_counter_ns()-begin
        result.update(normalP50Ns=quantile(result['normalSamplesNs'],.5),normalP99Ns=quantile(result['normalSamplesNs'],.99),normalBusyTotal=sum(result['normalBusyRetries']))
        head_read=get(client,0,keys[-1],1);result['headReadStatus']=head_read['status'];result['headReadDetail']=head_read['detail']
        if args.read_mode=='TAIL_ONLY':
            assert head_read['status']==4 and head_read['detail']=='redirect-node=2','head GET must redirect to tail'
        for i in range(128):
            t=time.perf_counter_ns();r=get(client,2,keys[30+i],1);result['tailReadSamplesNs'].append(time.perf_counter_ns()-t)
            assert r['status']==0 and r['value']==value,'tail GET failed'
        result.update(tailReadP50Ns=quantile(result['tailReadSamplesNs'],.5),tailReadP99Ns=quantile(result['tailReadSamplesNs'],.99))
        result['preCrashVerifiedCopies']=verify(client,[0,1,2],keys,value)
        # 로그 digest(SHA-256)는 master의 복구 검증 수단이다. 첫 호출의 JCA 초기화·JIT 비용이 failover에 섞이지 않도록 crash 전에 한 번 계산해 둔다.
        t=time.perf_counter_ns();result['preCrashDigestsEqual']=len({digest(client,n) for n in range(3)})==1;result['preCrashDigestNs']=time.perf_counter_ns()-t
        assert result['preCrashDigestsEqual'],'logs differ before crash'
        result['statesBefore']=states(client)
        assert all(s['logLength']==s['applied']==s['acknowledged']==158 for s in result['statesBefore']),'chain not settled before crash'
        pid=int(pb_control.PID.read_text());assert pb_control.alive(pid),'dedicated JVM identity check failed'
        result['faultCallBeginNs']=time.perf_counter_ns();os.kill(pid,signal.SIGKILL);result['faultCallReturnNs']=time.perf_counter_ns()
        survivors=failover(client,result,crashed,2,args.timeout)
        new_head,new_tail=survivors[0],survivors[-1]
        attempt={'node':new_head,'startNs':time.perf_counter_ns()}
        r,busy,_=set_retry(client,new_head,100000,b'post-first',value,2)
        attempt.update(status=r['status'],responseEpoch=r['epoch'],busyRetries=busy,endNs=time.perf_counter_ns());result['attempts'].append(attempt)
        assert r['status']==0 and r['epoch']==2,'first write after repair failed'
        result.update(newHead=new_head,newTail=new_tail,newEpoch=2,firstNewHeadOkNs=attempt['endNs'])
        result['failoverNs']=result['firstNewHeadOkNs']-result['faultCallBeginNs']
        result['detectWaitNs']=result['masterDetectedNs']-result['faultCallBeginNs']
        result['freezeNs']=result['masterFrozenNs']-result['masterDetectedNs']
        result['repairNs']=result['masterRepairedNs']-result['masterFrozenNs']
        result['repairControlNs']=result['masterRepairControlNs']-result['masterFrozenNs'];result['repairPollNs']=result['masterRepairPolledNs']-result['masterRepairControlNs']
        result['repairDigestNs']=result['masterRepairedNs']-result['masterRepairPolledNs']
        result['openNs']=result['masterOpenedNs']-result['masterRepairedNs']
        result['firstWriteNs']=result['firstNewHeadOkNs']-result['masterOpenedNs']
        result['postCrashVerifiedCopies']=verify(client,survivors,keys+[b'post-first'],value)
        result['statesAfter']=states(client,survivors)
        for s in result['statesAfter']:
            assert s['epoch']==2 and s['state']=='RUNNING' and s['chain']==survivors and s['readyNs']>=s['detectedNs']>=0
        result['promotionNs']=max(s['readyNs']-s['detectedNs'] for s in result['statesAfter'])
        result['preservedSuccesses']=len(keys)
        result['staleEpochStatus']=set_retry(client,new_head,100001,b'stale',value,1)[0]['status'];assert result['staleEpochStatus']==5
        other=[n for n in survivors if n!=new_head][0]
        r=client.set(other,100002,b'non-head',value,2);result['nonHeadWriteStatus']=r['status'];result['nonHeadWriteDetail']=r['detail']
        assert r['status']==4 and r['detail']==f'redirect-node={new_head}'
        r=get(client,new_tail,b'post-first',2);assert r['status']==0 and r['value']==value,'new tail read failed'
        result['ok']=True
    except Exception as e:
        result.update(ok=False,error=repr(e));raise
    finally:
        client.close();Path(args.out).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('ok','position','valueBytes','timeoutMs','repeat','normalP99Ns','normalBusyTotal','tailReadP99Ns','failoverNs','promotionNs')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hosts',required=True);p.add_argument('--out')
    p.add_argument('--discover',action='store_true');p.add_argument('--controller',type=int);p.add_argument('--read-mode',default='TAIL_ONLY',choices=['TAIL_ONLY','CRAQ'])
    p.add_argument('--position',choices=['head','middle','tail'],default='head');p.add_argument('--value',type=int,default=64)
    p.add_argument('--timeout',type=int,default=500);p.add_argument('--repeat',type=int,default=1);p.add_argument('--retry',type=int,default=200)
    args=p.parse_args()
    if args.discover:
        c=Client(args.hosts.split(','))
        try:
            print(json.dumps({'states':discover(c,args.read_mode)}))
        finally:
            c.close()
    else:
        run(args)
