"""Rosé 단일 파티션 변형 실서버 실험. controller는 Primary 호스트(node0)에서 실행하며 정확한 Primary crash를 아는 외부 master 역할을 맡는다.
L별 동시 8 client 쓰기 지연·backpressure·peakLag와, 쓰기 진행 중 Primary crash 후 OK 받은 쓰기의 유실 수(상한 L)를 측정한다."""
import argparse
import json
import os
from pathlib import Path
import signal
import struct
import threading
import time
import uuid
from pb_bench import Client,quantile
import pb_control

BUSY=6
THREADS=8


def freeze_body(epoch):
    return struct.pack('!BQ',0,epoch)


def repair_body(epoch,primary,survivors):
    return struct.pack('!BQQI',1,epoch,primary,len(survivors))+b''.join(struct.pack('!Q',s) for s in survivors)


def open_body(epoch,length):
    return struct.pack('!BQI',2,epoch,length)


def control(client,node,body):
    r=client.request(node,38,body,peer=True)
    if r['status']!=0:
        raise RuntimeError(f"master control rejected on node {node}: {r['status']} {r['detail']}")
    return r['detail']


def digest(client,node):
    r=client.request(node,37,b'',peer=True);assert r['status']==0 and len(r['value'])==36;return r['value']


def states(client,nodes=(0,1,2)):
    return [client.status(i) for i in nodes]


def discover(client,limit,budget=25):
    end=time.monotonic()+budget
    while time.monotonic()<end:
        try:
            s=states(client)
            if all(x['protocol']=='ROSE' and x['epoch']==1 and x['state']=='RUNNING' and x['primary']==0 and x['limit']==limit for x in s):
                return s
        except (OSError,EOFError):
            pass
        time.sleep(.05)
    raise RuntimeError('rose cluster did not reach RUNNING')


def set_retry(client,node,operation,key,value,epoch,budget=3.0):
    start=time.perf_counter_ns();end=time.monotonic()+budget;busy=0
    while True:
        r=client.set(node,operation,key,value,epoch)
        if r['status']!=BUSY or time.monotonic()>=end:
            return r,busy,time.perf_counter_ns()-start
        busy+=1;time.sleep(.0005)


class Writer(threading.Thread):
    """closed-loop client 하나. OK를 받은 key만 ok 목록에 넣고, 응답 없이 끊긴 요청은 unknown으로 둔다."""
    def __init__(self,hosts,index,prefix,count,node,epoch,stop=None):
        super().__init__(daemon=True);self.client=Client(hosts);self.index=index;self.prefix=prefix;self.count=count;self.node=node;self.epoch=epoch
        self.stop=stop;self.samples=[];self.busy=0;self.ok=[];self.unknown=None;self.error=None
    def run(self):
        try:
            i=0
            while i<self.count and (self.stop is None or not self.stop.is_set()):
                key=f'{self.prefix}{self.index}-{i}'.encode();op=self.index*1_000_000+(1 if self.prefix=='n' else 500_000)+i
                try:
                    r,b,took=set_retry(self.client,self.node,op,key,VALUE,self.epoch)
                except (OSError,EOFError):
                    self.unknown=key;break
                if r['status']!=0:
                    self.error=f"write status {r['status']} {r['detail']}";break
                self.samples.append(took);self.busy+=b;self.ok.append(key);i+=1
        finally:
            self.client.close()


VALUE=b''


def run(args):
    global VALUE
    VALUE=bytes((i*17+3)%256 for i in range(args.value))
    hosts=args.hosts.split(',');client=Client(hosts)
    result={'schema':'a1-real-rose-1','protocol':'ROSE','limit':args.limit,'runId':uuid.uuid4().hex,'utcEpochMs':int(time.time()*1000),
            'timeDomain':'wall-clock','durationUnit':'ns','valueBytes':args.value,'timeoutMs':args.timeout,'repeat':args.repeat,'retryMs':args.retry,
            'warmupRequests':30,'normalThreads':THREADS,'normalWritesPerThread':64,'crashThreads':THREADS,'crashMinOkPerThread':32,
            'clientPlacement':'original-primary-host','masterModel':'controller knows the exact permanent Primary crash; fixed detection wait = timeout.ms; longest applied prefix becomes Primary'}
    try:
        assert args.controller==0,'controller must run on the original Primary host'
        result['statesInitial']=discover(client,args.limit)
        for i in range(30):
            r,_,_=set_retry(client,0,100+i,f'warm-{i}'.encode(),VALUE,1);assert r['status']==0,'warmup failed'
        result['statesWarm']=states(client)
        writers=[Writer(hosts,i,'n',64,0,1) for i in range(THREADS)];begin=time.perf_counter_ns()
        for w in writers:w.start()
        for w in writers:w.join()
        elapsed=time.perf_counter_ns()-begin
        assert all(w.error is None and w.unknown is None and len(w.ok)==64 for w in writers),[w.error for w in writers]
        samples=[s for w in writers for s in w.samples]
        result['normal']={'samplesNs':samples,'p50Ns':quantile(samples,.5),'p99Ns':quantile(samples,.99),'elapsedNs':elapsed,'busyRetries':sum(w.busy for w in writers),
                          'writes':len(samples),'opsPerSecond':len(samples)*1e9/elapsed}
        normal_keys=[k for w in writers for k in w.ok]
        settle=time.monotonic()+10
        while not all(s['applied']==s['logLength']==30+THREADS*64 for s in states(client)):
            assert time.monotonic()<settle,'normal phase did not settle';time.sleep(.01)
        result['statesNormal']=states(client);result['normalVerifiedCopies']=verify(client,[0,1,2],normal_keys)
        # 로그 digest의 JCA 초기화·JIT 비용이 failover에 섞이지 않도록 crash 전에 한 번 계산한다.
        t=time.perf_counter_ns();result['preCrashDigestsEqual']=len({digest(client,n) for n in range(3)})==1;result['preCrashDigestNs']=time.perf_counter_ns()-t
        assert result['preCrashDigestsEqual'],'logs differ before crash'
        stop=threading.Event();crash_writers=[Writer(hosts,i,'c',100000,0,1,stop) for i in range(THREADS)]
        for w in crash_writers:w.start()
        wait=time.monotonic()+20
        while not all(len(w.ok)>=32 for w in crash_writers):
            assert time.monotonic()<wait and all(w.error is None for w in crash_writers),'crash phase writers stalled';time.sleep(.001)
        pid=int(pb_control.PID.read_text());assert pb_control.alive(pid),'dedicated JVM identity check failed'
        result['faultCallBeginNs']=time.perf_counter_ns();os.kill(pid,signal.SIGKILL);result['faultCallReturnNs']=time.perf_counter_ns()
        stop.set()
        for w in crash_writers:w.join(10)
        ok_keys=[k for w in crash_writers for k in w.ok];unknown=[w.unknown for w in crash_writers if w.unknown is not None]
        result['crashOkWrites']=len(ok_keys);result['crashUnknownWrites']=len(unknown);result['crashWriterErrors']=[w.error for w in crash_writers if w.error]
        survivors=[1,2];time.sleep(args.timeout/1000);result['masterDetectedNs']=time.perf_counter_ns()
        result['freezeReplies']=[control(client,n,freeze_body(2)) for n in survivors];result['masterFrozenNs']=time.perf_counter_ns()
        frozen=states(client,survivors);result['statesFrozen']=frozen
        primary=1 if frozen[0]['applied']>=frozen[1]['applied'] else 2
        for n in survivors:control(client,n,repair_body(2,primary,survivors))
        end=time.monotonic()+10;length=None
        while time.monotonic()<end:
            s=states(client,survivors);p=next(x for x in s if x['node']==primary);length=p['logLength']
            if all(x['state']=='REPAIRING' and x['epoch']==2 and x['primary']==primary and x['logLength']==x['applied']==length for x in s) and p['minAck']==length:
                break
            time.sleep(.005)
        else:
            raise RuntimeError('rose repair did not converge')
        assert len({digest(client,n) for n in survivors})==1,'survivor logs differ after repair'
        result['masterRepairedNs']=time.perf_counter_ns();result['repairedLength']=length
        for n in survivors:control(client,n,open_body(2,length))
        result['masterOpenedNs']=time.perf_counter_ns()
        r,busy,_=set_retry(client,primary,9_000_000,b'post-first',VALUE,2);assert r['status']==0 and r['epoch']==2,'first write after repair failed'
        result.update(newPrimary=primary,newEpoch=2,firstNewPrimaryOkNs=time.perf_counter_ns())
        result['failoverNs']=result['firstNewPrimaryOkNs']-result['faultCallBeginNs']
        result['detectWaitNs']=result['masterDetectedNs']-result['faultCallBeginNs'];result['freezeNs']=result['masterFrozenNs']-result['masterDetectedNs']
        result['repairNs']=result['masterRepairedNs']-result['masterFrozenNs'];result['openNs']=result['masterOpenedNs']-result['masterRepairedNs']
        lost=[k.decode() for k in ok_keys if client.local(primary,k)['status']!=0]
        unknown_kept=sum(1 for k in unknown if client.local(primary,k)['status']==0)
        result['lostOkWrites']=len(lost);result['lostOkKeys']=lost;result['unknownWritesKept']=unknown_kept
        if args.limit>=0:
            assert len(lost)<=args.limit,f'lost {len(lost)} successful writes exceeds bound L={args.limit}'
        kept=[k for k in ok_keys if k.decode() not in lost]
        result['postCrashVerifiedCopies']=verify(client,survivors,normal_keys+kept+[b'post-first'])
        result['statesAfter']=states(client,survivors)
        assert all(x['epoch']==2 and x['state']=='RUNNING' and x['primary']==primary for x in result['statesAfter'])
        result['promotionNs']=max(x['readyNs']-x['detectedNs'] for x in result['statesAfter'])
        result['staleEpochStatus']=set_retry(client,primary,9_000_001,b'stale',VALUE,1)[0]['status'];assert result['staleEpochStatus']==5
        other=[n for n in survivors if n!=primary][0];r=client.set(other,9_000_002,b'backup-direct',VALUE,2)
        result['backupWriteStatus']=r['status'];assert r['status']==4 and r['detail']==f'redirect-node={primary}'
        result['ok']=True
    except Exception as e:
        result.update(ok=False,error=repr(e));raise
    finally:
        client.close();Path(args.out).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('ok','limit','valueBytes','repeat','crashOkWrites','crashUnknownWrites','lostOkWrites','failoverNs','promotionNs') if k in result}
                     |{'normalP99Ms':result['normal']['p99Ns']/1e6,'opsPerSecond':result['normal']['opsPerSecond'],'peakLag':result['statesNormal'][0]['peakLag'],'backpressure':result['statesNormal'][0]['backpressureCount']}))


def verify(client,nodes,keys,budget=5):
    end=time.monotonic()+budget
    while True:
        good=all(client.local(n,k)['status']==0 and client.local(n,k)['value']==VALUE for n in nodes for k in keys)
        if good:
            return len(nodes)*len(keys)
        if time.monotonic()>=end:
            raise RuntimeError('successful SET value missing from local KV')
        time.sleep(.02)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hosts',required=True);p.add_argument('--out');p.add_argument('--discover',action='store_true')
    p.add_argument('--controller',type=int,default=0);p.add_argument('--limit',type=int,default=0);p.add_argument('--value',type=int,default=64)
    p.add_argument('--timeout',type=int,default=500);p.add_argument('--repeat',type=int,default=1);p.add_argument('--retry',type=int,default=200)
    args=p.parse_args()
    if args.discover:
        c=Client(args.hosts.split(','))
        try:
            print(json.dumps({'states':discover(c,args.limit)}))
        finally:
            c.close()
    else:
        run(args)
