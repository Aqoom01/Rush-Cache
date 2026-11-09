"""Chain TAIL_ONLY와 CRAQ 읽기 경로 비교. clean 읽기(노드별)·동시 쓰기 중 dirty 읽기·일관성(단조 증가)을 같은 부하로 측정한다. 장애는 주입하지 않는다."""
import argparse
import json
from pathlib import Path
import struct
import threading
import time
import uuid
from pb_bench import Client,quantile
from chain_bench import discover,get,set_retry,states,verify

READ_FIELDS=('localReads','queryAttempts','dirtyReads')


def read_follow(client,node,key,epoch):
    """첫 응답이 redirect면 대상 노드로 한 번 더 요청한다. 지연은 두 요청을 합친 client 관측값이다."""
    t=time.perf_counter_ns();r=get(client,node,key,epoch);hops=1
    if r['status']==4 and r['detail'].startswith('redirect-node='):
        r=get(client,int(r['detail'].split('=')[1]),key,epoch);hops=2
    return r,hops,time.perf_counter_ns()-t


def counters(client):
    return [{k:s[k] for k in READ_FIELDS} for s in states(client)]


def delta(before,after):
    return [{k:a[k]-b[k] for k in READ_FIELDS} for b,a in zip(before,after)]


def run(args):
    client=Client(args.hosts.split(','))
    result={'schema':'a1-real-craq-1','protocol':'CHAIN','readMode':args.read_mode,'runId':uuid.uuid4().hex,'utcEpochMs':int(time.time()*1000),
            'timeDomain':'wall-clock','durationUnit':'ns','valueBytes':args.value,'repeat':args.repeat,'retryMs':args.retry,'warmupRequests':30,
            'cleanReadsPerNode':128,'mixedWrites':128,'clientPlacement':'head-host','faultsInjected':False,'clean':{},'mixed':{}}
    try:
        result['statesInitial']=discover(client,args.read_mode)
        value=bytes((i*17+3)%256 for i in range(args.value));keys=[]
        for i in range(30):
            key=f'warm-{i}'.encode();keys.append(key)
            r,_,_=set_retry(client,0,100+i,key,value,1);assert r['status']==0,'warmup failed'
        end=time.monotonic()+5
        while not all(s['acknowledged']==30 for s in states(client)):
            assert time.monotonic()<end,'warmup did not settle';time.sleep(.01)
        before=counters(client);result['peerBefore']=[s['peerTx'] for s in states(client)]
        for node in range(3):
            samples=[];hops_total=0
            for i in range(128):
                r,hops,took=read_follow(client,node,keys[i%30],1)
                assert r['status']==0 and r['value']==value,f'clean read failed at node {node}'
                samples.append(took);hops_total+=hops
            result['clean'][str(node)]={'samplesNs':samples,'p50Ns':quantile(samples,.5),'p99Ns':quantile(samples,.99),'redirects':hops_total-128}
        after=counters(client);result['cleanCounterDelta']=delta(before,after)
        # mixed: writer updates one hot key at head while a reader polls the middle node. 값 앞 4byte가 쓰기 순번이다.
        reader=Client(args.hosts.split(','));reads=[];stop=threading.Event();errors=[]
        def read_loop():
            try:
                while not stop.is_set():
                    r,hops,took=read_follow(reader,1,b'hot',1)
                    if r['status']==1:
                        reads.append((took,hops,-1));continue
                    assert r['status']==0,f"mixed read status {r['status']}"
                    reads.append((took,hops,struct.unpack('!I',r['value'][:4])[0]))
            except Exception as e:
                errors.append(repr(e))
        t=threading.Thread(target=read_loop,daemon=True);t.start()
        writes=[];busy=0;begin=time.perf_counter_ns()
        for i in range(128):
            payload=struct.pack('!I',i+1)+value[4:]
            r,b,took=set_retry(client,0,5000+i,b'hot',payload,1);assert r['status']==0,'mixed write failed'
            writes.append(took);busy+=b
        elapsed=time.perf_counter_ns()-begin;stop.set();t.join(5);assert not errors,errors[0]
        settle=time.monotonic()+5
        while not all(s['acknowledged']==158 and s['pendingReads']==0 for s in states(client)):
            assert time.monotonic()<settle,'mixed phase did not settle';time.sleep(.01)
        mixed_after=counters(client);result['mixedCounterDelta']=delta(after,mixed_after);result['peerAfter']=[s['peerTx'] for s in states(client)]
        seqs=[s for _,_,s in reads];violations=sum(1 for a,b in zip(seqs,seqs[1:]) if b<a)
        result['mixed']={'writeSamplesNs':writes,'writeP50Ns':quantile(writes,.5),'writeP99Ns':quantile(writes,.99),'writeBusyRetries':busy,'elapsedNs':elapsed,
                         'readCount':len(reads),'readSamplesNs':[x[0] for x in reads],'readP50Ns':quantile([x[0] for x in reads],.5),'readP99Ns':quantile([x[0] for x in reads],.99),
                         'readRedirects':sum(x[1]-1 for x in reads),'readSequence':seqs,'monotonicViolations':violations,'maxSeq':max(seqs) if seqs else -1}
        assert violations==0,'read sequence went backwards'
        final=get(client,2,b'hot',1);assert final['status']==0 and struct.unpack('!I',final['value'][:4])[0]==128
        result['verifiedCopies']=verify(client,[0,1,2],keys,value)
        result['statesAfter']=states(client)
        result['ok']=True
    except Exception as e:
        result.update(ok=False,error=repr(e));raise
    finally:
        client.close();Path(args.out).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'ok':result['ok'],'readMode':args.read_mode,'valueBytes':args.value,'repeat':args.repeat,
                      'cleanP99Ms':{n:v['p99Ns']/1e6 for n,v in result['clean'].items()},'mixedReads':result['mixed']['readCount'],
                      'mixedDelta':result['mixedCounterDelta'],'mixedReadP99Ms':result['mixed']['readP99Ns']/1e6}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hosts',required=True);p.add_argument('--out',required=True)
    p.add_argument('--read-mode',default='TAIL_ONLY',choices=['TAIL_ONLY','CRAQ']);p.add_argument('--value',type=int,default=64)
    p.add_argument('--repeat',type=int,default=1);p.add_argument('--retry',type=int,default=200)
    run(p.parse_args())
