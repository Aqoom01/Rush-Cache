"""VR 정상 부하의 opcode별 전송 완료와 상태 전달 이유를 측정한다. 장애는 주입하지 않는다. --mode는 서버 vr.mode와 대조만 한다."""
import argparse
import json
from pathlib import Path
import time
import uuid
from pb_bench import Client,quantile
from election_bench import discover,verify


def states(client):
    return [client.status(i) for i in range(3)]


def settle(client,entries,budget=8,mode=None):
    end=time.monotonic()+budget;previous=None;stable=0
    while time.monotonic()<end:
        snapshot=states(client)
        assert all(s['epoch']==0 and s['state']=='NORMAL' for s in snapshot),'unexpected view change'
        assert mode is None or all(s.get('vrMode')==mode for s in snapshot),'server vr.mode differs from requested mode'
        data=sum(v['bytes'] for s in snapshot for opcode,v in s['peerTx']['opcodes'].items() if opcode in ('48','49','53','54'))
        complete=all(s['logLength']==s['commit']==s['applied']==entries for s in snapshot)
        stable=stable+1 if complete and data==previous else 0
        if stable>=2:
            return snapshot
        previous=data;time.sleep(.15)
    raise RuntimeError('normal traffic did not settle')


def run(args):
    client=Client(args.hosts.split(','))
    result={'schema':'a1-real-vr-traffic-2','protocol':'VR','vrMode':args.mode,'runId':uuid.uuid4().hex,
            'timeDomain':'wall-clock','durationUnit':'ns','utcEpochMs':int(time.time()*1000),
            'valueBytes':args.value,'timeoutMs':args.timeout,'repeat':args.repeat,'warmupRequests':30,
            'normalRequests':128,'normalConcurrency':1,'heartbeatMs':50,'tickMs':10,
            'clientPlacement':'original-primary-host','faultsInjected':False,'normalSamplesNs':[],
            'counterScope':'successful local TCP frame writes; length/header included; no TCP/IP overhead or remote receipt guarantee'}
    try:
        leader,epoch,_=discover(client,'VR');assert (leader,epoch)==(0,0)
        value=bytes((i*17+3)%256 for i in range(args.value));keys=[]
        for i in range(30):
            key=f'warm-{i}'.encode();keys.append(key)
            assert client.set(0,100+i,key,value,0)['status']==0
        result['before']=settle(client,30,mode=args.mode)
        begin=time.perf_counter_ns()
        for i in range(128):
            key=f'pre-{i}'.encode();keys.append(key);t=time.perf_counter_ns()
            r=client.set(0,1000+i,key,value,0)
            result['normalSamplesNs'].append(time.perf_counter_ns()-t)
            assert r['status']==0 and r['epoch']==0,'normal SET failed'
        elapsed=time.perf_counter_ns()-begin
        result.update(normalElapsedNs=elapsed,normalP50Ns=quantile(result['normalSamplesNs'],.5),
                      normalP99Ns=quantile(result['normalSamplesNs'],.99))
        result['immediateAfter']=states(client)
        result['verifiedCopies']=verify(client,[0,1,2],keys,value)
        result['settledAfter']=settle(client,158,mode=args.mode)
        result['ok']=True
    except Exception as e:
        result.update(ok=False,error=repr(e));raise
    finally:
        client.close();Path(args.out).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('ok','vrMode','valueBytes','timeoutMs','repeat','normalP99Ns','verifiedCopies')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hosts',required=True);p.add_argument('--out',required=True)
    p.add_argument('--value',type=int,default=64);p.add_argument('--timeout',type=int,default=500)
    p.add_argument('--repeat',type=int,default=1);p.add_argument('--mode',default='INCREMENTAL',choices=['INCREMENTAL','INCREMENTAL_CATCHUP'])
    run(p.parse_args())
