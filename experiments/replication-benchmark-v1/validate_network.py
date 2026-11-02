"""Independent stdlib verification of network-observation-v1 local evidence.
Hashes are integrity checks, not signatures. COMPLETE certifies storage only.
"""
from collections import Counter
import argparse
import hashlib
import json
from pathlib import Path

METRICS=set('BEGUN OK FAILED REJECTED CREATED CONNECT_ATTEMPTS CONNECTED CONNECT_FAILED CLOSED CLOSE_UNCONFIRMED REQUESTS_WRITTEN REPLIES_READ SENT_BYTES RECEIVED_BYTES WRITE_UNCERTAIN READ_ERRORS WRITE_ERRORS CROSS_STAGE ELAPSED_NS'.split())
STAGES='PREFLIGHT LOAD LOAD_DRAIN WARMUP WARMUP_DRAIN MEASURE FINAL_DRAIN CLEANUP'.split()
FILES={'network.json','logical.json','execution.json','summary.json'}
def check(ok,message):
    if not ok: raise ValueError(message)
def load(p):return json.loads(p.read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def total(network,frame,metric,from_node=None,to_node=None,kind=None):
    return sum(r['counts'][metric] for r in network['rows'] if r['key']['frame']==frame
        and (from_node is None or r['key']['from']==from_node) and (to_node is None or r['key']['to']==to_node)
        and (kind is None or r['key']['kind']==kind))
def inspect_network(n,qualified):
    check(isinstance(n['processInstanceId'],str) and n['processInstanceId'] and type(n['pid']) is int and n['pid']>=0,'process identity')
    check(n['startedNanos']<=n['finishedNanos'],'source clock')
    check(len(n['members'])==len(set(n['members'])) and all(type(i) is int and i>0 for i in n['members']),'membership')
    if qualified:
        check(n['sealed'] and not n['issues'] and n['pending']==n['pendingFrames']==n['activeConnections']==n['lateFinishes']==0,'capture incomplete')
    seen=set()
    for row in n['rows']:
        k,c=row['key'],row['counts'];key=json.dumps(k,sort_keys=True)
        check(key not in seen,'duplicate aggregate row');seen.add(key)
        check(set(c)==METRICS and all(type(v) is int and v>=0 for v in c.values()),'counter shape')
        check(c['BEGUN']==c['OK']+c['FAILED']+c['REJECTED'],'exchange/frame conservation')
        if not qualified:continue
        check(type(k['frame']) is bool and type(k['window']) is int and 0<=k['window']<=(n['finishedNanos']-n['startedNanos'])//1_000_000_000,'window')
        check(k['startedStage'] in STAGES and k['terminalStage'] in STAGES and STAGES.index(k['startedStage'])<=STAGES.index(k['terminalStage']),'stage order')
        check(k['from'] in [0]+n['members'] and k['to'] in [0]+n['members'] and 1<=k['kind']<=7,'endpoint/kind coverage')
        check(c['CROSS_STAGE']==(c['BEGUN'] if k['startedStage']!=k['terminalStage'] else 0),'cross-stage accounting')
        check(c['ELAPSED_NS']<=c['BEGUN']*(n['finishedNanos']-n['startedNanos']),'elapsed interval bound')
        check(c['WRITE_UNCERTAIN']==0 and c['CLOSE_UNCONFIRMED']==0,'uncertain byte/close coverage')
        if k['frame']:
            check(c['CREATED']==c['CONNECTED']==c['CONNECT_ATTEMPTS']==c['CLOSED']==c['REPLIES_READ']==c['RECEIVED_BYTES']==0,'frame/exchange conflation')
            check(c['REQUESTS_WRITTEN']==c['OK'],'completed frame')
        else:
            check(c['CREATED']==c['CLOSED'] and c['CONNECT_ATTEMPTS']==c['CONNECTED']+c['CONNECT_FAILED'],'connection conservation')
            check(c['CONNECTED']<=c['CREATED']<=c['BEGUN'] and c['REQUESTS_WRITTEN']<=c['CONNECTED'] and c['REPLIES_READ']<=c['REQUESTS_WRITTEN'],'exchange ordering')
    return len(n['rows'])

def verify(directory):
    root=Path(directory)
    check((root/'STARTED').is_file() and (root/'COMPLETE').is_file(),'missing completion')
    check((root/'COMPLETE').read_text(encoding='utf-8').strip()=='integrity_sha256='+sha(root/'integrity.json'),'integrity digest')
    integrity=load(root/'integrity.json');check(integrity['schema_version']=='network-integrity-v1','integrity schema')
    check(len(integrity['files'])==4 and {r['path'] for r in integrity['files']}==FILES,'file whitelist/duplicates')
    for f in integrity['files']:check(sha(root/f['path'])==f['sha256'],'data hash: '+f['path'])
    s,n,l,e=[load(root/name) for name in ['summary.json','network.json','logical.json','execution.json']]
    check(s['schema_version']=='network-observation-v1' and s['token']==n['token']==integrity['token'],'schema/token')
    check(s['validity']==integrity['validity'] and s['validity'] in {'valid','invalid'},'validity')
    valid=s['validity']=='valid';check(not s['issues'] if valid else bool(s['issues']),'issue qualification')
    rows=inspect_network(n,valid)
    check(s['process_count']==1 and s['nominal_period_ns']==1_000_000_000,'scope/period')
    check(s['sum_method']=='sender_frames_only_never_add_exchange_or_receive_copies','double-count policy')
    check(s['standard_M15_M16_M17']=='unsupported_until_declared_process_coverage_and_profile_integration','standard metric overclaim')
    check(s['tcp_ip_packets'] is None and s['remote_process_bytes'] is None and s['os_time_wait'] is None and s['preexisting_connections'] is None,'unsupported sources')
    check(s['connection_peak_method']=='observer_event_order_gauge_not_OS_peak','peak method')
    if not valid:return {'directory':str(root),'validity':'invalid','storage_verified':True,'qualified':False,'issues':s['issues']}
    r,p,t=e['result'],e['plan'],e['trace'];check(e['mode'] in {'open','closed'},'mode')
    check(s['run_id']==l['runId']==p['runId'] and s['load_slot']==l['loadSlot']==p['loadSlot'] and s['clock_id']==l['clockId'],'run/slot/clock')
    check(n['startedNanos']<=r['startedNanos']<=n['finishedNanos']<=r['finishedNanos'],'run/source clock scope')
    check(r['failure'] is None and l['failure'] is None,'execution/logical failure')
    if e['mode']=='closed':check(r['passed'] and r['workersTerminated'],'closed failed')
    else:check(all(r[k] for k in ['allArrivalsConsidered','collectionValid','scheduleQualified','serviceHealthy']) and r['outstandingCallbacks']==0,'open failed')
    check([x['stage'] for x in n['transitions']]==STAGES,'lifecycle coverage')
    last=n['startedNanos']
    for x in n['transitions']:
        check(last<=x['requestedNanos']<=x['observedNanos']<=n['finishedNanos'],'phase clock');last=x['requestedNanos']
    cohorts={};logical_total=0
    for row in l['counts']:
        key=row['phase'],row['operation'];check(key not in cohorts,'duplicate logical cohort')
        check(row['phase'] in {'LOAD','WARMUP','MEASURE'} and row['operation'] in {'GET','SET','DELETE'},'logical population')
        check(all(type(v) is int and v>=0 for v in row['tally'].values()),'logical count shape');cohorts[key]=row['tally']
    for phase in ['LOAD','WARMUP','MEASURE']:
        phase_total=0
        for op in ['GET','SET','DELETE']:
            actual=cohorts.get((phase,op),dict(success=0,errorResponse=0,noResponse=0,attempts=0,retried=0))
            wanted=r['phases'][phase]['byOperation'][op]
            for k in ['success','errorResponse','noResponse','attempts']:check(actual[k]==wanted[k],'logical cohort: '+k)
            check(actual['retried']==wanted['retriedLogical'],'retried logical')
            phase_total+=sum(actual[k] for k in ['success','errorResponse','noResponse'])
        check(phase_total==(t['keyCount'] if phase=='LOAD' else p['warmupRequests'] if phase=='WARMUP' else p['measuredRequests']),'plan count')
        logical_total+=phase_total
    final=r['finalClient']['byOperation']
    for op,c in final.items():check(c['admitted']==c['success']+c['errorResponse']+c['noResponse']+c['pending'] and c['pending']==0,'final client conservation')
    check(logical_total==s['logical_outcomes']==sum(c['admitted'] for c in final.values()),'logical total')
    check(s['sender_frames']==total(n,True,'OK') and s['sender_confirmed_bytes']==total(n,True,'SENT_BYTES') and s['sender_exact_bytes']==total(n,True,'SENT_BYTES'),'sender totals')
    check(s['initiator_exchanges']==total(n,False,'BEGUN') and s['connected']==total(n,False,'CONNECTED'),'exchange totals')
    check(total(n,True,'OK',from_node=0)>0 and total(n,True,'OK',from_node=0)==total(n,False,'REQUESTS_WRITTEN',from_node=0),'client frame/exchange')
    check(total(n,True,'SENT_BYTES',from_node=0)==total(n,False,'SENT_BYTES',from_node=0),'client byte cross-check')
    exact=total(n,False,'FAILED')==total(n,False,'REJECTED')==total(n,True,'FAILED')==0
    check(0<n['peakConnections']<=total(n,False,'CONNECTED') and 0<n['peakExchanges']<=total(n,False,'BEGUN'),'tracked peak bounds')
    check(s['flow_reconciliation']==('exact_error_free_flow' if exact else 'diagnostic_only_with_transport_failures'),'failure flow policy')
    if exact:
        check(total(n,True,'OK',to_node=0)==total(n,False,'REPLIES_READ',from_node=0),'reply frame cross-check')
        check(total(n,True,'SENT_BYTES',to_node=0)==total(n,False,'RECEIVED_BYTES',from_node=0),'reply byte cross-check')
    return {'directory':str(root),'validity':'valid','storage_verified':True,'qualified':True,'logical_outcomes':logical_total,'rows':rows,'processes':1,'transport_failures':total(n,False,'FAILED')}

def verify_burst(directory):
    p=Path(directory)/'burst.json';b=load(p);check(b['schema_version']=='network-burst-v1' and b['logical_queries']==4 and b['raw_batch_frames']==1,'burst schema/count')
    n=b['network'];inspect_network(n,True)
    check(total(n,False,'BEGUN',from_node=0,kind=7)==total(n,True,'OK',from_node=0,kind=7)==1,'batch actual frame')
    check(total(n,False,'OK',from_node=0,kind=4)==4,'read exchanges')
    return {'directory':str(directory),'sha256':sha(p),'logical_queries':4,'batch_frames':1,'verified':True}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('directories',type=Path,nargs='+');args=parser.parse_args()
    print(json.dumps([verify(p) for p in args.directories],indent=2))
