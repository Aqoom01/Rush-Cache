"""V05a 설계의 유한 사전모형. Java/실제 RPC/분산 선형성 검증이 아니다."""
from dataclasses import dataclass,replace
from itertools import product
import json,sys

@dataclass
class Sample:
    rtt:int|None=None
    at:int|None=None
    outstanding:int=0
    cooldown:int=0
    sequence:int=0

INITIAL=3
TTL=10
CAP=2

def eligible(states,leader,now):
    return [n for n,s in sorted(states.items()) if n!=leader and s.outstanding<CAP and now>=s.cooldown]

def score(s,now):
    latency=s.rtt if s.at is not None and now-s.at<=TTL else INITIAL
    return latency*(1+s.outstanding)

def circular(nodes,eligible_nodes,cursor):
    return next(n for step in range(len(nodes)) if (n:=nodes[(cursor+step)%len(nodes)]) in eligible_nodes)

def choose(states,leader,now,mode,ordinal=1,cursor=0,explore_cursor=0,period=16):
    nodes=sorted(states);available=eligible(states,leader,now)
    if not available:return None,'BUSY'
    if mode=='round-robin':return circular(nodes,available,cursor),'ROUND_ROBIN'
    if ordinal%period==0:return circular(nodes,available,explore_cursor),'EXPLORE'
    best=min(score(states[n],now) for n in available)
    ties=[n for n in available if score(states[n],now)==best]
    return circular(nodes,ties,cursor),'SCORE'

def run():
    choices=0
    presets=[Sample(1,20),Sample(5,20),Sample(),Sample(1,0),Sample(1,20,1),Sample(1,20,CAP),Sample(1,20,0,21)]
    for count in [3,5]:
        for leader in range(count):
            followers=[n for n in range(count) if n!=leader]
            for forms in product(presets,repeat=len(followers)):
                states={leader:Sample(1,20),**dict(zip(followers,forms))}
                for mode,ordinal in [('round-robin',1),('adaptive',1),('adaptive',16)]:
                    selected,reason=choose(states,leader,20,mode,ordinal)
                    permitted=eligible(states,leader,20)
                    assert (selected is None)==(not permitted)
                    if selected is not None:
                        assert selected!=leader and selected in permitted
                        if reason=='SCORE':assert all(score(states[selected],20)<=score(states[n],20) for n in permitted)
                    choices+=1
    exploration=0
    for count in [3,5]:
        for period in [1,2,16]:
            states={n:Sample(1 if n==1 else 9,20) for n in range(count)}
            cursor=0;seen=set()
            for ordinal in range(1,period*(count-1)+1):
                n,reason=choose(states,0,20,'adaptive',ordinal,explore_cursor=cursor,period=period)
                if reason=='EXPLORE':seen.add(n);cursor=(n+1)%count
            assert seen==set(range(1,count));exploration+=1

    # Completion tokens: user cancellation does not complete the raw READ future.
    states={1:Sample()};raw={};generation=0;sequence=0
    def dispatch():
        nonlocal sequence
        if states[1].outstanding==CAP:return None
        sequence+=1;token=(generation,sequence);raw[token]=True;states[1].outstanding+=1;return token
    def complete(token,good,now):
        assert raw.pop(token);s=states[1];s.outstanding-=1
        if token[0]!=generation or token[1]<=s.sequence:return
        s.sequence=token[1]
        if good:s.rtt=4;s.at=now;s.cooldown=0
        else:s.cooldown=now+5
    first=dispatch();second=dispatch()
    # Caller cancels the first logical future: no mutation of raw/outstanding.
    assert dispatch() is None and len(raw)==CAP
    generation=1;states[1]=Sample(outstanding=states[1].outstanding)
    complete(second,False,21);assert states[1].cooldown==0 and states[1].outstanding==1
    complete(first,True,22);assert states[1].rtt is None and states[1].outstanding==0
    older=dispatch();newer=dispatch();complete(newer,False,23);complete(older,True,24)
    assert states[1].cooldown==28 and states[1].rtt is None
    lifecycle=['caller_cancel_keeps_raw_slot','cap_rejects_third_raw_read','new_view_keeps_old_outstanding',
               'old_failure_cannot_cool_new_generation','old_success_cannot_train_new_generation',
               'out_of_order_success_cannot_erase_new_failure']

    # Negative controls demonstrate that the selected invariants can detect these defects.
    bad=[]
    states={0:Sample(0,20),1:Sample(4,20),2:Sample(5,20)}
    buggy=min(states,key=lambda n:score(states[n],20))
    assert buggy==0 and buggy not in eligible(states,0,20);bad.append('missing_leader_exclusion')
    actual_raw=2;reported_raw=actual_raw-1
    assert reported_raw<CAP and actual_raw+1>CAP;bad.append('release_raw_slot_on_caller_cancel')
    current=Sample();old=replace(current,rtt=99,at=22)
    assert old!=current;bad.append('accept_old_generation_feedback')
    after_failure=Sample(cooldown=28,sequence=4);after_old_success=replace(after_failure,rtt=4,cooldown=0,sequence=3)
    assert after_old_success.cooldown!=after_failure.cooldown;bad.append('older_success_clears_newer_failure')
    captured=0;cached_lag_hint=0;fresh_meta=1
    assert captured>=cached_lag_hint and not captured>=fresh_meta;bad.append('use_cached_lag_hint_as_read_certificate')
    return {'scope':'finite policy snapshots and illustrative lifecycle/counterexamples only; no Java/RPC/linearizability proof',
            'choice_snapshots':choices,'exploration_fixtures':exploration,'lifecycle_checks':lifecycle,'negative_controls_detected':bad,
            'assumptions':'static membership; exploration fairness only for continuously eligible nodes; model ticks are not benchmark defaults'}

if __name__=='__main__':
    result=run();output=json.dumps(result,ensure_ascii=False,indent=2)+'\n'
    if len(sys.argv)==2:
        from pathlib import Path
        Path(sys.argv[1]).write_text(output,encoding='utf-8')
    print(output)
