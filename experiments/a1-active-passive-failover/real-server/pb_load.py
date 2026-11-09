"""중단 없는 closed-loop SET 중 client 요청 전송 직후 Primary JVM을 종료한다."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import signal
import struct
import time
import uuid
import pb_control
from pb_bench import Client,quantile

def classify_attempt(attempt,fault):
    """OK 응답 완료 시각/epoch로 구분한다. 종료 후 old Primary OK를 새 Primary 복구로 세지 않는다."""
    if attempt.get("status")!=0:
        return "NOT_OK"
    if attempt["endNs"]<fault:
        return "PRE_FAULT_OK"
    if attempt["node"]==1 and attempt["responseEpoch"]==2:
        return "NEW_PRIMARY_OK"
    return "OLD_PRIMARY_LATE_OK"

def value_for(operation,size):
    assert size>=8
    return struct.pack("!Q",operation)+bytes((i*17+3)%256 for i in range(size-8))

def validate(result):
    assert result["schema"]=="a1-real-pb-load-1" and result["ok"]
    attempts=result["attempts"]
    fault=result["faultCallBeginNs"]
    candidate=[a for a in attempts if a["logical"]==result["inflightLogical"]]
    assert candidate and candidate[0]["node"]==0
    assert candidate[0]["startNs"]<=candidate[0]["sentNs"]<=fault<=result["faultCallReturnNs"]<=candidate[0]["endNs"]
    pre=[a for a in attempts if classify_attempt(a,fault)=="PRE_FAULT_OK"]
    new=[a for a in attempts if classify_attempt(a,fault)=="NEW_PRIMARY_OK"]
    late=[a for a in attempts if classify_attempt(a,fault)=="OLD_PRIMARY_LATE_OK"]
    assert len(pre)==result["targetCount"]==result["preFaultOk"]
    assert len(new)==result["targetCount"]==result["newPrimaryOk"]
    assert len(late)==result["oldPrimaryLateOk"]
    successes=pre+new+late
    assert len({a["logical"] for a in successes})==len(successes)==result["successes"]==result["preservedSuccesses"]
    assert result["verifiedCopies"]==2*len(successes) and result["missingCopies"]==result["mismatchedCopies"]==0
    assert result["failoverNs"]==new[0]["endNs"]-fault>0
    assert result["inflightFirstOutcome"]==candidate[0]["status"]
    assert candidate[-1]["status"]==0 and result["inflightFinalStatus"]==0
    assert result["unresolvedLogical"]==0
    assert all(a["startNs"]<=a["endNs"] for a in attempts)
    assert result["staleEpochStatus"]==5 and result["backupWriteStatus"]==4

def run(args):
    result={"schema":"a1-real-pb-load-1","timeDomain":"wall-clock","durationUnit":"ns","runId":uuid.uuid4().hex,
            "utcEpochMs":int(time.time()*1000),"valueBytes":args.value,"timeoutMs":args.timeout,"repeat":args.repeat,
            "targetCount":args.count,"heartbeatMs":50,"tickMs":10,"clientPlacement":"original-primary-host",
            "concurrency":1,"retryIntervalMs":10,"attemptDeadlineMs":1000,"platform":platform.platform(),
            "faultStage":"after client sendall, before reading its response; server acceptance/commit stage is not forced",
            "attempts":[],"logical":[],"candidateReplicaBeforeRetry":[],"preFaultOk":0,"newPrimaryOk":0,"oldPrimaryLateOk":0}
    hosts=args.hosts.split(",")
    primary=None
    current=None
    enabled=False
    def sent(node,opcode,peer,sequence):
        if not enabled or peer or opcode!=2:
            return
        current["sentNs"]=time.perf_counter_ns()
        if "faultCallBeginNs" not in result and result["preFaultOk"]>=args.count and node==0:
            result["inflightLogical"]=current["logical"]
            result["faultCallBeginNs"]=time.perf_counter_ns()
            primary.send_signal(signal.SIGKILL)
            result["faultCallReturnNs"]=time.perf_counter_ns()
    client=Client(hosts,sent)
    retained={}
    try:
        primary=pb_control.start()
        limit=time.monotonic()+20
        while True:
            try:
                states=[client.status(i) for i in range(3)]
                if all(s["epoch"]==1 for s in states) and all(s["heartbeatSeen"] for s in states[1:]):
                    break
            except (OSError,EOFError):
                pass
            if time.monotonic()>limit:
                raise RuntimeError("load startup readiness failed")
            time.sleep(.05)
        for i in range(30):
            r=client.set(0,100+i,f"warm-{i}".encode(),value_for(i,args.value))
            assert r["status"]==0
        result["statesBefore"]=states
        enabled=True
        route=0
        epoch=1
        operation=1000
        inspected=False
        while result["newPrimaryOk"]<args.count:
            key=f"load-{operation}".encode()
            value=value_for(operation,args.value)
            logical_start=time.perf_counter_ns()
            retry=0
            while True:
                current={"logical":operation,"attempt":retry,"node":route,"epoch":epoch,"startNs":time.perf_counter_ns()}
                try:
                    response=client.set(route,operation,key,value,epoch)
                    current.update(status=response["status"],responseEpoch=response["epoch"],detail=response["detail"],endNs=time.perf_counter_ns())
                    epoch=max(epoch,response["epoch"])
                except (OSError,EOFError) as error:
                    current.update(status="CLIENT_FAILURE",error=type(error).__name__,endNs=time.perf_counter_ns())
                result["attempts"].append(current)
                if current["status"]==0:
                    retained[operation]=(key,value)
                    if "faultCallBeginNs" not in result:
                        result["preFaultOk"]+=1
                    else:
                        classification=classify_attempt(current,result["faultCallBeginNs"])
                        if classification=="NEW_PRIMARY_OK":
                            result["newPrimaryOk"]+=1
                            result.setdefault("firstNewPrimaryOkNs",current["endNs"])
                        elif classification=="OLD_PRIMARY_LATE_OK":
                            result["oldPrimaryLateOk"]+=1
                        else:
                            raise RuntimeError("unexpected success classification")
                    result["logical"].append({"operation":operation,"startNs":logical_start,"okNs":current["endNs"],
                                              "attempts":retry+1,"valueSha256":hashlib.sha256(value).hexdigest()})
                    break
                if "faultCallBeginNs" not in result:
                    raise RuntimeError("unexpected pre-fault failure")
                if operation==result["inflightLogical"] and not inspected:
                    for node in (1,2):
                        r=client.local(node,key)
                        result["candidateReplicaBeforeRetry"].append({"node":node,"observedNs":time.perf_counter_ns(),
                            "status":r["status"],"version":r["version"],"matches":r["status"]==0 and r["value"]==value})
                    inspected=True
                if time.perf_counter_ns()-result["faultCallBeginNs"]>15_000_000_000:
                    raise RuntimeError("new Primary not available within 15 seconds")
                if current["status"]=="CLIENT_FAILURE":
                    route=(route+1)%3
                elif current.get("detail","").startswith("redirect-node="):
                    route=int(current["detail"].split("=")[1])
                elif current["status"] not in (5,8,6):
                    route=(route+1)%3
                retry+=1
                time.sleep(.01)
            operation+=1
        enabled=False
        primary.wait(timeout=5)
        result["statesAfter"]=[client.status(i) for i in (1,2)]
        state=result["statesAfter"][0]
        assert state["node"]==1 and state["epoch"]==2 and state["readyNs"]>=state["detectedNs"]>=0
        result["promotionNs"]=state["readyNs"]-state["detectedNs"]
        result["failoverNs"]=result["firstNewPrimaryOkNs"]-result["faultCallBeginNs"]
        candidate=[a for a in result["attempts"] if a["logical"]==result["inflightLogical"]]
        result["inflightFirstOutcome"]=candidate[0]["status"]
        result["inflightFinalStatus"]=candidate[-1]["status"]
        result["inflightAttemptCount"]=len(candidate)
        result["successes"]=len(retained)
        result["verifiedCopies"]=0
        result["missingCopies"]=0
        result["mismatchedCopies"]=0
        for node in (1,2):
            for key,value in retained.values():
                response=client.local(node,key)
                if response["status"]!=0:
                    result["missingCopies"]+=1
                elif response["value"]!=value:
                    result["mismatchedCopies"]+=1
                else:
                    result["verifiedCopies"]+=1
        result["preservedSuccesses"]=len(retained) if not (result["missingCopies"] or result["mismatchedCopies"]) else 0
        result["unresolvedLogical"]=0
        ok_times=sorted(a["endNs"] for a in result["attempts"] if a["status"]==0)
        result["maxSuccessGapNs"]=max(b-a for a,b in zip(ok_times,ok_times[1:]))
        result["maxLogicalLatencyNs"]=max(r["okNs"]-r["startNs"] for r in result["logical"])
        result["preP99Ns"]=quantile([a["endNs"]-a["startNs"] for a in result["attempts"] if classify_attempt(a,result["faultCallBeginNs"])=="PRE_FAULT_OK"],.99)
        result["newPrimaryP99Ns"]=quantile([a["endNs"]-a["startNs"] for a in result["attempts"] if classify_attempt(a,result["faultCallBeginNs"])=="NEW_PRIMARY_OK"],.99)
        result["staleEpochStatus"]=client.set(1,999001,b"stale-load",value_for(999001,args.value),1)["status"]
        backup=client.set(2,999002,b"direct-load",value_for(999002,args.value),2)
        result["backupWriteStatus"]=backup["status"]
        assert backup["detail"]=="redirect-node=1"
        result["ok"]=True
        validate(result)
    except Exception as error:
        result.update(ok=False,error=repr(error))
        raise
    finally:
        enabled=False
        client.close()
        if primary is not None and primary.poll() is None:
            primary.terminate()
            primary.wait(timeout=10)
        Path(args.out).write_text(json.dumps(result,indent=2)+"\n")
    fields=("ok","valueBytes","timeoutMs","repeat","preFaultOk","newPrimaryOk","oldPrimaryLateOk","inflightFirstOutcome","inflightAttemptCount","failoverNs","preservedSuccesses")
    print(json.dumps({k:result[k] for k in fields}))

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--hosts",required=True)
    parser.add_argument("--out",required=True)
    parser.add_argument("--value",type=int,default=64)
    parser.add_argument("--timeout",type=int,default=500)
    parser.add_argument("--repeat",type=int,default=1)
    parser.add_argument("--count",type=int,default=300)
    run(parser.parse_args())
