"""PC 조정기의 GO/RESTORED 신호로 Backup 장애 중 쓰기와 복구 후 동작을 관찰한다."""
import argparse
import json
from pathlib import Path
import platform
import time
import uuid
import pb_control
from pb_bench import Client
from pb_load import value_for

def validate(result):
    assert result["schema"]=="a1-real-pb-backup-fault-1" and result["ok"]
    kind=result["scenario"]
    assert kind in ("BASELINE","SLOW_BACKUP","ACK_BLOCK")
    assert result["firstStatus"]==(7 if kind=="ACK_BLOCK" else 0)
    initial=result["copiesAfterFirst"]
    assert len(initial)==3 and [r["node"] for r in initial]==[0,1,2]
    expected=[1,0,0] if kind=="ACK_BLOCK" else [0,0,0]
    assert [r["status"] for r in initial]==expected
    assert all(r["matches"] for r in initial if r["status"]==0)
    assert result["retryStatus"]==(0 if kind=="ACK_BLOCK" else None)
    assert result["followupStatus"]==0 and result["protocolProbeStatuses"]==[0,0]
    assert result["finalCopies"]==6 and result["missingCopies"]==result["mismatchedCopies"]==0
    assert all(state["epoch"]==1 for state in result["statesAfter"]),"unexpected promotion"
    assert result["firstResponseNs"]>=result["firstSentNs"]
    if kind=="ACK_BLOCK":
        assert result["deadlineMs"]==200 and result["holdMs"]==800
    elif kind=="SLOW_BACKUP":
        assert result["firstResponseNs"]-result["firstSentNs"]>=100_000_000,"pause did not overlap the request enough"

def run(args):
    result={"schema":"a1-real-pb-backup-fault-1","timeDomain":"wall-clock","durationUnit":"ns",
            "runId":uuid.uuid4().hex,"utcEpochMs":int(time.time()*1000),"scenario":args.scenario,
            "valueBytes":args.value,"repeat":args.repeat,"timeoutMs":500,"heartbeatMs":50,"tickMs":10,
            "clientPlacement":"original-primary-host","platform":platform.platform(),
            "deadlineMs":200 if args.scenario=="ACK_BLOCK" else 1000,
            "holdMs":800 if args.scenario=="ACK_BLOCK" else 200 if args.scenario=="SLOW_BACKUP" else 0,
            "settleAfterRestoreMs":2000}
    client=Client(args.hosts.split(","))
    primary=None
    try:
        primary=pb_control.start()
        end=time.monotonic()+20
        while True:
            try:
                states=[client.status(i) for i in range(3)]
                if all(s["epoch"]==1 for s in states) and all(s["heartbeatSeen"] for s in states[1:]):
                    break
            except (OSError,EOFError):
                pass
            if time.monotonic()>end:
                raise RuntimeError("backup fault readiness failed")
            time.sleep(.05)
        value=value_for(1000,args.value)
        for i in range(30):
            assert client.set(0,100+i,f"warm-{i}".encode(),value)["status"]==0
        result["statesBefore"]=states
        print(json.dumps({"event":"READY_FOR_FAULT"}),flush=True)
        assert input().strip()=="GO"
        result["firstSentNs"]=time.perf_counter_ns()
        first=client.set(0,1000,b"fault-key",value,1,result["deadlineMs"])
        result["firstResponseNs"]=time.perf_counter_ns()
        result["firstStatus"]=first["status"]
        result["copiesAfterFirst"]=[]
        for node in range(3):
            r=client.local(node,b"fault-key")
            result["copiesAfterFirst"].append({"node":node,"status":r["status"],"version":r["version"],
                                              "matches":r["status"]==0 and r["value"]==value})
        print(json.dumps({"event":"MEASURED","status":first["status"],"elapsedNs":result["firstResponseNs"]-result["firstSentNs"]}),flush=True)
        assert input().strip()=="RESTORED"
        result["restoreNotificationNs"]=time.perf_counter_ns()
        time.sleep(2)
        result["protocolProbeStatuses"]=[client.request(0,4)["status"]]
        time.sleep(.1)
        result["protocolProbeStatuses"].append(client.request(0,4)["status"])
        result["retryStatus"]=None
        if args.scenario=="ACK_BLOCK":
            start=time.perf_counter_ns()
            result["retryStatus"]=client.set(0,1000,b"fault-key",value,1,1000)["status"]
            result["retryElapsedNs"]=time.perf_counter_ns()-start
        result["followupStatus"]=client.set(0,1001,b"restored-next",value,1,1000)["status"]
        result["finalCopies"]=result["missingCopies"]=result["mismatchedCopies"]=0
        result["finalVersions"]=[]
        for node in range(3):
            for key in (b"fault-key",b"restored-next"):
                r=client.local(node,key)
                result["finalVersions"].append({"node":node,"key":key.decode(),"version":r["version"]})
                if r["status"]!=0:
                    result["missingCopies"]+=1
                elif r["value"]!=value:
                    result["mismatchedCopies"]+=1
                else:
                    result["finalCopies"]+=1
        result["statesAfter"]=[client.status(i) for i in range(3)]
        result["ok"]=True
        validate(result)
    except Exception as error:
        result.update(ok=False,error=repr(error))
        raise
    finally:
        client.close()
        if primary is not None and primary.poll() is None:
            primary.terminate()
            primary.wait(timeout=10)
        Path(args.out).write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({"event":"DONE","ok":True,"scenario":args.scenario,"valueBytes":args.value,"repeat":args.repeat,
                      "firstStatus":result["firstStatus"],"firstElapsedNs":result["firstResponseNs"]-result["firstSentNs"],
                      "retryStatus":result["retryStatus"]}),flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--hosts",required=True)
    p.add_argument("--out",required=True)
    p.add_argument("--scenario",choices=("BASELINE","SLOW_BACKUP","ACK_BLOCK"),required=True)
    p.add_argument("--value",type=int,required=True)
    p.add_argument("--repeat",type=int,required=True)
    run(p.parse_args())
