"""Backup2의 전용 JVM 일시정지 또는 Primary peer 목적지 ACK 경로만 잠시 차단한다."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid
import pb_control

def emit(event,**values):
    print(json.dumps({"event":event,**values}),flush=True)

def interrupted(signum,frame):
    raise InterruptedError(f"fault controller interrupted: {signum}")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--kind",choices=("SLOW_BACKUP","ACK_BLOCK"),required=True)
    p.add_argument("--primary",required=True)
    p.add_argument("--hold-ms",type=int,required=True)
    args=p.parse_args()
    assert 1<=args.hold_ms<=5000
    primary=str(ipaddress.IPv4Address(args.primary))
    config=dict(line.split("=",1) for line in (pb_control.ROOT/"node.properties").read_text().splitlines() if "=" in line)
    assert config["node.id"]=="2" and config["peer.0"]==primary
    pid=int(pb_control.PID.read_text())
    assert pb_control.alive(pid),"not the dedicated PB JVM"
    for kind in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):
        signal.signal(kind,interrupted)
    tag="a1-pb-ack-"+uuid.uuid4().hex
    rule=["-p","tcp","-d",primary+"/32","--dport","7001","-m","comment","--comment",tag,"-j","DROP"]
    def iptables(operation):
        return subprocess.run(["sudo","-n","/usr/sbin/iptables","-w","5",*operation],check=True,
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True).stdout
    applied=False
    begin=None
    counters={"matchedPackets":0,"matchedBytes":0}
    try:
        if args.kind=="SLOW_BACKUP":
            applied=True
            os.kill(pid,signal.SIGSTOP)
            limit=time.monotonic()+2
            while "\nState:\tT" not in Path(f"/proc/{pid}/status").read_text():
                if time.monotonic()>limit:
                    raise RuntimeError("JVM did not enter stopped state")
                time.sleep(.001)
        else:
            iptables(["-I","OUTPUT","1",*rule])
        applied=True
        begin=time.perf_counter_ns()
        emit("FAULT_APPLIED",kind=args.kind,beginNs=begin,holdMs=args.hold_ms)
        time.sleep(args.hold_ms/1000)
        if args.kind=="ACK_BLOCK":
            listing=iptables(["-n","-v","-x","-L","OUTPUT"])
            matches=[line for line in listing.splitlines() if tag in line]
            assert len(matches)==1
            columns=matches[0].split()
            counters={"matchedPackets":int(columns[0]),"matchedBytes":int(columns[1])}
    finally:
        if applied:
            if args.kind=="SLOW_BACKUP":
                if pb_control.alive(pid):
                    os.kill(pid,signal.SIGCONT)
            else:
                iptables(["-D","OUTPUT",*rule])
                listing=iptables(["-S","OUTPUT"])
                assert tag not in listing,"temporary ACK rule still exists"
            emit("FAULT_RESTORED",kind=args.kind,beginNs=begin,endNs=time.perf_counter_ns(),
                 restored=True,**counters)

if __name__=="__main__":
    main()
