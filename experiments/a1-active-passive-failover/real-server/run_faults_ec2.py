"""Backup fault만 한 단계 실행한다. 기존 crash/다른 알고리즘은 호출하지 않는다."""
import argparse
import concurrent.futures
import hashlib
import ipaddress
import json
from pathlib import Path
import queue
import subprocess
import threading
import time

HERE=Path(__file__).resolve().parent
REMOTE="/home/ubuntu/a1-real"
SCRIPTS=("pb_control.py","pb_bench.py","pb_load.py","pb_fault.py","pb_fault_bench.py","run_faults_ec2.py")

class Stream:
    def __init__(self,argv):
        self.process=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                                      text=True,encoding="utf-8",errors="replace",bufsize=1)
        self.lines=[]
        self.queue=queue.Queue()
        def read():
            for line in self.process.stdout:
                self.lines.append(line)
                self.queue.put(line)
            self.queue.put(None)
        threading.Thread(target=read,daemon=True).start()
    def event(self,name,timeout=30):
        end=time.monotonic()+timeout
        while True:
            line=self.queue.get(timeout=max(.01,end-time.monotonic()))
            if line is None:
                raise RuntimeError(f"stream ended before {name}: {''.join(self.lines)}")
            try:
                data=json.loads(line)
            except json.JSONDecodeError:
                continue
            if data.get("event")==name:
                return data
            if time.monotonic()>end:
                raise TimeoutError(name)
    def send(self,text):
        self.process.stdin.write(text+"\n")
        self.process.stdin.flush()
    def wait(self):
        code=self.process.wait(timeout=30)
        if code:
            raise RuntimeError(f"remote process exit {code}: {''.join(self.lines)}")

def main():
    p=argparse.ArgumentParser()
    for arg in ("hosts","key","known-hosts","dist","out"):
        p.add_argument("--"+arg,type=Path,required=True)
    p.add_argument("--quick",action="store_true")
    args=p.parse_args()
    hosts=json.loads(args.hosts.read_text(encoding="utf-8"))
    assert len(hosts)==3
    for host in hosts:
        ipaddress.IPv4Address(host["public"])
        ipaddress.IPv4Address(host["private"])
    args.out.mkdir(parents=True,exist_ok=False)
    opts=["-i",str(args.key),"-o","BatchMode=yes","-o","ConnectTimeout=8","-o","StrictHostKeyChecking=yes",
          "-o",f"UserKnownHostsFile={args.known_hosts}"]
    def ssh_args(i,cmd):
        return ["ssh",*opts,f"ubuntu@{hosts[i]['public']}",cmd]
    def command(argv):
        r=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding="utf-8",errors="replace",timeout=120)
        if r.returncode:
            raise RuntimeError(r.stdout)
        return r.stdout
    def ssh(i,cmd):
        return command(ssh_args(i,cmd))
    def put(i,path,target):
        return command(["scp",*opts,str(path),f"ubuntu@{hosts[i]['public']}:{target}"])
    def get(i,path,target):
        return command(["scp",*opts,f"ubuntu@{hosts[i]['public']}:{path}",str(target)])
    def parallel(fn,indices=range(3)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            return list(executor.map(fn,indices))
    def stop(i):
        return ssh(i,f"python3 {REMOTE}/pb_control.py stop")
    def deploy(i):
        ssh(i,f"mkdir -p {REMOTE}/dist {REMOTE}/results")
        put(i,args.dist,f"{REMOTE}/dist.tar")
        for script in SCRIPTS[:-1]:
            put(i,HERE/script,f"{REMOTE}/{script}")
        ssh(i,f"tar -xf {REMOTE}/dist.tar -C {REMOTE}/dist --strip-components=1")
        return ssh(i,"uname -a; java -version")
    environment=parallel(deploy)
    (args.out/"environment.json").write_text(json.dumps(environment,indent=2),encoding="utf-8")
    manifest={"schema":"a1-real-pb-backup-fault-deployment-1","utcEpochMs":int(time.time()*1000),"hosts":hosts,
              "distSha256":hashlib.sha256(args.dist.read_bytes()).hexdigest(),
              "scriptsSha256":{name:hashlib.sha256((HERE/name).read_bytes()).hexdigest() for name in SCRIPTS},
              "quick":args.quick,"scenarios":["BASELINE","SLOW_BACKUP","ACK_BLOCK"]}
    (args.out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    fault=None
    bench=None
    try:
        for size in ([64] if args.quick else [64,8192]):
            for scenario in ("BASELINE","SLOW_BACKUP","ACK_BLOCK"):
                for repeat in range(1,2 if args.quick else 4):
                    parallel(stop)
                    def configure(i):
                        path=args.out/f"node-{i}.properties"
                        path.write_text(f"node.id={i}\npeer.0={hosts[0]['private']}\npeer.1={hosts[1]['private']}\npeer.2={hosts[2]['private']}\n"
                                        "client.port=7000\npeer.port=7001\nadmin.port=9091\nheartbeat.ms=50\ntimeout.ms=500\ntick.ms=10\n",encoding="utf-8")
                        put(i,path,f"{REMOTE}/node.properties")
                    parallel(configure)
                    parallel(lambda i:ssh(i,f"python3 {REMOTE}/pb_control.py start"),(1,2))
                    name=f"{scenario}-v{size}-r{repeat}.json"
                    bench=Stream(ssh_args(0,f"python3 {REMOTE}/pb_fault_bench.py --hosts {','.join(h['private'] for h in hosts)}"
                                                f" --scenario {scenario} --value {size} --repeat {repeat} --out {REMOTE}/results/{name}"))
                    bench.event("READY_FOR_FAULT")
                    events={"schema":"a1-backup-fault-control-1","scenario":scenario}
                    if scenario!="BASELINE":
                        hold=200 if scenario=="SLOW_BACKUP" else 800
                        fault=Stream(ssh_args(2,f"python3 {REMOTE}/pb_fault.py --kind {scenario} --primary {hosts[0]['private']} --hold-ms {hold}"))
                        events["applied"]=fault.event("FAULT_APPLIED")
                        events["appliedObservedPcNs"]=time.perf_counter_ns()
                    events["goSentPcNs"]=time.perf_counter_ns()
                    bench.send("GO")
                    events["measured"]=bench.event("MEASURED")
                    events["measuredObservedPcNs"]=time.perf_counter_ns()
                    if fault:
                        events["restored"]=fault.event("FAULT_RESTORED")
                        events["restoreObservedPcNs"]=time.perf_counter_ns()
                        assert events["restored"]["restored"]
                        if scenario=="ACK_BLOCK":
                            assert events["restored"]["matchedPackets"]>0,"no ACK packets hit drop rule"
                        fault.wait()
                        fault=None
                    bench.send("RESTORED")
                    done=bench.event("DONE")
                    bench.wait()
                    bench=None
                    get(0,f"{REMOTE}/results/{name}",args.out/name)
                    (args.out/(name+".control.json")).write_text(json.dumps(events,indent=2),encoding="utf-8")
                    assert json.loads((args.out/name).read_text(encoding="utf-8"))["ok"]
                    print(json.dumps(done),flush=True)
    finally:
        # fault controller는 자체 finally로 SIGCONT/정확한 임시 규칙 제거를 수행한다.
        if fault:
            try:
                fault.event("FAULT_RESTORED",timeout=10)
                fault.wait()
            except Exception as error:
                print(f"FAULT CLEANUP NEEDS CHECK: {error}",flush=True)
        if bench and bench.process.poll() is None:
            bench.process.stdin.close()
            try:
                bench.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                bench.process.terminate()
        for i in range(3):
            try:
                stop(i)
                get(i,f"{REMOTE}/server.log",args.out/f"server-{i}.log")
            except Exception as error:
                print(f"cleanup node {i}: {error}",flush=True)
    final_rules=ssh(2,"sudo -n /usr/sbin/iptables -S OUTPUT")
    assert "a1-pb-ack-" not in final_rules,"an A1 temporary drop rule remains"
    process_checks=parallel(lambda i:ssh(i,"pgrep -af '[P]bServerMain' || true"))
    assert all(not output.strip() for output in process_checks),"a dedicated PB JVM is still running"
    (args.out/"cleanup.json").write_text(json.dumps({"temporaryRulesRemaining":0,"dedicatedJvmsStopped":True}),encoding="utf-8")
    print(f"RESULTS {args.out}",flush=True)

if __name__=="__main__":
    main()
