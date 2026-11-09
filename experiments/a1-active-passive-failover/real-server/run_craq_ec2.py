"""전용 EC2 세 대에서 Chain TAIL_ONLY와 CRAQ 읽기 경로를 같은 배포본·부하로 번갈아 측정한다. 접속 정보는 인자/로컬 host 파일로만 받는다."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import time

HERE=Path(__file__).resolve().parent
REMOTE="/home/ubuntu/a1-real"
MODES=("TAIL_ONLY","CRAQ")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--hosts",type=Path,required=True)
    p.add_argument("--key",type=Path,required=True)
    p.add_argument("--known-hosts",type=Path,required=True)
    p.add_argument("--dist",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--quick",action="store_true")
    p.add_argument("--retry-ms",type=int,default=200)
    args=p.parse_args()
    hosts=json.loads(args.hosts.read_text(encoding="utf-8"))
    assert len(hosts)==3
    args.out.mkdir(parents=True,exist_ok=False)
    opts=["-i",str(args.key),"-o","BatchMode=yes","-o","ConnectTimeout=8","-o","StrictHostKeyChecking=yes",
          "-o",f"UserKnownHostsFile={args.known_hosts}"]
    def command(argv):
        completed=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding="utf-8",errors="replace",timeout=180)
        if completed.returncode:
            raise RuntimeError(f"command failed ({completed.returncode}): {completed.stdout}")
        return completed.stdout
    def ssh(i,cmd):
        return command(["ssh",*opts,f"ubuntu@{hosts[i]['public']}",cmd])
    def put(i,path,target):
        return command(["scp",*opts,str(path),f"ubuntu@{hosts[i]['public']}:{target}"])
    def parallel(fn,indices):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(fn,indices))
    scripts=("pb_control.py","pb_bench.py","chain_bench.py","craq_bench.py")
    def deploy(i):
        ssh(i,f"mkdir -p {REMOTE}/dist {REMOTE}/results")
        put(i,args.dist,f"{REMOTE}/dist.tar")
        for name in scripts:
            put(i,HERE/name,f"{REMOTE}/{name}")
        ssh(i,f"rm -rf {REMOTE}/dist/* && tar -xf {REMOTE}/dist.tar -C {REMOTE}/dist --strip-components=1")
        return ssh(i,"uname -a; lscpu; java -version")
    environment=parallel(deploy,range(3))
    (args.out/"environment.json").write_text(json.dumps(environment,indent=2),encoding="utf-8")
    (args.out/"manifest.json").write_text(json.dumps({
        "schema":"a1-real-deployment-1","utcEpochMs":int(time.time()*1000),
        "distSha256":hashlib.sha256(args.dist.read_bytes()).hexdigest(),"hosts":hosts,
        "scriptsSha256":{name:hashlib.sha256((HERE/name).read_bytes()).hexdigest() for name in scripts+("run_craq_ec2.py",)},
        "commandLineScope":"CHAIN TAIL_ONLY vs CRAQ: clean reads per node, concurrent writer/reader on hot key, no injected fault; modes interleaved per condition",
        "retryMs":args.retry_ms,"quick":args.quick
    },indent=2),encoding="utf-8")
    def stop(i):
        return ssh(i,f"python3 {REMOTE}/pb_control.py stop")
    try:
        for value in ([64] if args.quick else [64,8192]):
            for repeat in range(1,2 if args.quick else 4):
                for mode in MODES:
                    parallel(stop,range(3))
                    def configure(i):
                        config=args.out/f"node-{i}.properties"
                        config.write_text(f"protocol=CHAIN\nchain.read.mode={mode}\nretry.ms={args.retry_ms}\nnode.id={i}\npeer.0={hosts[0]['private']}\npeer.1={hosts[1]['private']}\npeer.2={hosts[2]['private']}\n"
                                          f"client.port=7000\npeer.port=7001\nadmin.port=9091\nheartbeat.ms=50\ntimeout.ms=500\ntick.ms=10\n",encoding="utf-8")
                        put(i,config,f"{REMOTE}/node.properties")
                    parallel(configure,range(3))
                    parallel(lambda i:ssh(i,f"python3 {REMOTE}/pb_control.py start"),range(3))
                    name=f"CRAQ-{mode}-v{value}-r{repeat}.json"
                    output=ssh(0,f"python3 {REMOTE}/craq_bench.py --hosts {','.join(h['private'] for h in hosts)} --read-mode {mode}"
                                 f" --value {value} --repeat {repeat} --retry {args.retry_ms} --out {REMOTE}/results/{name}")
                    command(["scp",*opts,f"ubuntu@{hosts[0]['public']}:{REMOTE}/results/{name}",str(args.out/name)])
                    result=json.loads((args.out/name).read_text(encoding="utf-8"))
                    if not result.get("ok") or result.get("readMode")!=mode:
                        raise RuntimeError(f"case failed: {name}")
                    print(output.strip(),flush=True)
    finally:
        for i in range(3):
            try:
                stop(i)
                command(["scp",*opts,f"ubuntu@{hosts[i]['public']}:{REMOTE}/server.log",str(args.out/f"server-{i}.log")])
            except Exception as error:
                print(f"cleanup node {i}: {error}",flush=True)
    print(f"RESULTS {args.out}",flush=True)

if __name__=="__main__":
    main()
