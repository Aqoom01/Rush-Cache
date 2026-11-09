"""전용 EC2 세 대에 배포해 PB crash를 측정한다. 접속 정보는 인자/로컬 host 파일로만 받는다."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import time

HERE=Path(__file__).resolve().parent
REMOTE="/home/ubuntu/a1-real"

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--hosts",type=Path,required=True)
    p.add_argument("--key",type=Path,required=True)
    p.add_argument("--known-hosts",type=Path,required=True)
    p.add_argument("--dist",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--quick",action="store_true")
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
    def deploy(i):
        ssh(i,f"mkdir -p {REMOTE}/dist {REMOTE}/results")
        put(i,args.dist,f"{REMOTE}/dist.tar")
        put(i,HERE/"pb_control.py",f"{REMOTE}/pb_control.py")
        put(i,HERE/"pb_bench.py",f"{REMOTE}/pb_bench.py")
        put(i,HERE/"election_bench.py",f"{REMOTE}/election_bench.py")
        ssh(i,f"tar -xf {REMOTE}/dist.tar -C {REMOTE}/dist --strip-components=1")
        return ssh(i,"uname -a; lscpu; java -version")
    environment=parallel(deploy,range(3))
    (args.out/"environment.json").write_text(json.dumps(environment,indent=2),encoding="utf-8")
    (args.out/"manifest.json").write_text(json.dumps({
        "schema":"a1-real-deployment-1","utcEpochMs":int(time.time()*1000),
        "distSha256":hashlib.sha256(args.dist.read_bytes()).hexdigest(),"hosts":hosts,
        "scriptsSha256":{name:hashlib.sha256((HERE/name).read_bytes()).hexdigest() for name in ("pb_bench.py","election_bench.py","pb_control.py","run_election_ec2.py")},
        "commandLineScope":"PB/VR/RAFT matched settled idle crash; warmup30 normal128; elected-leader controller","quick":args.quick
    },indent=2),encoding="utf-8")
    def stop(i):
        return ssh(i,f"python3 {REMOTE}/pb_control.py stop")
    try:
        for protocol in ("PB","VR","RAFT"):
            for timeout in ([500] if args.quick else [500,1000]):
                for value in ([64] if args.quick else [64,8192]):
                    for repeat in range(1,2 if args.quick else 4):
                        parallel(stop,range(3))
                        def configure(i):
                            config=args.out/f"node-{i}.properties"
                            config.write_text(f"protocol={protocol}\nelection.seed={repeat}\nnode.id={i}\npeer.0={hosts[0]['private']}\npeer.1={hosts[1]['private']}\npeer.2={hosts[2]['private']}\n"
                                              f"client.port=7000\npeer.port=7001\nadmin.port=9091\nheartbeat.ms=50\ntimeout.ms={timeout}\ntick.ms=10\n",encoding="utf-8")
                            put(i,config,f"{REMOTE}/node.properties")
                        parallel(configure,range(3))
                        parallel(lambda i:ssh(i,f"python3 {REMOTE}/pb_control.py start"),range(3))
                        discovery=json.loads(ssh(0,f"python3 {REMOTE}/election_bench.py --discover --protocol {protocol} --hosts {','.join(h['private'] for h in hosts)}"))
                        controller=discovery["leader"]
                        name=f"{protocol}-v{value}-t{timeout}-r{repeat}.json"
                        backend="election_bench.py"
                        count=128
                        output=ssh(controller,f"python3 {REMOTE}/{backend} --controller {controller} --protocol {protocol} --hosts {','.join(h['private'] for h in hosts)} --value {value} --timeout {timeout}"
                                     f" --repeat {repeat} --count {count} --out {REMOTE}/results/{name}")
                        command(["scp",*opts,f"ubuntu@{hosts[controller]['public']}:{REMOTE}/results/{name}",str(args.out/name)])
                        result=json.loads((args.out/name).read_text(encoding="utf-8"))
                        if not result.get("ok"):
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
