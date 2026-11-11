"""SSM/S3 deployment of Raft + SWIM/Lifeguard to three existing dedicated EC2s."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import sys
import time
import uuid

from ec2_ssm import AWS
from local_raft_fd_validation import properties

HERE=Path(__file__).resolve().parent
SCRIPTS=('pb_control.py','pb_bench.py','election_bench.py')


def run(a):
    ids=a.instances.split(',')
    if len(ids)!=3 or len(set(ids))!=3 or any(not re.fullmatch(r'i-[0-9a-f]+',i) for i in ids):
        raise ValueError('three distinct EC2 IDs required')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]+',a.bucket):
        raise ValueError('invalid S3 bucket')
    if a.repeat<1 or a.timeout not in (500,1000) or a.value not in (64,8192):
        raise ValueError('repeat >= 1, timeout 500/1000 and value 64/8192 required')
    aws=AWS(a.aws_bin,a.profile,a.region)
    identity=aws.call('sts','get-caller-identity')
    response=aws.call('ec2','describe-instances','--instance-ids',*ids)
    nodes={n['InstanceId']:n for r in response['Reservations'] for n in r['Instances']}
    for instance in ids:
        node=nodes[instance]
        if node['State']['Name']!='running' or node['InstanceType']!='m6gd.medium':
            raise RuntimeError('requires running, existing m6gd.medium experiment instances')
        if not any('openkache' in t['Value'].lower() for t in node.get('Tags',[])):
            raise RuntimeError('instance lacks OpenKache experiment tag')
    if len({nodes[i]['VpcId'] for i in ids})!=1:
        raise RuntimeError('three nodes must share a VPC')
    hosts=[nodes[i]['PrivateIpAddress'] for i in ids]
    if not all(ipaddress.ip_address(h).is_private for h in hosts):
        raise RuntimeError('private network required')
    agents=aws.call('ssm','describe-instance-information','--filters',
                    json.dumps([{'Key':'InstanceIds','Values':ids}]))
    if {n['InstanceId'] for n in agents['InstanceInformationList'] if n['PingStatus']=='Online'}!=set(ids):
        raise RuntimeError('SSM agent offline')
    a.out.mkdir(parents=True,exist_ok=False)
    campaign='raft-fd-'+time.strftime('%Y%m%d-%H%M%S',time.gmtime())+'-'+uuid.uuid4().hex[:8]
    remote='/opt/openkache-raft-fd/'+campaign
    destination=f's3://{a.bucket}/{campaign}'
    dist_hash=hashlib.sha256(a.dist.read_bytes()).hexdigest()
    manifest=dict(schema='raft-fd-ssm-1',campaign=campaign,account=identity['Account'],instances=ids,
                  hosts=hosts,region=a.region,detectors=a.detectors,repeat=a.repeat,valueBytes=a.value,
                  timeoutMs=a.timeout,distSha256=dist_hash,resultPrefix=destination,
                  scriptsSha256={n:hashlib.sha256((HERE/n).read_bytes()).hexdigest() for n in SCRIPTS},
                  scenario='settled leader process crash',ssdStallCovered=False)
    (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    def parallel(fn):
        with ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(fn,range(3)))
    preflight="""command -v java
java -version
command -v python3
command -v aws
python3 - <<'PY'
import socket
for kind,port in [(socket.SOCK_STREAM,7000),(socket.SOCK_STREAM,7001),(socket.SOCK_DGRAM,7002)]:
    with socket.socket(type=kind) as s: s.bind(('0.0.0.0',port))
PY
"""
    environment=parallel(lambda i:aws.shell(ids[i],preflight))
    (a.out/'environment.json').write_text(json.dumps(environment,indent=2))
    aws.call('s3','cp',aws.path(a.dist),destination+'/dist.tar','--only-show-errors')
    for name in SCRIPTS:
        aws.call('s3','cp',aws.path(HERE/name),destination+'/'+name,'--only-show-errors')
    def deploy(i):
        script=f'mkdir -p {remote}/dist\naws s3 cp {destination}/dist.tar {remote}/dist.tar --region {a.region} --only-show-errors\n'
        script+=f'echo {dist_hash}  {remote}/dist.tar | sha256sum -c -\ntar -xf {remote}/dist.tar -C {remote}/dist --strip-components=1\n'
        for name in SCRIPTS:
            script+=f'aws s3 cp {destination}/{name} {remote}/{name} --region {a.region} --only-show-errors\n'
            script+=f'echo {manifest["scriptsSha256"][name]}  {remote}/{name} | sha256sum -c -\n'
        return aws.shell(ids[i],script)
    parallel(deploy)
    completed=[]
    try:
        for detector in a.detectors:
            for repeat in range(1,a.repeat+1):
                case=f'{detector.lower()}-v{a.value}-t{a.timeout}-r{repeat}'
                work=remote+'/'+case
                def configure(i):
                    config=properties(hosts,i,detector,(7000,7001,7002),repeat)
                    config=config.replace('timeout.ms=500\n',f'timeout.ms={a.timeout}\n')
                    config=config.replace('failure.suspicion.ms=500\n',f'failure.suspicion.ms={a.timeout}\n')
                    config=config.replace('failure.startup.grace.ms=1000\n','failure.startup.grace.ms=15000\n')
                    encoded=base64.b64encode(config.encode()).decode()
                    return aws.shell(ids[i],f'mkdir {work}\nln -s {remote}/dist {work}/dist\n'
                                     f'cp {remote}/*.py {work}/\nprintf %s {encoded} | base64 -d > {work}/node.properties\n')
                parallel(configure)
                try:
                    parallel(lambda i:aws.shell(ids[i],f'python3 {work}/pb_control.py start'))
                    discovery=json.loads(aws.shell(ids[0],f'python3 {work}/election_bench.py --discover --protocol RAFT --hosts {",".join(hosts)}'))
                    controller=discovery['leader']
                    benchmark=(f'python3 {work}/election_bench.py --protocol RAFT --hosts {",".join(hosts)}'
                               f' --controller {controller} --out {work}/result.json --repeat {repeat}'
                               f' --value {a.value} --timeout {a.timeout}')
                    print(case,flush=True)
                    script=(f'set +e\n{benchmark}\nbench_rc=$?\n'
                            f'aws s3 cp {work}/result.json {destination}/{case}/result.json --region {a.region} --only-show-errors\n'
                            'upload_rc=$?\nexit $((bench_rc || upload_rc))\n')
                    print(aws.shell(ids[controller],script,timeout=120).strip(),flush=True)
                    aws.call('s3','cp',destination+'/'+case+'/result.json',aws.path(a.out/(case+'.json')),'--only-show-errors')
                    data=json.loads((a.out/(case+'.json')).read_text())
                    if not data.get('ok'):
                        raise RuntimeError('benchmark failed')
                    completed.append(case)
                finally:
                    def cleanup(i):
                        return aws.shell(ids[i],f'python3 {work}/pb_control.py stop\n'
                                         f'aws s3 cp {work}/server.log {destination}/{case}/node-{i}.log --region {a.region} --only-show-errors\n')
                    errors=[]
                    with ThreadPoolExecutor(max_workers=3) as pool:
                        jobs=[pool.submit(cleanup,i) for i in range(3)]
                        for job in jobs:
                            try:
                                job.result()
                            except Exception as error:
                                errors.append(str(error))
                    if errors:
                        raise RuntimeError('cleanup incomplete; inspect before rerunning: '+repr(errors))
    finally:
        (a.out/'completed.json').write_text(json.dumps(completed,indent=2)+'\n')
    print('RESULTS',a.out,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--instances',required=True,help='node0,node1,node2 instance IDs')
    p.add_argument('--bucket',required=True);p.add_argument('--dist',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True);p.add_argument('--aws-bin',default='aws')
    p.add_argument('--profile');p.add_argument('--region',default='ap-northeast-2')
    p.add_argument('--repeat',type=int,default=3);p.add_argument('--value',type=int,default=64)
    p.add_argument('--timeout',type=int,default=500)
    p.add_argument('--detectors',nargs='+',choices=['FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'],
                   default=['FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'])
    run(p.parse_args())
