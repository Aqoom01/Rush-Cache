"""One resumable Raft/FD campaign: prepare, inject, restore, audit, collect and summarize."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
import re
import os
import platform

from ec2_ssm import AWS
from election_bench import discover
from local_fencing_validation import free_port,HOSTS
from local_raft_fd_validation import properties
from pb_bench import Client
from raft_campaign_plan import matrix,DETECTORS,SCENARIOS,FIXED_SETTINGS,CORE_SCENARIOS
from summarize_raft_campaign import summarize as write_report

HERE=Path(__file__).resolve().parent
FILES=('pb_control.py','pb_bench.py','election_bench.py','raft_scenario_bench.py','raft_campaign_fault.py','raft_measurements.py')
SNAPSHOT_FILES=(*FILES,'raft_campaign_plan.py','run_raft_campaign.py','summarize_raft_campaign.py',
                'local_raft_fd_validation.py','ec2_ssm.py','plot_raft_campaign.py','requirements-raft-plots.txt')


def progress(a,event,manifest,**details):
    callback=getattr(a,'progress',None)
    if callback is not None: callback(event,manifest,details)


class Local:
    def __init__(self,a,manifest):
        self.a=a;self.manifest=manifest;self.hosts=HOSTS
        self.sources={name:(HERE/name).read_bytes() for name in FILES}
        if any(hashlib.sha256(data).hexdigest()!=manifest['scriptsSha256'][name] for name,data in self.sources.items()):
            raise RuntimeError('source changed while taking campaign snapshot')
        self.ports=[free_port(False),free_port(False),free_port(True)]
        while len(set(self.ports))!=3: self.ports=[free_port(False),free_port(False),free_port(True)]
    def setup(self,work,spec):
        self.works=[work/f'node-{i}' for i in range(3)]
        for node,root in enumerate(self.works):
            root.mkdir(parents=True)
            (root/'dist').symlink_to(self.a.lib.resolve().parent,target_is_directory=True)
            for name,data in self.sources.items(): (root/name).write_bytes(data)
            (root/'node.properties').write_text(properties(self.hosts,node,spec['detector'],self.ports,spec['repeat']))
            runtime={'OPENKACHE_PB_FAULT_FILE':str(root/'io-fault.properties')}
            if spec.get('storage')=='nvme':
                runtime.update(OPENKACHE_PB_STORAGE_DIR=str(root/'nvme'),OPENKACHE_PB_STORAGE_DIRECT='true')
            (root/'runtime.json').write_text(json.dumps(runtime))
    def start(self):
        for root in self.works:
            subprocess.run([sys.executable,str(root/'pb_control.py'),'start'],check=True,capture_output=True)
    def bench(self,leader,target,spec):
        root=self.works[leader]
        p=subprocess.run([sys.executable,str(root/'raft_scenario_bench.py'),'--hosts',','.join(self.hosts),
                          '--controller',str(leader),'--target',str(target),'--detector',spec['detector'],
                          '--ports',','.join(map(str,self.ports)),'--spec',json.dumps(spec)],
                         capture_output=True,text=True,timeout=spec.get('normalSeconds',0)+spec.get('warmupSeconds',0)+120)
        (root/'bench.log').write_text(p.stdout+'\n'+p.stderr)
        if p.returncode: raise RuntimeError(p.stderr[-3000:])
    def ready(self,leader): return (self.works[leader]/'ready.json').exists()
    def fault(self,target,spec,leader):
        root=self.works[target]
        p=subprocess.run([sys.executable,str(root/'raft_campaign_fault.py'),'--root',str(root),
                          '--hosts',','.join(self.hosts),'--node',str(target),'--spec',json.dumps(spec),
                          '--ports',','.join(map(str,self.ports)),'--local-model'],capture_output=True,text=True,timeout=45)
        source=root/'fault-events.json'
        if source.exists() and target!=leader: shutil.copy2(source,self.works[leader]/source.name)
        if p.returncode: raise RuntimeError(p.stderr[-3000:])
    def result(self,leader,destination):
        shutil.copy2(self.works[leader]/'result.json',destination)
        return json.loads(destination.read_text())
    def stop(self):
        errors=[]
        for root in self.works:
            p=subprocess.run([sys.executable,str(root/'pb_control.py'),'stop'],capture_output=True,text=True,timeout=15)
            if p.returncode: errors.append(p.stderr)
        if errors: raise RuntimeError('cleanup failed: '+repr(errors))


class SSM:
    def __init__(self,a,manifest):
        self.a=a;self.manifest=manifest;self.aws=AWS(a.aws_bin,a.profile,a.region)
        self.ids=a.instances.split(',');self.ports=[7000,7001,7002]
        if len(self.ids)!=3 or len(set(self.ids))!=3 or any(not re.fullmatch(r'i-[0-9a-f]+',i) for i in self.ids):
            raise ValueError('three distinct existing EC2 instances required')
        if not re.fullmatch(r'[a-z0-9][a-z0-9.-]+',a.bucket) or not re.fullmatch(r'/[A-Za-z0-9/_-]+',a.data_root):
            raise ValueError('invalid bucket or data root')
        if manifest.get('instances') and manifest['instances']!=self.ids:
            raise ValueError('resume cannot change EC2 instances')
        response=self.aws.call('ec2','describe-instances','--instance-ids',*self.ids)
        nodes={n['InstanceId']:n for r in response['Reservations'] for n in r['Instances']}
        for instance in self.ids:
            n=nodes[instance]
            if n['InstanceType']!='m6gd.medium' or n['State']['Name']!='running':
                raise RuntimeError('all experiment nodes must be running m6gd.medium instances')
            if not any('openkache' in t['Value'].lower() for t in n.get('Tags',[])):
                raise RuntimeError('missing OpenKache experiment tag')
        if len({nodes[i]['VpcId'] for i in self.ids})!=1: raise RuntimeError('nodes must share VPC')
        if len({nodes[i]['Placement']['AvailabilityZone'] for i in self.ids})!=1:
            raise RuntimeError('comparison nodes must share availability zone')
        self.hosts=[nodes[i]['PrivateIpAddress'] for i in self.ids]
        self.remote='/opt/openkache-raft-campaign/'+manifest['campaign']
        self.s3=f's3://{a.bucket}/{manifest["campaign"]}'
        manifest.update(instances=self.ids,hosts=self.hosts,resultPrefix=self.s3,
                        nodePlacement=[dict(instance=i,zone=nodes[i]['Placement']['AvailabilityZone'],
                                            type=nodes[i]['InstanceType'],privateIp=nodes[i]['PrivateIpAddress']) for i in self.ids])
        preflight='''command -v java
java -version 2>&1
uname -a
lscpu -J
command -v python3
python3 --version
command -v aws
command -v tc
command -v iptables
command -v fio
python3 - <<'PY'
import socket
for kind,port in [(socket.SOCK_STREAM,7000),(socket.SOCK_STREAM,7001),(socket.SOCK_DGRAM,7002)]:
    with socket.socket(type=kind) as s: s.bind(('0.0.0.0',port))
PY
'''
        if any(s.get('storage')=='nvme' for s in manifest['matrix']):
            # Do not format or choose an arbitrary disk. Require an already prepared instance store.
            preflight+=f'''test -d {a.data_root}
device=$(findmnt -n -o SOURCE --target {a.data_root})
test "$(lsblk -dn -o MODEL "$device" | xargs)" = 'Amazon EC2 NVMe Instance Storage'
'''
        with ThreadPoolExecutor(max_workers=3) as pool:
            manifest['environment']=list(pool.map(lambda i:self.aws.shell(self.ids[i],preflight),range(3)))
        self.aws.call('s3','cp',self.aws.path(a.dist),self.s3+'/dist.tar','--only-show-errors')
        for name in FILES: self.aws.call('s3','cp',self.aws.path(HERE/name),self.s3+'/'+name,'--only-show-errors')
        def deploy(i):
            script=f'mkdir -p {self.remote}/dist\naws s3 cp {self.s3}/dist.tar {self.remote}/dist.tar --region {a.region} --only-show-errors\n'
            script+=f'echo {manifest["distSha256"]}  {self.remote}/dist.tar | sha256sum -c -\n'
            script+=f'tar -xf {self.remote}/dist.tar -C {self.remote}/dist --strip-components=1\n'
            for name in FILES:
                script+=f'aws s3 cp {self.s3}/{name} {self.remote}/{name} --region {a.region} --only-show-errors\n'
                script+=f'echo {manifest["scriptsSha256"][name]}  {self.remote}/{name} | sha256sum -c -\n'
            return self.aws.shell(self.ids[i],script)
        with ThreadPoolExecutor(max_workers=3) as pool: list(pool.map(deploy,range(3)))
    def setup(self,work,spec):
        import base64
        self.case=spec['case']+'-'+work.name
        self.work=self.remote+'/'+self.case;self.case_s3=self.s3+'/'+self.case
        def configure(i):
            config=properties(self.hosts,i,spec['detector'],self.ports,spec['repeat'])
            config=config.replace('failure.startup.grace.ms=1000','failure.startup.grace.ms=15000')
            runtime={'OPENKACHE_PB_FAULT_FILE':self.work+'/io-fault.properties'}
            extra=''
            if spec.get('storage')=='nvme':
                directory=f'{self.a.data_root}/{self.manifest["campaign"]}/{self.case}/node-{i}'
                extra=f'mkdir -p {directory}\n'
                runtime.update(OPENKACHE_PB_STORAGE_DIR=directory+'/nvme',OPENKACHE_PB_STORAGE_DIRECT='true')
            encode=lambda v:base64.b64encode(v.encode()).decode()
            script=f'mkdir {self.work}\nln -s {self.remote}/dist {self.work}/dist\ncp {self.remote}/*.py {self.work}/\n'+extra
            script+=f'printf %s {encode(config)} | base64 -d > {self.work}/node.properties\n'
            script+=f'printf %s {encode(json.dumps(runtime))} | base64 -d > {self.work}/runtime.json\n'
            return self.aws.shell(self.ids[i],script)
        with ThreadPoolExecutor(max_workers=3) as pool: list(pool.map(configure,range(3)))
    def start(self):
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda i:self.aws.shell(self.ids[i],f'python3 {self.work}/pb_control.py start'),range(3)))
    def bench(self,leader,target,spec):
        import shlex
        command=(f'python3 {self.work}/raft_scenario_bench.py --hosts {",".join(self.hosts)} --controller {leader}'
                 f' --target {target} --detector {spec["detector"]} --spec {shlex.quote(json.dumps(spec))}'
                 f' --s3 {self.case_s3} --region {self.a.region}')
        return self.aws.shell(self.ids[leader],command,timeout=spec.get('normalSeconds',0)+spec.get('warmupSeconds',0)+120)
    def ready(self,leader):
        try:
            return bool(self.aws.call('s3api','head-object','--bucket',self.a.bucket,
                                      '--key',self.manifest['campaign']+'/'+self.case+'/ready.json'))
        except RuntimeError as error:
            if not any(s in str(error) for s in ('404','NotFound','NoSuchKey')): raise
            return False
    def fault(self,target,spec,leader):
        import shlex
        script=(f'set +e\npython3 {self.work}/raft_campaign_fault.py --root {self.work} --hosts {",".join(self.hosts)}'
                f' --node {target} --spec {shlex.quote(json.dumps(spec))}\nrc=$?\n'
                f'aws s3 cp {self.work}/fault-events.json {self.case_s3}/fault-events.json --region {self.a.region} --only-show-errors\n'
                'upload_rc=$?\nexit $((rc || upload_rc))\n')
        return self.aws.shell(self.ids[target],script,timeout=90)
    def result(self,leader,destination):
        self.aws.call('s3','cp',self.case_s3+'/result.json',self.aws.path(destination),'--only-show-errors')
        return json.loads(destination.read_text())
    def stop(self):
        def stop(i):
            return self.aws.shell(self.ids[i],f'python3 {self.work}/pb_control.py stop\n'
                                  f'aws s3 cp {self.work}/server.log {self.case_s3}/node-{i}.log --region {self.a.region} --only-show-errors\n')
        errors=[]
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs=[pool.submit(stop,i) for i in range(3)]
            for job in jobs:
                try: job.result()
                except Exception as error: errors.append(str(error))
        if errors: raise RuntimeError('cleanup failed; inspect before resuming: '+repr(errors))

    def publish(self,root,full=False):
        # Copy only our artifacts. Never delete S3 objects, even when resuming.
        names=['manifest.json','summary.json']
        if full: names+=['runs.csv','conditions.csv','paired-comparisons.csv','comparison-summary.csv','report.md','analysis-provenance.json']
        paths=[root/name for name in names if (root/name).exists()]
        if full and (root/'figures').exists():
            paths += [p for p in sorted((root/'figures').iterdir()) if p.suffix in ('.png','.svg','.md','.json')]
        status=root/'archive-status.json'
        record=dict(state='uploading',prefix=self.s3,files=[p.relative_to(root).as_posix() for p in paths])
        if full: status.write_text(json.dumps(record,indent=2)+'\n')
        try:
            for path in paths:
                self.aws.call('s3','cp',self.aws.path(path),self.s3+'/'+path.relative_to(root).as_posix(),'--only-show-errors')
            if full:
                record.update(state='complete',uploadedAtEpochNs=time.time_ns())
                status.write_text(json.dumps(record,indent=2)+'\n')
                # Last object is the publication marker, separate from experiment pass/fail.
                self.aws.call('s3','cp',self.aws.path(status),self.s3+'/archive-status.json','--only-show-errors')
        except Exception as error:
            if full:
                record.update(state='failed',error=repr(error));status.write_text(json.dumps(record,indent=2)+'\n')
            raise


def report(a):
    write_report(a.out)
    if getattr(a,'study','validation')=='presentation':
        subprocess.run([getattr(a,'plot_python',None) or sys.executable,str(HERE/'plot_raft_campaign.py'),str(a.out)],check=True)


def summary(a,manifest):
    records=[]
    for spec in manifest['matrix']:
        file=a.out/spec['case']/'result.json'
        outcome=a.out/spec['case']/'outcome.json'
        if outcome.exists(): file=outcome
        result=json.loads(file.read_text()) if file.exists() else {}
        records.append(dict(case=spec['case'],scenario=spec['scenario'],detector=spec['detector'],
                            status='passed' if result.get('ok') else 'failed' if result else 'pending',
                            error=result.get('error'),recoveryUpperNs=result.get('recoveryUpperNs'),
                            preservedSuccesses=result.get('preservedSuccesses'),falseDeadEvents=result.get('falseDeadEvents')))
    complete=all(r['status']=='passed' for r in records)
    (a.out/'summary.json').write_text(json.dumps(dict(complete=complete,planned=len(records),
                                                    passed=sum(r['status']=='passed' for r in records),cases=records),indent=2)+'\n')
    return complete


def run(a):
    study=getattr(a,'study','validation')
    plan=matrix(a.repeat,a.detectors,a.scenarios,study,getattr(a,'core_repeat',10))
    hashes={n:hashlib.sha256((HERE/n).read_bytes()).hexdigest() for n in SNAPSHOT_FILES}
    # Local manifest binds both protocol and server jars, as well as all scripts.
    artifact_hash=hashlib.sha256(b''.join(p.read_bytes() for p in sorted(a.lib.glob('*.jar')))).hexdigest() if a.local else hashlib.sha256(a.dist.read_bytes()).hexdigest()
    if a.resume:
        manifest=json.loads((a.out/'manifest.json').read_text())
        if manifest['matrix']!=plan or manifest['scriptsSha256']!=hashes or manifest['distSha256']!=artifact_hash:
            raise ValueError('resume requires identical matrix, binaries and scripts')
    else:
        campaign=getattr(a,'campaign_id',None) or 'raft-all-'+uuid.uuid4().hex[:12]
        if not re.fullmatch(r'raft-(?:all|ec2)-[a-f0-9]{12}',campaign): raise ValueError('invalid campaign ID')
        a.out.mkdir(parents=True,exist_ok=False)
        manifest=dict(schema='raft-full-campaign-2',campaign=campaign,matrix=plan,
                      study=study,fixedSettings=FIXED_SETTINGS,coreScenarios=CORE_SCENARIOS,
                      executionController=getattr(a,'execution_controller',{'mode':'local-direct'}),
                      controllerEnvironment=dict(platform=platform.platform(),python=sys.version,
                                                 logicalCpus=os.cpu_count(),allowedCpus=sorted(os.sched_getaffinity(0))),
                      hypotheses=['crash recovery versus live-leader election starts',
                                  'observer slowdown: false leader DEAD and actual leader identity changes',
                                  'healthy: request latency and application bytes per second / successful SET'],
                      scriptsSha256=hashes,distSha256=artifact_hash,backend='local' if a.local else 'ssm',
                      ioStallModel='deterministic software I/O completion stall on NVMe KV',
                      networkModel='application message drop/delay' if a.local else 'kernel tc netem / iptables',
                      contentionModel='local filesystem fsync writer' if a.local else 'fio direct I/O on verified instance NVMe')
        (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if a.plan_only:
        summary(a,manifest);print(json.dumps(manifest,indent=2));return
    if a.resume and summary(a,manifest):
        report(a)
        if not a.local:
            # Retry report publication without initializing SSM/deploying/starting any JVM.
            archive=SSM.__new__(SSM);archive.aws=AWS(a.aws_bin,a.profile,a.region);archive.s3=manifest['resultPrefix']
            archive.publish(a.out,full=True)
        print('CAMPAIGN ALREADY COMPLETE',a.out,flush=True);return
    try:
        if study=='presentation':
            subprocess.run([getattr(a,'plot_python',None) or sys.executable,'-c','import matplotlib'],check=True,capture_output=True)
        backend=Local(a,manifest) if a.local else SSM(a,manifest)
    except Exception as error:
        manifest['preflightError']=repr(error)
        (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        summary(a,manifest);write_report(a.out)
        raise
    (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if not a.local: backend.publish(a.out)
    progress(a,'backend-ready',manifest)
    for index,spec in enumerate(plan,1):
        case_root=a.out/spec['case'];case_root.mkdir(exist_ok=True)
        destination=case_root/'result.json'
        if a.resume and destination.exists() and json.loads(destination.read_text()).get('ok'): continue
        work=case_root/f'attempt-{len(list(case_root.glob("attempt-*")))+1}';work.mkdir()
        print(f'[{index}/{len(plan)}] {spec["case"]}',flush=True)
        prepared=False
        leader=None
        try:
            progress(a,'case-started',manifest,case=spec['case'],attempt=work.name)
            backend.setup(work,spec);prepared=True;backend.start()
            if a.local:
                c=Client(backend.hosts,client_port=backend.ports[0],peer_port=backend.ports[1])
                try: leader,_,_=discover(c,'RAFT')
                finally: c.close()
            else:
                info=json.loads(backend.aws.shell(backend.ids[0],f'python3 {backend.work}/election_bench.py --discover --protocol RAFT --hosts {",".join(backend.hosts)}'))
                leader=info['leader']
            target=leader if spec['target']=='leader' else (leader+1)%3
            progress(a,'case-ready',manifest,case=spec['case'],attempt=work.name,leader=leader,target=target)
            with ThreadPoolExecutor(max_workers=2) as pool:
                bench=pool.submit(backend.bench,leader,target,spec)
                deadline=time.monotonic()+spec.get('normalSeconds',0)+spec.get('warmupSeconds',0)+90
                while not backend.ready(leader):
                    if bench.done(): bench.result();raise RuntimeError('benchmark ended without readiness marker')
                    if time.monotonic()>deadline: raise TimeoutError('benchmark readiness timed out')
                    time.sleep(.25)
                fault=pool.submit(backend.fault,target,spec,leader)
                # Fault completion includes restoration and evidence upload.
                fault.result(timeout=100);bench.result(timeout=100)
            data=backend.result(leader,destination)
            if not data.get('ok'): raise RuntimeError('scenario audit failed')
        except Exception as error:
            # Preserve the failed attempt, continue only after cleanup succeeds.
            if leader is not None:
                try: backend.result(leader,work/'partial-result.json')
                except Exception: pass
            retained=destination if destination.exists() else work/'partial-result.json'
            previous=json.loads(retained.read_text()) if retained.exists() else {}
            previous.update(ok=False,error=repr(error),case=spec['case'])
            destination.write_text(json.dumps(previous,indent=2)+'\n')
            print('FAILED',spec['case'],str(error),flush=True)
        finally:
            if prepared:
                try: backend.stop()
                except Exception as error:
                    manifest['cleanupError']=repr(error)
                    (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
                    summary(a,manifest);report(a)
                    if not a.local: backend.publish(a.out,full=True)
                    raise
            if destination.exists():
                outcome=json.loads(destination.read_text())
                (case_root/'outcome.json').write_text(json.dumps({k:outcome.get(k) for k in
                    ('ok','error','recoveryUpperNs','preservedSuccesses','falseDeadEvents')})+'\n')
            summary(a,manifest)
            progress(a,'case-finished',manifest,case=spec['case'],attempt=work.name)
            if not a.local: backend.publish(a.out)
    complete=summary(a,manifest);report(a)
    if not a.local: backend.publish(a.out,full=True)
    if not complete: raise RuntimeError('campaign has failed/pending cases; see summary.json and resume after diagnosis')
    print('CAMPAIGN COMPLETE',a.out,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--local',action='store_true');p.add_argument('--lib',type=Path)
    p.add_argument('--instances');p.add_argument('--bucket');p.add_argument('--dist',type=Path)
    p.add_argument('--profile',default='openkache');p.add_argument('--aws-bin',default='aws')
    p.add_argument('--region',default='ap-northeast-2');p.add_argument('--data-root',default='/mnt/openkache-data')
    p.add_argument('--repeat',type=int,default=3);p.add_argument('--resume',action='store_true');p.add_argument('--plan-only',action='store_true')
    p.add_argument('--study',choices=['validation','presentation'],default='validation')
    p.add_argument('--core-repeat',type=int,default=10)
    p.add_argument('--plot-python',help='Python with Matplotlib; defaults to the runner Python')
    p.add_argument('--campaign-id',help='Fixed campaign identity for a detached EC2 coordinator')
    p.add_argument('--foreground',action='store_true',help='Explicit debugging only: keep EC2 orchestration on this terminal')
    p.add_argument('--detectors',nargs='+',choices=DETECTORS,default=list(DETECTORS))
    p.add_argument('--scenarios',nargs='+',choices=list(SCENARIOS),default=list(SCENARIOS))
    a=p.parse_args();a.out=a.out.resolve()
    if a.local and not a.lib: p.error('--local requires --lib')
    if not a.local and not(a.instances and a.bucket and a.dist): p.error('EC2 requires --instances, --bucket, --dist')
    if not a.local and not a.plan_only and not a.foreground:
        p.error('Use raft_ec2_campaign.py prepare/start for detached EC2 execution; --foreground is debugging only')
    run(a)
