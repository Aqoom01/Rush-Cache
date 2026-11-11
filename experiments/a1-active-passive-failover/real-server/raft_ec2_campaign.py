"""Prepare/start a detached EC2 campaign; status/collect never start an experiment."""
import argparse
import json
from pathlib import Path
import shlex
import tempfile
import time

from ec2_ssm import AWS
from raft_campaign_plan import DETECTORS,SCENARIOS
from raft_ec2_job import prepare,verify_package,validate,atomic_json,sha,prefix,remote_root,unit


def bootstrap(job,job_sha,resume=False):
    validate(job);root=remote_root(job);source=prefix(job)+'/control';python=job['python']
    lines=['set -euo pipefail','command -v systemd-run','command -v flock',
           f'install -d -m 0755 {root}',f'exec 9>{root}/launch.lock','flock -n 9',
           f'if test -f {root}/job.json; then',
           f'  echo "{job_sha}  {root}/job.json" | sha256sum -c -','else']
    for name,digest in (('source.tar',job['sourceTarSha256']),('dist.tar',job['distSha256']),('job.json',job_sha)):
        lines += [f'  aws s3 cp {source}/{name} {root}/{name}.tmp --region {job["region"]} --only-show-errors',
                  f'  echo "{digest}  {root}/{name}.tmp" | sha256sum -c -',
                  f'  mv {root}/{name}.tmp {root}/{name}']
    # Flat, regular source files only. No tar symlinks, absolute paths or traversal extraction.
    extract=("import json,tarfile,hashlib\nfrom pathlib import Path\n"
             f"root=Path({root!r});job=json.loads((root/'job.json').read_text())\n"
             "dest=root/'source';dest.mkdir(exist_ok=True)\n"
             "with tarfile.open(root/'source.tar') as archive:\n"
             " members=archive.getmembers()\n"
             " if len(members)!=len(job['sourceSha256']): raise ValueError('source member count')\n"
             " for member in members:\n"
             "  if not member.isfile() or member.name not in job['sourceSha256'] or member.size>2000000: raise ValueError('unsafe source member')\n"
             "  data=archive.extractfile(member).read()\n"
             "  if hashlib.sha256(data).hexdigest()!=job['sourceSha256'][member.name]: raise ValueError('source digest mismatch')\n"
             "  (dest/member.name).write_bytes(data)\n")
    # Always verify/extract the exact immutable bundle, including a recovered partial first setup.
    lines += ['fi',f'echo "{job["sourceTarSha256"]}  {root}/source.tar" | sha256sum -c -',
              f'echo "{job["distSha256"]}  {root}/dist.tar" | sha256sum -c -',
              f"python3 - <<'PY'\n{extract}PY",
              f'if systemctl is-active --quiet {unit(job)}; then',
              "  echo 'Coordinator service already active; no duplicate launch'",'  exit 0','fi']
    if not resume:
        lines += [f'if test -d {root}/results; then',"  echo 'Existing results: use explicit resume' >&2",'  exit 1','fi']
    if job['study']=='presentation': lines += [shlex.join([python,'-c','import matplotlib'])]
    command=['systemd-run','--collect','--service-type=exec','--unit='+unit(job),
             '--property=WorkingDirectory='+root,'--property=Restart=no',
             '--property=RuntimeMaxSec='+str(job['maxHours']*3600),'--property=TimeoutStopSec=300',
             '--property=KillMode=mixed','--property=StandardOutput=append:'+root+'/service.log',
             '--property=StandardError=append:'+root+'/service.log',
             python,'-u',root+'/source/raft_ec2_worker.py','--job',root+'/job.json']
    if resume: command.append('--resume')
    lines += [shlex.join(command),f'systemctl show {unit(job)} --property=ActiveState --property=SubState --no-pager']
    return '\n'.join(lines)+'\n'


def client(a,job): return AWS(a.aws_bin,a.profile,job['region'])


def start(a,resume=False):
    job=verify_package(a.job);aws=client(a,job)
    identity=aws.call('sts','get-caller-identity')
    if identity['Account']!=job['accountId']: raise ValueError('AWS account differs from prepared job')
    response=aws.call('ec2','describe-instances','--instance-ids',*job['instances'])
    nodes={n['InstanceId']:n for r in response['Reservations'] for n in r['Instances']}
    for instance in job['instances']:
        n=nodes[instance]
        if n['InstanceType']!='m6gd.medium':
            raise ValueError('all three nodes must be running m6gd.medium')
        if not any('openkache' in t['Value'].lower() for t in n.get('Tags',[])):
            raise ValueError('missing experiment tag')
    if nodes[job['coordinator']]['State']['Name']!='running':raise ValueError('coordinator must be running')
    states={n['State']['Name'] for n in nodes.values()}
    if states!={'running'}:
        if not(resume and job['stopOnSuccess'] and states<= {'running','stopping','stopped'}):
            raise ValueError('all experiment nodes must be running')
        # A failed last stop request may leave the helpers stopped. Retry only an already completed archive.
        with tempfile.TemporaryDirectory(prefix='raft-stop-resume-') as temp:
            values={}
            for name in ('summary.json','archive-status.json'):
                path=Path(temp)/name
                aws.call('s3','cp',prefix(job)+'/'+name,aws.path(path),'--only-show-errors')
                values[name]=json.loads(path.read_text())
        summary=values['summary.json'];archive=values['archive-status.json']
        if not(summary.get('complete') is True and summary.get('passed')==summary.get('planned') and
               summary.get('planned',0)>0 and archive.get('state')=='complete' and archive.get('prefix')==prefix(job)):
            raise ValueError('stopped helpers may only resume an already completed archived campaign')
    if len({n['VpcId'] for n in nodes.values()})!=1 or len({n['Placement']['AvailabilityZone'] for n in nodes.values()})!=1:
        raise ValueError('experiment nodes must share VPC and AZ')
    if not nodes[job['coordinator']].get('IamInstanceProfile'): raise ValueError('coordinator instance role required')
    for name in ('source.tar','dist.tar','job.json'):
        aws.call('s3','cp',aws.path(a.job/name),prefix(job)+'/control/'+name,'--only-show-errors')
    script=bootstrap(job,sha(a.job/'job.json'),resume)
    (a.job/'start-script.sh').write_text(script)
    receipt=dict(campaign=job['campaign'],coordinator=job['coordinator'],unit=unit(job),
                 resultPrefix=prefix(job),remoteRoot=remote_root(job),state='submitting',resume=resume)
    atomic_json(a.job/'launch-receipt.json',receipt)
    output=aws.shell(job['coordinator'],script,timeout=120)
    receipt.update(state='service-accepted',acceptedAtEpochNs=time.time_ns(),serviceOutput=output)
    atomic_json(a.job/'launch-receipt.json',receipt)
    return receipt


def status(a):
    job=validate(json.loads((a.job/'job.json').read_text()));aws=client(a,job)
    with tempfile.TemporaryDirectory(prefix='raft-status-') as temp:
        path=Path(temp)/'status.json'
        try: aws.call('s3','cp',prefix(job)+'/control/status.json',aws.path(path),'--only-show-errors')
        except RuntimeError as error:
            if not any(s in str(error) for s in ('404','NoSuchKey','Not Found')): raise
            value=dict(state='not-published',note='Not evidence that the service is stopped; inspect --live.',resultPrefix=prefix(job))
        else:value=json.loads(path.read_text())
    if 'heartbeatEpochNs' in value:
        age=max(0,(time.time_ns()-value['heartbeatEpochNs'])/1e9)
        value.update(heartbeatAgeSeconds=round(age,1),heartbeatStale=age>90 and value['state'] not in
                     ('completed','failed','interrupted','publication-failed'))
    if getattr(a,'live',False):
        response=aws.call('ec2','describe-instances','--instance-ids',*job['instances'])
        states={n['InstanceId']:n['State']['Name'] for r in response['Reservations'] for n in r['Instances']}
        value['instanceStates']=states
        value['autoStopObservedComplete']=set(states)==set(job['instances']) and all(s=='stopped' for s in states.values())
        root=remote_root(job)
        if states.get(job['coordinator'])=='running':
            value['service']=aws.shell(job['coordinator'],
                f'systemctl show {unit(job)} --property=ActiveState --property=SubState --no-pager || true\n'
                f'tail -n 30 {root}/service.log 2>/dev/null || true\n')
        else:value['service']='Coordinator is not running; archived results remain in S3.'
    return value


def collect(a):
    job=validate(json.loads((a.job/'job.json').read_text()));aws=client(a,job)
    a.out.mkdir(parents=True,exist_ok=True)
    # Download copies only. No S3 rm, sync --delete or local result dependency during the run.
    aws.call('s3','cp',prefix(job)+'/',aws.path(a.out),'--recursive','--only-show-errors')
    return dict(downloadedTo=str(a.out.resolve()),source=prefix(job))


def main():
    parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
    p=commands.add_parser('prepare',help='local packaging only; no AWS calls')
    p.add_argument('--out',type=Path,required=True);p.add_argument('--dist',type=Path,required=True)
    p.add_argument('--campaign-id',help='Reserve the same identity as Terraform IAM/S3 permissions')
    p.add_argument('--instances',required=True);p.add_argument('--coordinator')
    p.add_argument('--account-id',required=True);p.add_argument('--bucket',required=True)
    p.add_argument('--region',default='ap-northeast-2');p.add_argument('--data-root',default='/mnt/openkache-data')
    p.add_argument('--remote-python',default='/usr/bin/python3');p.add_argument('--max-hours',type=int,default=10)
    p.add_argument('--keep-running',action='store_true',help='Disable default EC2 auto-stop after successful S3 archival')
    p.add_argument('--study',choices=['validation','presentation'],default='presentation')
    p.add_argument('--repeat',type=int,default=3);p.add_argument('--core-repeat',type=int,default=10)
    p.add_argument('--detectors',nargs='+',choices=DETECTORS,default=list(DETECTORS))
    p.add_argument('--scenarios',nargs='+',choices=list(SCENARIOS),default=list(SCENARIOS))
    for name in ('start','resume','status','collect'):
        p=commands.add_parser(name);p.add_argument('--job',type=Path,required=True)
        p.add_argument('--profile',default='openkache');p.add_argument('--aws-bin',default='aws')
        if name=='status': p.add_argument('--live',action='store_true')
        if name=='collect': p.add_argument('--out',type=Path,required=True)
    a=parser.parse_args()
    if a.command=='prepare':
        job=prepare(a);(a.out/'start-script.sh').write_text(bootstrap(job,sha(a.out/'job.json')))
        value=dict(prepared=str(a.out.resolve()),campaign=job['campaign'],resultPrefix=prefix(job),
                   planned=len(json.loads((a.out/'plan.json').read_text())),awsCalled=False)
    elif a.command=='start': value=start(a)
    elif a.command=='resume': value=start(a,resume=True)
    elif a.command=='status': value=status(a)
    else: value=collect(a)
    print(json.dumps(value,indent=2))


if __name__=='__main__': main()
