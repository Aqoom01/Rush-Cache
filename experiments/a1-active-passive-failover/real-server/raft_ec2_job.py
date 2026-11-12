"""Immutable detached-job format and bundle. Contains no credentials or laptop profile."""
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import tarfile
import uuid

from raft_campaign_plan import DETECTORS,SCENARIOS,FIXED_SETTINGS,matrix
from run_raft_campaign import HERE,SNAPSHOT_FILES

SOURCES=tuple(dict.fromkeys((*SNAPSHOT_FILES,'local_fencing_validation.py',
                             'raft_ec2_job.py','raft_ec2_worker.py','raft_ec2_campaign.py','raft_ec2_stop.py')))
REMOTE_BASE='/opt/openkache-raft-coordinator'


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def atomic_json(path,value):
    path=Path(path);tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,indent=2)+'\n');tmp.replace(path)


def validate(job):
    if job.get('schema')!='raft-detached-job-1': raise ValueError('unsupported job schema')
    if not re.fullmatch(r'raft-ec2-[a-f0-9]{12}',job['campaign']): raise ValueError('invalid campaign')
    if not re.fullmatch(r'[0-9]{12}',job['accountId']): raise ValueError('invalid account ID')
    if not re.fullmatch(r'[a-z]{2}(?:-[a-z]+)+-[0-9]+',job['region']): raise ValueError('invalid region')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',job['bucket']): raise ValueError('invalid bucket')
    ids=job['instances']
    if len(ids)!=3 or len(set(ids))!=3 or any(not re.fullmatch(r'i-[a-f0-9]+',i) for i in ids):
        raise ValueError('three distinct experiment instances required')
    if job['coordinator'] not in ids: raise ValueError('coordinator must be one of the three nodes')
    for key in ('dataRoot','python'):
        if not re.fullmatch(r'/[A-Za-z0-9_./-]+',job[key]) or '..' in Path(job[key]).parts:
            raise ValueError('invalid remote path: '+key)
    if not isinstance(job['maxHours'],int) or not 1<=job['maxHours']<=24: raise ValueError('invalid service limit')
    if job['study'] not in ('validation','presentation'): raise ValueError('invalid study')
    if not isinstance(job.get('stopOnSuccess'),bool): raise ValueError('invalid auto-stop setting')
    if job['fixedSettings']!=FIXED_SETTINGS:raise ValueError('job must retain the fixed timeout/settings')
    if not job['detectors'] or len(set(job['detectors']))!=len(job['detectors']) or set(job['detectors'])-set(DETECTORS):
        raise ValueError('invalid detectors')
    if not job['scenarios'] or len(set(job['scenarios']))!=len(job['scenarios']) or set(job['scenarios'])-set(SCENARIOS):
        raise ValueError('invalid scenarios')
    if set(job['sourceSha256'])!=set(SOURCES): raise ValueError('source bundle incomplete')
    for value in [job['distSha256'],job['sourceTarSha256'],*job['sourceSha256'].values()]:
        if not re.fullmatch(r'[a-f0-9]{64}',value): raise ValueError('invalid artifact digest')
    matrix(job['repeat'],job['detectors'],job['scenarios'],job['study'],job['coreRepeat'])
    return job


def remote_root(job): return REMOTE_BASE+'/'+job['campaign']
def prefix(job): return f's3://{job["bucket"]}/{job["campaign"]}'
def unit(job): return 'openkache-'+job['campaign']+'.service'


def coordinator_policy(job):
    validate(job);region=job['region'];account=job['accountId'];bucket=job['bucket'];campaign=job['campaign']
    statements=[
        dict(Effect='Allow',Action=['s3:ListBucket'],Resource=f'arn:aws:s3:::{bucket}',
             Condition={'StringLike':{'s3:prefix':[campaign,campaign+'/*']}}),
        dict(Effect='Allow',Action=['s3:GetObject','s3:PutObject'],Resource=f'arn:aws:s3:::{bucket}/{campaign}/*'),
        dict(Effect='Allow',Action=['ec2:DescribeInstances','ssm:GetCommandInvocation'],Resource='*'),
        dict(Effect='Allow',Action=['ssm:SendCommand'],Resource=[
             f'arn:aws:ssm:{region}::document/AWS-RunShellScript',
             *[f'arn:aws:ec2:{region}:{account}:instance/{i}' for i in job['instances']]])]
    if job['stopOnSuccess']:
        statements.append(dict(Effect='Allow',Action=['ec2:StopInstances'],Resource=[
            f'arn:aws:ec2:{region}:{account}:instance/{i}' for i in job['instances']]))
    return dict(Version='2012-10-17',Statement=statements)


def prepare(a):
    out=Path(a.out);out.mkdir(parents=True,exist_ok=False)
    sources={name:(HERE/name).read_bytes() for name in SOURCES}
    with tarfile.open(out/'source.tar','w') as archive:
        for name,data in sorted(sources.items()):
            item=tarfile.TarInfo(name);item.size=len(data);item.mode=0o644
            archive.addfile(item,io.BytesIO(data))
    shutil.copyfile(a.dist,out/'dist.tar')
    job=dict(schema='raft-detached-job-1',campaign=getattr(a,'campaign_id',None) or 'raft-ec2-'+uuid.uuid4().hex[:12],
             accountId=a.account_id,region=a.region,bucket=a.bucket,instances=a.instances.split(','),
             coordinator=a.coordinator or a.instances.split(',')[-1],dataRoot=a.data_root,python=a.remote_python,
             maxHours=a.max_hours,study=a.study,repeat=a.repeat,coreRepeat=a.core_repeat,fixedSettings=FIXED_SETTINGS,
             stopOnSuccess=not getattr(a,'keep_running',False),
             detectors=list(a.detectors),scenarios=list(a.scenarios),distSha256=sha(out/'dist.tar'),
             sourceTarSha256=sha(out/'source.tar'),
             sourceSha256={n:hashlib.sha256(data).hexdigest() for n,data in sources.items()})
    validate(job);atomic_json(out/'job.json',job)
    atomic_json(out/'package-manifest.json',dict(campaign=job['campaign'],jobSha256=sha(out/'job.json')))
    atomic_json(out/'coordinator-policy.json',coordinator_policy(job))
    atomic_json(out/'plan.json',matrix(job['repeat'],job['detectors'],job['scenarios'],job['study'],job['coreRepeat']))
    return job


def verify_package(folder):
    folder=Path(folder);job=validate(json.loads((folder/'job.json').read_text()))
    seal=json.loads((folder/'package-manifest.json').read_text())
    if seal['jobSha256']!=sha(folder/'job.json'):raise ValueError('prepared job changed; prepare a new package')
    if sha(folder/'dist.tar')!=job['distSha256'] or sha(folder/'source.tar')!=job['sourceTarSha256']:
        raise ValueError('prepared artifacts changed')
    return job
