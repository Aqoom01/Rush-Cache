"""Stop only this job's instances after completed results are verified in S3."""
import json
from pathlib import Path,PurePosixPath
import tempfile
import time

from raft_campaign_plan import matrix
from raft_ec2_job import validate,prefix,sha,atomic_json


def check_stop_permission(job,aws):
    if not job['stopOnSuccess']:return
    try:aws.call('ec2','stop-instances','--instance-ids',*job['instances'],'--dry-run')
    except RuntimeError as error:
        if 'DryRunOperation' in str(error):return
        raise RuntimeError('auto-stop permission preflight failed: '+str(error)) from error
    raise RuntimeError('expected DryRunOperation from stop permission check')


def verify_archive(job,out,aws):
    validate(job);out=Path(out)
    plan=matrix(job['repeat'],job['detectors'],job['scenarios'],job['study'],job['coreRepeat'])
    expected={s['case'] for s in plan}
    summary=json.loads((out/'summary.json').read_text())
    rows=summary.get('cases',[])
    if not (summary.get('complete') is True and summary.get('planned')==len(plan) and
            summary.get('passed')==len(plan) and len(rows)==len(plan) and
            {r.get('case') for r in rows}==expected and all(r.get('status')=='passed' for r in rows)):
        raise RuntimeError('auto-stop requires every planned case to pass')
    manifest=json.loads((out/'manifest.json').read_text())
    if manifest.get('backend')!='ssm' or manifest.get('campaign')!=job['campaign'] or manifest.get('matrix')!=plan or manifest.get('instances')!=job['instances']:
        raise RuntimeError('auto-stop manifest does not match this job')
    archive=json.loads((out/'archive-status.json').read_text())
    files=archive.get('files',[])
    required={'manifest.json','summary.json','runs.csv','conditions.csv',
              'comparison-summary.csv','report.md','analysis-provenance.json'}
    if job['study']=='presentation':
        figures=['recovery','healthy-cost']
        if set(job['scenarios']) & {'cpu-observer','cpu-contention-observer','delay-udp','delay-both'}:figures.append('stability')
        required.update('figures/'+name+ext for name in figures for ext in ('.png','.svg'))
    if archive.get('state')!='complete' or archive.get('prefix')!=prefix(job) or not required.issubset(files):
        raise RuntimeError('auto-stop requires a complete matching archive')
    for name in files:
        path=PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or str(path)!=name:
            raise RuntimeError('invalid archive artifact path')
    # Read back the final marker and key evidence, rather than relying on local flags alone.
    with tempfile.TemporaryDirectory(prefix='raft-stop-check-') as temp:
        for name in ('archive-status.json','manifest.json','summary.json'):
            target=Path(temp)/name
            aws.call('s3','cp',prefix(job)+'/'+name,aws.path(target),'--only-show-errors')
            if sha(target)!=sha(out/name):raise RuntimeError('S3 archive readback differs: '+name)
    for name in files:
        response=aws.call('s3api','head-object','--bucket',job['bucket'],'--key',job['campaign']+'/'+name)
        if response.get('ContentLength')!=(out/name).stat().st_size:
            raise RuntimeError('S3 artifact missing or size differs: '+name)


def stop_after_success(job,out,aws,publisher,timeout=180,poll_seconds=5):
    if not job['stopOnSuccess']:return
    verify_archive(job,out,aws)
    identity=aws.call('sts','get-caller-identity')
    if identity['Account']!=job['accountId']:raise RuntimeError('auto-stop AWS account mismatch')
    ids=job['instances'];coordinator=job['coordinator'];workers=[i for i in ids if i!=coordinator]
    def describe(wanted):
        response=aws.call('ec2','describe-instances','--instance-ids',*wanted)
        nodes={n['InstanceId']:n for r in response['Reservations'] for n in r['Instances']}
        if set(nodes)!=set(wanted):raise RuntimeError('auto-stop target set differs')
        for node in nodes.values():
            if node.get('InstanceType')!='m6gd.medium' or node.get('RootDeviceType')!='ebs' or not any(
                    'openkache' in t['Value'].lower() for t in node.get('Tags',[])):
                raise RuntimeError('auto-stop target identity/root disk/tag mismatch')
            if node['State']['Name'] not in ('running','stopping','stopped'):
                raise RuntimeError('auto-stop target has unexpected state')
        return {i:n['State']['Name'] for i,n in nodes.items()}
    states=describe(ids)
    if states[coordinator]!='running':raise RuntimeError('coordinator must still be running')
    record=dict(enabled=True,instances=ids,coordinator=coordinator,archiveVerified=True)
    def mark(phase,**details):
        record.update(phase=phase,updatedAtEpochNs=time.time_ns(),**details)
        path=Path(out).parent/'auto-stop.json';atomic_json(path,record)
        aws.call('s3','cp',aws.path(path),prefix(job)+'/control/auto-stop.json','--only-show-errors')
        publisher.update(autoStop=dict(record));publisher.upload()
    def request(wanted):
        response=aws.call('ec2','stop-instances','--instance-ids',*wanted)
        changes=response.get('StoppingInstances',[])
        if {c.get('InstanceId') for c in changes}!=set(wanted) or any(
                c.get('CurrentState',{}).get('Name') not in ('stopping','stopped') for c in changes):
            raise RuntimeError('unexpected stop-instances response')
        return response
    mark('stopping-workers',observedStates=states)
    running=[i for i in workers if states[i]=='running']
    if running:request(running)
    deadline=time.monotonic()+timeout
    while True:
        observed=describe(workers)
        if all(s=='stopped' for s in observed.values()):break
        if time.monotonic()>=deadline:raise TimeoutError('worker stop not confirmed; coordinator retained')
        time.sleep(poll_seconds)
    # Persist intent before stopping the machine that must upload it. Do not claim its final state.
    mark('coordinator-stop-submitting',workersStopped=True,observedStates=observed,coordinatorStopConfirmed=False)
    request([coordinator])
    # The API can shut down this VM before any further code executes. Read-only status verifies all three later.
    record.update(phase='coordinator-stop-accepted')
    atomic_json(Path(out).parent/'auto-stop.json',record)
