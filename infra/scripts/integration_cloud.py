#!/usr/bin/env python3
"""Bounded GitLab-runner orchestration: Terraform -> SSM -> S3 proof -> owned cleanup.

Never run this controller on an experiment instance that it might delete.
"""
import argparse
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
from datetime import datetime

REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/'experiments/system-integration/ec2'))
from cloud_archive import Aws,BUCKET,REGION,check,digest,guarded_terminate,owned_instances,prefix,verify


def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')


def run(command,**kwargs):
    p=subprocess.run(command,capture_output=True,text=True,**kwargs)
    check(p.returncode==0,'command failed: '+str(command[:3])+'\n'+p.stderr[-2000:])
    return p.stdout


def ssm(aws,instance,script,timeout=120):
    encoded=base64.b64encode(script.encode()).decode()
    sent=aws.call('ssm','send-command',instance_ids=[instance],document_name='AWS-RunShellScript',
                  parameters=json.dumps(dict(commands=['echo '+encoded+' | base64 -d | bash'],executionTimeout=[str(timeout)])))
    command=sent['Command']['CommandId'];deadline=time.monotonic()+timeout+60
    while time.monotonic()<deadline:
        try:
            result=aws.call('ssm','get-command-invocation',command_id=command,instance_id=instance)
            if result['Status'] in ('Pending','InProgress','Delayed'):time.sleep(3);continue
            check(result['Status']=='Success','SSM failed: '+result.get('StandardErrorContent','')[-1500:])
            return result['StandardOutputContent']
        except ValueError as error:
            if 'InvocationDoesNotExist' not in str(error):raise
            time.sleep(3)
    raise TimeoutError('SSM command timeout')


def make_topology(ids,ips):
    check(len(ids)==len(ips)==8 and len(set(ids))==8 and len(set(ips))==8,'need eight distinct hosts')
    return dict(replica_hosts=ips[:6],gateway_host=ips[6],load_host=ips[7],ssh_user='kacheexp',
                remote_root='/opt/openkache/integration/runs',environment='ec2-distinct-hosts')


def validate_plan_arguments(arguments):
    allowed={'--requests','--rate','--warmup','--keys','--value-bytes','--read','--delete','--concurrency',
             '--hot-shift','--control-ms','--initial-groups','--min-groups','--override'}
    check(len(arguments)%2==0,'plan arguments must be option/value pairs')
    for option,value in zip(arguments[::2],arguments[1::2]):
        check(option in allowed,'unsupported cloud workload option: '+option)
        check('\n' not in value and '\r' not in value,'multiline plan argument')
        if option=='--override':
            key=value.split('=',1)[0]
            check('=' in value and not key.endswith('.file') and key!='native.data.root','cloud file paths are managed by the deployment')


def campaign_has_window(created_at,now):
    created=datetime.fromisoformat(created_at.replace('Z','+00:00')).timestamp()
    # No new three-hour allocation in the final three hours of the 24h budget.
    return created-300<=now<created+21*3600


def setup_ssh(aws,ids,ips):
    # Public host keys are read over authenticated SSM; no trust-on-first-use scan.
    keys=[]
    for instance in ids:
        public=ssm(aws,instance,'cat /etc/ssh/ssh_host_ed25519_key.pub').strip().split()
        check(len(public)>=2 and public[0]=='ssh-ed25519','unexpected host key')
        keys.append(public[:2])
    public=ssm(aws,ids[6],"runuser -u kacheexp -- ssh-keygen -q -t ed25519 -N '' -f /home/kacheexp/.ssh/id_ed25519\ncat /home/kacheexp/.ssh/id_ed25519.pub").strip()
    check(public.startswith('ssh-ed25519 ') and '\n' not in public,'invalid controller public key')
    authorized='from="'+ips[6]+'",no-agent-forwarding,no-port-forwarding,no-X11-forwarding '+public+'\n'
    for instance in ids:
        encoded=base64.b64encode(authorized.encode()).decode()
        ssm(aws,instance,'echo '+encoded+' | base64 -d > /home/kacheexp/.ssh/authorized_keys\nchown kacheexp:kacheexp /home/kacheexp/.ssh/authorized_keys\nchmod 600 /home/kacheexp/.ssh/authorized_keys')
    hosts=''.join(ip+' '+' '.join(key)+'\n' for ip,key in zip(ips,keys))
    encoded=base64.b64encode(hosts.encode()).decode()
    ssm(aws,ids[6],'echo '+encoded+' | base64 -d > /opt/openkache/integration/known_hosts\nchown kacheexp:kacheexp /opt/openkache/integration/known_hosts')


def identity(aws,account):
    current=aws.call('sts','get-caller-identity')
    check(current.get('Arn')=='arn:aws:iam::'+account+':user/OpenStd','OpenStd identity required')


def stop_owned(aws,batch,account):
    identity(aws,account)
    deadline=time.monotonic()+120;stopped=[]
    while True:
        response=aws.call('ec2','describe-instances',filters=json.dumps([
            dict(Name='tag:Project',Values=['openkache']),dict(Name='tag:ManagedBy',Values=['terraform-integration']),
            dict(Name='tag:ExperimentRun',Values=[batch]),dict(Name='instance-state-name',Values=['pending','running','stopping','stopped'])]))
        instances=[i for r in response['Reservations'] for i in r['Instances']]
        check(len(instances)<=8 and all(i['InstanceType']=='m6gd.medium' for i in instances),'unexpected owned instance inventory')
        for instance in instances:
            tags={t['Key']:t['Value'] for t in instance.get('Tags',[])}
            check(tags.get('Project')=='openkache' and tags.get('ManagedBy')=='terraform-integration' and tags.get('ExperimentRun')==batch,'refuse to stop a foreign instance')
        active=[i['InstanceId'] for i in instances if i['State']['Name']=='running']
        if active:aws.call('ec2','stop-instances',instance_ids=active);stopped.extend(active)
        if not any(i['State']['Name']=='pending' for i in instances):break
        check(time.monotonic()<deadline,'owned instances still pending; emergency guest shutdown remains scheduled')
        time.sleep(5)
    return dict(stopped=sorted(set(stopped)),deleted=False,reason='Unresolved cleanup; preserve existing EBS for recovery')


def terraform(args,work,*command):
    env=os.environ.copy();env['TF_DATA_DIR']=str(work/'.terraform')
    env.update(TF_VAR_integration_mode='true',TF_VAR_experiment_run=args.batch,
               TF_VAR_integration_bundle_sha256=args.bundle_sha,TF_VAR_max_lifetime_minutes='180',TF_VAR_storage_engine='nvme')
    return run(['terraform','-chdir='+str(REPO/'infra/experiment'),*command],env=env)


def resource_inventory(state):
    resources=[]
    def visit(module):
        for r in module.get('resources',[]):
            if r.get('mode')=='managed':resources.append(dict(address=r['address'],type=r['type'],id=r.get('values',{}).get('id')))
        for child in module.get('child_modules',[]):visit(child)
    visit(state.get('values',{}).get('root_module',{}))
    return resources


def validate_destroy(plan,ownership,binding):
    check(ownership['batch']==binding['batch'],'wrong state ownership snapshot')
    known={r['address']:(r['type'],r['id']) for r in ownership['resources']}
    allowed={'aws_instance','aws_vpc','aws_internet_gateway','aws_subnet','aws_route_table','aws_route_table_association',
             'aws_security_group','aws_vpc_security_group_ingress_rule','aws_vpc_security_group_egress_rule','aws_scheduler_schedule','time_static','terraform_data'}
    for change in plan.get('resource_changes',[]):
        if change.get('mode')=='data':continue
        check(change['type'] in allowed and change['change']['actions'] in (['delete'],['no-op']),'unexpected destroy target')
        if change['change']['actions']==['delete']:
            before=change['change']['before']
            check(known.get(change['address'])==(change['type'],before.get('id')),'resource was not in the created state snapshot')
            if change['type']=='aws_instance':check(before['id'] in binding['instanceIds'],'foreign instance in destroy plan')


def finalize(args,work,aws):
    binding_path=work/'binding.json'
    try:
        check(binding_path.exists(),'no complete owned inventory')
        binding=json.loads(binding_path.read_text())
        check(binding['batch']==args.batch and binding['campaign']==args.campaign and binding['account']==args.account,'cleanup binding mismatch')
        proof=guarded_terminate(binding,aws);save(work/'termination.json',proof)
        save(work/'backup-verified.json',dict(proof['backup'],phase=getattr(args,'phase','manual')))
        run(['aws','--region',REGION,'ec2','wait','instance-terminated','--instance-ids',*binding['instanceIds']],timeout=300)
        # Only this per-job Terraform state is reachable; bootstrap/S3 never belongs to it.
        terraform(args,work,'plan','-destroy','-input=false','-var=node_count=7','-var=source_commit='+args.commit,'-out='+str(work/'destroy.tfplan'))
        plan=json.loads(terraform(args,work,'show','-json',str(work/'destroy.tfplan')))
        validate_destroy(plan,json.loads((work/'resource-ownership.json').read_text()),binding)
        terraform(args,work,'apply','-input=false',str(work/'destroy.tfplan'))
        proof['terraformDestroyed']=True;save(work/'cleanup-complete.json',proof)
        receipt=work/'cleanup-complete.json';aws.put(prefix(binding)+'CLEANUP_COMPLETE.json',receipt)
        print(json.dumps(dict(stage='cleanup-complete',batch=args.batch,resultPrefix='s3://'+BUCKET+'/'+prefix(binding))),flush=True)
        return True
    except Exception as error:
        save(work/'cleanup-blocked.json',dict(error=str(error)))
        save(work/'preserved-instances.json',stop_owned(aws,args.batch,args.account))
        print(json.dumps(dict(stage='cleanup-blocked',batch=args.batch,reason=str(error),evidence=str(work))),flush=True)
        return False


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign',required=True);p.add_argument('--batch',required=True);p.add_argument('--account',required=True)
    p.add_argument('--bundle',required=True);p.add_argument('--matrix',required=True);p.add_argument('--start',type=int,default=0);p.add_argument('--limit',type=int,default=12)
    p.add_argument('--work',required=True);p.add_argument('--commit',required=True);p.add_argument('--finalize-only',action='store_true')
    p.add_argument('--plan-arg',action='append',default=[]);p.add_argument('--execute',action='store_true')
    p.add_argument('--plan-args-json',default='[]')
    p.add_argument('--plan-spec',default='')
    p.add_argument('--phase',default='manual')
    args=p.parse_args();check(0<=args.start and 1<=args.limit<=20,'bounded batches require start>=0 and limit1..20')
    extra=json.loads(args.plan_args_json);check(isinstance(extra,list) and len(extra)<=128 and all(isinstance(v,str) for v in extra),'plan args must be a JSON string array');args.plan_arg.extend(extra)
    spec=json.loads(Path(args.plan_spec).read_text()) if args.plan_spec else {}
    args.plan_arg.extend(spec.get('planArguments',[]))
    for values in spec.get('perRowArguments',[]):validate_plan_arguments(values)
    validate_plan_arguments(args.plan_arg)
    check(__import__('re').fullmatch('[A-Za-z0-9_-]{1,48}',args.batch) and __import__('re').fullmatch('[A-Za-z0-9_-]{1,48}',args.campaign),'invalid campaign/batch token')
    args.bundle_sha=digest(Path(args.bundle))
    if not args.execute:
        print(json.dumps(dict(execute=False,campaign=args.campaign,batch=args.batch,instances=8,start=args.start,limit=args.limit,
                              workflow='CI -> isolated Terraform -> SSM/systemd -> versioned S3 download/hash verification -> owned termination -> isolated-state destroy')));return
    check(os.environ.get('GITLAB_CI')=='true' and os.environ.get('CI_COMMIT_REF_PROTECTED')=='true','execution requires the protected external GitLab runner')
    check(args.campaign=='ci-'+os.environ.get('CI_PIPELINE_ID','') and args.batch==args.campaign+'-'+os.environ.get('CI_JOB_ID',''),'campaign/batch must belong to the current CI job')
    if not args.finalize_only:
        check(campaign_has_window(os.environ.get('CI_PIPELINE_CREATED_AT',''),time.time()),'24h campaign budget: insufficient window for a new three-hour batch')
    work=Path(args.work).resolve();work.mkdir(parents=True,exist_ok=True);aws=Aws();identity(aws,args.account)
    terraform(args,work,'init','-input=false','-reconfigure','-backend-config=key=integration/'+args.campaign+'/'+args.batch+'.tfstate')
    if args.finalize_only:
        check(finalize(args,work,aws),'backup not verified; instances preserved');return
    try:
        save(work/'execution-started.json',dict(campaign=args.campaign,batch=args.batch,startedUtcNs=time.time_ns()))
        # Count untagged instances as well; do not destroy or adopt existing machines.
        current=aws.call('ec2','describe-instances',filters=json.dumps([dict(Name='instance-state-name',Values=['pending','running','stopping','stopped'])]))
        count=sum(len(r['Instances']) for r in current['Reservations']);check(count+8<=9,'account instance ceiling would be exceeded')
        with Path(args.matrix).open(newline='') as stream:all_rows=list(csv.DictReader(stream))
        rows=all_rows[args.start:args.start+args.limit]
        per_row=spec.get('perRowArguments',[[] for _ in all_rows])
        check(len(per_row)==len(all_rows),'per-row argument count differs from matrix')
        check(rows,'empty batch')
        from integration import row_plan_arguments
        for row in rows:row_plan_arguments(row)
        buf=io.StringIO(newline='');writer=csv.DictWriter(buf,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
        matrix_bytes=buf.getvalue().encode();(work/'matrix.csv').write_bytes(matrix_bytes)
        aws.put('bundles/integration/'+args.bundle_sha+'.zip',Path(args.bundle))
        terraform(args,work,'plan','-input=false','-var=node_count=7','-var=source_commit='+args.commit,'-out='+str(work/'create.tfplan'))
        before=resource_inventory(json.loads(terraform(args,work,'show','-json')))
        check(not before,'refuse to adopt a nonempty Terraform state')
        terraform(args,work,'apply','-input=false',str(work/'create.tfplan'))
        save(work/'resource-ownership.json',dict(batch=args.batch,resources=resource_inventory(json.loads(terraform(args,work,'show','-json')))))
        outputs=json.loads(terraform(args,work,'output','-json'));ids=outputs['node_instance_ids']['value']+[outputs['loadgen_instance_id']['value']]
        check(len(ids)==8,'wrong Terraform inventory')
        response=aws.call('ec2','describe-instances',instance_ids=ids);by_id={i['InstanceId']:i for r in response['Reservations'] for i in r['Instances']}
        ips=[by_id[i]['PrivateIpAddress'] for i in ids]
        binding=dict(campaign=args.campaign,batch=args.batch,account=args.account,instanceIds=ids,bundleSha256=args.bundle_sha,
                     matrixSha256=hashlib.sha256(matrix_bytes).hexdigest(),expectedCases=len(rows))
        save(work/'binding.json',binding);owned_instances(binding,aws)
        print(json.dumps(dict(stage='provisioned',batch=args.batch,instances=len(ids),resultPrefix='s3://'+BUCKET+'/'+prefix(binding))),flush=True)
        deadline=time.monotonic()+900
        for instance in ids:
            while True:
                try:
                    check(ssm(aws,instance,'test "$(cat /opt/openkache/STATUS)" = ready && echo ready',30).strip()=='ready','not ready');break
                except (ValueError,TimeoutError):
                    check(time.monotonic()<deadline,'bootstrap readiness timeout');time.sleep(5)
        setup_ssh(aws,ids,ips)
        plan_args=['--groups','2','--initial-groups','1','--override','native.data.root=/mnt/nvme/openkache',*args.plan_arg]
        job=dict(binding=binding,rows=rows,topology=make_topology(ids,ips),planArguments=plan_args,caseTimeoutSeconds=600,phase=args.phase,
                 perRowArguments=per_row[args.start:args.start+args.limit],sourceCommit=args.commit)
        job['hostInventory']=[dict(instanceId=i,imageId=by_id[i].get('ImageId'),privateIp=by_id[i]['PrivateIpAddress'],
                                   availabilityZone=by_id[i].get('Placement',{}).get('AvailabilityZone')) for i in ids]
        save(work/'job.json',job);key='bundles/integration/jobs/'+args.campaign+'/'+args.batch+'.json';aws.put(key,work/'job.json')
        base='/opt/openkache/integration';remote=base+'/job.json'
        worker='exec python3 '+base+'/bundle/tools/headless_worker.py --job '+remote+' > '+base+'/worker-'+args.batch+'.log 2>&1'
        script='set -e\naws s3 cp --region '+REGION+' '+shlex.quote('s3://'+BUCKET+'/'+key)+' '+remote+'\nchown kacheexp:kacheexp '+remote+'\nsystemd-run --unit=openkache-'+args.batch+' --uid=kacheexp --property=RuntimeMaxSec=7800 --property=TimeoutStopSec=300 --property=KillMode=mixed /bin/bash -c '+shlex.quote(worker)
        ssm(aws,ids[6],script)
        print(json.dumps(dict(stage='remote-running',batch=args.batch,unit='openkache-'+args.batch)),flush=True)
        deadline=time.monotonic()+8100
        while True:
            try:
                receipt=verify(binding,aws);receipt['phase']=args.phase;save(work/'backup-verified.json',receipt);break
            except (ValueError,KeyError) as failure:
                if 'uncollected or mismatched evidence' in str(failure):raise
                check(time.monotonic()<deadline,'no verified complete S3 backup before deadline');time.sleep(15)
        check(finalize(args,work,aws),'cleanup not completed')
        check(set(receipt['experimentOutcomes'])=={'PASS'},'experiments failed; evidence verified and resources cleaned, inspect results')
    except BaseException:
        save(work/'failure.json',dict(error=__import__('traceback').format_exc()))
        if not (work/'cleanup-complete.json').exists():finalize(args,work,aws)
        raise


if __name__=='__main__':main()
