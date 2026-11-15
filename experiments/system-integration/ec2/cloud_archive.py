#!/usr/bin/env python3
"""S3 backup proof. ETags are not treated as checksums; deletion requires a fresh download."""
import argparse
import base64
import csv
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile
import uuid

BUCKET='openstd-s15p31d111-results'
REGION='ap-northeast-2'


def check(condition,message):
    if not condition:raise ValueError(message)


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for data in iter(lambda:stream.read(1024*1024),b''):h.update(data)
    return h.hexdigest()


def safe_name(name):
    p=PurePosixPath(name)
    return bool(name) and not p.is_absolute() and '..' not in p.parts and '.' not in p.parts and str(p)==name


class Aws:
    def call(self,service,operation,**arguments):
        command=['aws','--region',REGION,service,operation]
        for key,value in arguments.items():
            command.append('--'+key.replace('_','-'))
            command.extend(map(str,value) if isinstance(value,list) else [str(value)])
        command+=['--output','json']
        p=subprocess.run(command,capture_output=True,text=True,timeout=180)
        check(p.returncode==0,'AWS '+service+'/'+operation+' failed: '+p.stderr[-1500:])
        return json.loads(p.stdout or '{}')

    def get(self,key,path,version=None):
        command=['aws','--region',REGION,'s3api','get-object','--bucket',BUCKET,'--key',key]
        p=subprocess.run(command+[str(path),'--output','json'],capture_output=True,text=True,timeout=180)
        check(p.returncode==0,'S3 download failed: '+p.stderr[-1000:])
        metadata=json.loads(p.stdout)
        if version:check(metadata.get('VersionId')==version,'S3 object version changed')
        return metadata

    def put(self,key,path):
        checksum=base64.b64encode(bytes.fromhex(digest(path))).decode()
        return self.call('s3api','put-object',bucket=BUCKET,key=key,body=str(path),
                         server_side_encryption='AES256',checksum_algorithm='SHA256',checksum_sha256=checksum)


def validate_binding(binding):
    check(set(binding)=={'campaign','batch','account','instanceIds','bundleSha256','matrixSha256','expectedCases'},'invalid backup binding fields')
    check(all(re.fullmatch('[a-zA-Z0-9_-]{1,48}',binding[k]) for k in ('campaign','batch')),'invalid campaign/batch')
    check(re.fullmatch('[0-9]{12}',binding['account']),'invalid account')
    check(len(binding['instanceIds'])==8 and len(set(binding['instanceIds']))==8,'exactly eight owned instances required')
    check(all(re.fullmatch('i-[0-9a-f]{8,17}',i) for i in binding['instanceIds']),'invalid instance ID')
    check(all(re.fullmatch('[0-9a-f]{64}',binding[k]) for k in ('bundleSha256','matrixSha256')),'invalid input hashes')
    check(isinstance(binding['expectedCases'],int) and 1<=binding['expectedCases']<=20,'batch must have 1..20 cases')


def prefix(binding):
    validate_binding(binding)
    return 'results/system-integration-core/'+binding['campaign']+'/'+binding['batch']+'/'


def collected(root,binding):
    """Backup success is separate from experiment success. Failed experiments keep their evidence."""
    root=Path(root)
    records=json.loads((root/'batch-results.json').read_text())
    check(len(records)==binding['expectedCases'],'not all requested cases have terminal records')
    check({r['row'] for r in records}==set(range(binding['expectedCases'])),'duplicate or unexpected case indices')
    check(digest(root/'matrix.csv')==binding['matrixSha256'],'matrix changed')
    check(json.loads((root/'binding.json').read_text())==binding,'binding changed')
    with (root/'matrix.csv').open(newline='') as stream:matrix=list(csv.DictReader(stream))
    check(len(matrix)==len(records),'matrix/result count differs')
    for record in records:
        check(record['status'] in ('PASS','FAILED_VALIDATION','REJECTED_CONFIG','UNSUPPORTED','RUN_ERROR'),'case did not finish cleanly')
        check({k:str(v) for k,v in record['selectors'].items()}==matrix[record['row']],'wrong case selectors')
        row=root/('row-%06d'%record['row'])
        check((row/'console.log').is_file() and (row/'plan/plan.json').is_file(),'missing case input/log')
        run=row/'run'
        if (run/'manifest.json').exists():
            for file in ('cleanup.json','collection.json'):
                check(json.loads((run/file).read_text()).get('complete') is True,'incomplete '+file)
        if record['status']=='PASS':
            check((run/'result.json').is_file() and (run/'RUN_COMPLETE').is_file(),'PASS without collected result')
    return records


def pack(root,binding,destination):
    root=Path(root).resolve();destination=Path(destination).resolve()
    validate_binding(binding)
    try:collected(root,binding);complete=True;problem=None
    except (ValueError,KeyError,OSError,json.JSONDecodeError) as failure:complete=False;problem=str(failure)
    check(root!=destination and root not in destination.parents,'backup output must be outside result directory')
    destination.mkdir(parents=True,exist_ok=False)
    inventory=[]
    for path in sorted(root.rglob('*')):
        check(not path.is_symlink(),'symlink in experiment evidence')
        if path.is_file():
            name=path.relative_to(root).as_posix();check(safe_name(name),'unsafe evidence path')
            inventory.append(dict(path=name,size=path.stat().st_size,sha256=digest(path)))
    archive=destination/'results.tar.gz'
    with tarfile.open(archive,'w:gz') as tar:
        for entry in inventory:tar.add(root/entry['path'],arcname=entry['path'],recursive=False)
    # Reject concurrent changes rather than uploading a mismatched inventory.
    for entry in inventory:
        check(digest(root/entry['path'])==entry['sha256'],'evidence changed while archiving')
    manifest=dict(schema=1,binding=binding,collectionComplete=complete,collectionProblem=problem,files=inventory,
                  archive=dict(size=archive.stat().st_size,sha256=digest(archive)))
    (destination/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    return manifest


def upload(root,binding,destination,aws):
    manifest=pack(root,binding,destination);destination=Path(destination)
    attempt=prefix(binding)+'attempt-'+uuid.uuid4().hex+'/'
    key=attempt+'results.tar.gz';response=aws.put(key,destination/'results.tar.gz')
    version=response.get('VersionId');check(version and version!='null','versioned S3 bucket required')
    manifest['archive'].update(key=key,versionId=version)
    (destination/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    manifest_key=attempt+'manifest.json';response=aws.put(manifest_key,destination/'manifest.json')
    check(response.get('VersionId') not in (None,'null'),'versioned manifest required')
    pointer=dict(binding=binding,manifestKey=manifest_key,manifestVersionId=response['VersionId'],manifestSha256=digest(destination/'manifest.json'))
    (destination/'UPLOAD_COMPLETE.json').write_text(json.dumps(pointer,sort_keys=True)+'\n')
    aws.put(prefix(binding)+'UPLOAD_COMPLETE.json',destination/'UPLOAD_COMPLETE.json')
    return pointer


def read_archive_metadata(path,manifest):
    """Hash every archived file; return bounded metadata without claiming collection is complete."""
    check(path.stat().st_size==manifest['archive']['size'] and digest(path)==manifest['archive']['sha256'],'archive checksum/size mismatch')
    expected={f['path']:f for f in manifest['files']};check(len(expected)==len(manifest['files']),'duplicate inventory paths')
    check(all(safe_name(p) for p in expected),'unsafe inventory path')
    seen=set();small={}
    with tarfile.open(path,'r|gz') as tar:
        for member in tar:
            check(member.isfile() and safe_name(member.name) and member.name in expected and member.name not in seen,'unexpected or unsafe archive member')
            entry=expected[member.name];check(member.size==entry['size'],'member size mismatch')
            h=hashlib.sha256();keep=member.name in ('batch-results.json','binding.json','matrix.csv','worker-result.json') or bool(re.fullmatch(r'row-[0-9]{6}/run/(cleanup|collection|manifest|result|gateway-final-controller|RUN_FAILED)\.json',member.name))
            tail=member.name=='worker-failure.log' or bool(re.fullmatch(r'row-[0-9]{6}/(console|batch-driver)\.log',member.name))
            if keep:check(member.size<=8*1024*1024,'oversized evidence metadata');small[member.name]=bytearray()
            elif tail:small[member.name]=bytearray()
            with tar.extractfile(member) as stream:
                for data in iter(lambda:stream.read(1024*1024),b''):
                    h.update(data)
                    if keep:small[member.name].extend(data)
                    elif tail:small[member.name][:]=(small[member.name]+data)[-6000:]
            check(h.hexdigest()==entry['sha256'],'member checksum mismatch');seen.add(member.name)
    check(seen==set(expected),'missing evidence files')
    return small


def verify_archive(path,manifest):
    small=read_archive_metadata(path,manifest);seen={f['path'] for f in manifest['files']}
    binding=manifest['binding']
    check({'batch-results.json','binding.json','matrix.csv'}<=seen,'batch metadata missing')
    check(json.loads(small['binding.json'])==binding,'archived binding mismatch')
    check(hashlib.sha256(small['matrix.csv']).hexdigest()==binding['matrixSha256'],'archived matrix mismatch')
    matrix=list(csv.DictReader(bytes(small['matrix.csv']).decode().splitlines()))
    records=json.loads(small['batch-results.json'])
    check(len(records)==len(matrix)==binding['expectedCases'] and {r['row'] for r in records}==set(range(binding['expectedCases'])),'missing/duplicate terminal records')
    for record in records:
        stem='row-%06d/'%record['row']
        check({k:str(v) for k,v in record['selectors'].items()}==matrix[record['row']],'archived selector mismatch')
        check(record['status'] in ('PASS','FAILED_VALIDATION','REJECTED_CONFIG','UNSUPPORTED','RUN_ERROR'),'unresolved case state')
        check({stem+'console.log',stem+'plan/plan.json'}<=seen,'case input/log missing')
        if stem+'run/manifest.json' in seen:
            for file in ('cleanup.json','collection.json'):
                check(json.loads(small.get(stem+'run/'+file,b'{}')).get('complete') is True,'uncollected runtime evidence')
        if record['status']=='PASS':
            check({stem+'run/result.json',stem+'run/RUN_COMPLETE',stem+'run/manifest.json'}<=seen,'PASS result missing')
            if record['selectors'].get('operating_case'):
                checks=record.get('operatingChecks')
                check(isinstance(checks,dict) and checks and all(value is True for value in checks.values()),'operating PASS without scenario checks')
    return dict(outcomes={status:sum(r['status']==status for r in records) for status in sorted({r['status'] for r in records})},
                results=[dict(row=r['row'],selectors=r['selectors'],status=r['status'],
                              **({'operatingChecks':r.get('operatingChecks')} if r['selectors'].get('operating_case') else {}),
                              result=json.loads(small['row-%06d/run/result.json'%r['row']]) if 'row-%06d/run/result.json'%r['row'] in small else None,
                              gateway=json.loads(small['row-%06d/run/gateway-final-controller.json'%r['row']]) if 'row-%06d/run/gateway-final-controller.json'%r['row'] in small else None) for r in records])


def verify(binding,aws):
    base=prefix(binding)
    with tempfile.TemporaryDirectory(prefix='openkache-s3-verify-') as temp:
        work=Path(temp);aws.get(base+'UPLOAD_COMPLETE.json',work/'pointer.json')
        pointer=json.loads((work/'pointer.json').read_text());check(pointer['binding']==binding,'backup belongs to another run')
        check(pointer['manifestKey'].startswith(base+'attempt-') and pointer['manifestKey'].endswith('/manifest.json'),'manifest outside owned prefix')
        aws.get(pointer['manifestKey'],work/'manifest.json',pointer['manifestVersionId'])
        check(digest(work/'manifest.json')==pointer['manifestSha256'],'manifest checksum mismatch')
        manifest=json.loads((work/'manifest.json').read_text())
        check(manifest['binding']==binding,'uncollected or mismatched evidence: manifest binding differs')
        check(manifest['collectionComplete'] is True,
              'uncollected or mismatched evidence: '+str(manifest.get('collectionProblem') or 'collection not complete')
              +'; manifest=s3://'+BUCKET+'/'+pointer['manifestKey'])
        archive=manifest['archive'];check(archive['key']==pointer['manifestKey'].removesuffix('manifest.json')+'results.tar.gz','archive outside manifest attempt')
        aws.get(archive['key'],work/'results.tar.gz',archive['versionId']);outcomes=verify_archive(work/'results.tar.gz',manifest)
        return dict(verified=True,binding=binding,manifestKey=pointer['manifestKey'],manifestVersionId=pointer['manifestVersionId'],
                    manifestSha256=pointer['manifestSha256'],archiveSha256=archive['sha256'],filesVerified=len(manifest['files']),
                    experimentOutcomes=outcomes['outcomes'],experimentResults=outcomes['results'])


def owned_instances(binding,aws):
    validate_binding(binding)
    identity=aws.call('sts','get-caller-identity')
    check(identity.get('Account')==binding['account'] and identity.get('Arn')=='arn:aws:iam::'+binding['account']+':user/OpenStd','wrong AWS deletion identity')
    response=aws.call('ec2','describe-instances',instance_ids=binding['instanceIds'])
    instances=[i for r in response['Reservations'] for i in r['Instances']]
    check({i['InstanceId'] for i in instances}==set(binding['instanceIds']),'instance set changed')
    for instance in instances:
        tags={t['Key']:t['Value'] for t in instance.get('Tags',[])}
        check(tags.get('Project')=='openkache' and tags.get('ManagedBy')=='terraform-integration' and tags.get('ExperimentRun')==binding['batch'],'instance is not owned by this batch')
        check(instance['InstanceType']=='m6gd.medium','unexpected instance type')
    return instances


def guarded_terminate(binding,aws):
    receipt=verify(binding,aws)  # Always a fresh, independent S3 download, not a local success marker.
    owned_instances(binding,aws)
    response=aws.call('ec2','terminate-instances',instance_ids=binding['instanceIds'])
    check({r['InstanceId'] for r in response['TerminatingInstances']}==set(binding['instanceIds']),'partial terminate response')
    return dict(backup=receipt,terminationRequested=True,instanceIds=binding['instanceIds'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('upload','verify','terminate'))
    parser.add_argument('--binding',required=True);parser.add_argument('--results');parser.add_argument('--snapshot')
    parser.add_argument('--receipt',required=True)
    args=parser.parse_args();binding=json.loads(Path(args.binding).read_text());aws=Aws()
    if args.action=='upload':result=upload(args.results,binding,args.snapshot,aws)
    elif args.action=='verify':result=verify(binding,aws)
    else:result=guarded_terminate(binding,aws)
    Path(args.receipt).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))


if __name__=='__main__':main()
