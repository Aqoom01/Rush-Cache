#!/usr/bin/env python3
"""Read-only inspection of an existing integration backup and its owned instances."""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'experiments/system-integration/ec2'))
from cloud_archive import Aws,BUCKET,check,digest,owned_instances,prefix,read_archive_metadata,validate_binding


def diagnose(pipeline,job,account,work,aws):
    check(str(pipeline).isdigit() and str(job).isdigit(),'source pipeline/job must be numeric')
    work=Path(work);work.mkdir(parents=True,exist_ok=False)
    campaign='ci-'+str(pipeline);batch=campaign+'-'+str(job)
    aws.get('bundles/integration/jobs/'+campaign+'/'+batch+'.json',work/'source-job.json')
    source=json.loads((work/'source-job.json').read_text());binding=source['binding'];validate_binding(binding)
    check(binding['campaign']==campaign and binding['batch']==batch and binding['account']==account,'wrong source binding')
    instances=owned_instances(binding,aws)
    base=prefix(binding);aws.get(base+'UPLOAD_COMPLETE.json',work/'pointer.json')
    pointer=json.loads((work/'pointer.json').read_text());check(pointer['binding']==binding,'pointer belongs to another run')
    key=pointer['manifestKey'];check(key.startswith(base+'attempt-') and key.endswith('/manifest.json'),'wrong manifest prefix')
    aws.get(key,work/'manifest.json',pointer['manifestVersionId'])
    check(digest(work/'manifest.json')==pointer['manifestSha256'],'manifest checksum mismatch')
    manifest=json.loads((work/'manifest.json').read_text());check(manifest['binding']==binding,'manifest binding differs')
    archive=manifest['archive'];check(archive['key']==key.removesuffix('manifest.json')+'results.tar.gz','wrong archive prefix')
    aws.get(archive['key'],work/'results.tar.gz',archive['versionId'])
    metadata=read_archive_metadata(work/'results.tar.gz',manifest)
    records=json.loads(metadata.get('batch-results.json',b'[]'));cases=[]
    for row in range(binding['expectedCases']):
        stem='row-%06d/'%row;case={'row':row}
        for name in ('cleanup','collection','RUN_FAILED'):
            raw=metadata.get(stem+'run/'+name+'.json')
            if raw is not None:case[name]=json.loads(raw)
        for name in ('console','batch-driver'):
            raw=metadata.get(stem+name+'.log')
            if raw is not None:case[name+'Tail']=bytes(raw).decode(errors='replace')
        cases.append(case)
    report=dict(stage='backup-diagnostics',readOnly=True,collectionVerified=False,binding=binding,
                collectionComplete=manifest['collectionComplete'],collectionProblem=manifest.get('collectionProblem'),
                filesHashed=len(manifest['files']),manifestKey=key,
                instances=[dict(instanceId=i['InstanceId'],state=i.get('State',{}).get('Name')) for i in instances],
                terminalRecords=[dict(row=r.get('row'),status=r.get('status'),exitCode=r.get('exitCode')) for r in records],
                cases=cases,workerFailure=bytes(metadata.get('worker-failure.log',b'')).decode(errors='replace'))
    (work/'diagnostics.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2),flush=True)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pipeline',required=True);parser.add_argument('--source-job',required=True)
    parser.add_argument('--account',required=True);parser.add_argument('--work',required=True)
    parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    if not args.execute:
        print(json.dumps(dict(execute=False,readOnly=True,sourcePipeline=args.source_pipeline,sourceJob=args.source_job)));return
    check(os.environ.get('GITLAB_CI')=='true' and os.environ.get('CI_COMMIT_REF_PROTECTED')=='true','protected external runner required')
    diagnose(args.source_pipeline,args.source_job,args.account,args.work,Aws())


if __name__=='__main__':main()
