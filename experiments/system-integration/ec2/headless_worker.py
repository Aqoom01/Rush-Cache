#!/usr/bin/env python3
"""Runs on the gateway under systemd; instance-role credentials upload evidence only."""
import argparse
import csv
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
from cloud_archive import Aws,check,upload,validate_binding


def child_running(pid):
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():continue
        try:
            stat=(path/'stat').read_text();fields=stat[stat.rfind(')')+2:].split()
            if int(fields[1])==pid and fields[0]!='Z':return True
        except (FileNotFoundError,ProcessLookupError,PermissionError):pass
    return False


def execute(job,job_file,base=None):
    import shutil
    binding=job['binding'];validate_binding(binding)
    check(len(job['rows'])==binding['expectedCases'],'wrong row count')
    per_row=job.get('perRowArguments',[[] for _ in job['rows']])
    check(len(per_row)==len(job['rows']),'wrong per-row workload count')
    base=Path('/opt/openkache/integration') if base is None else Path(base)
    directory=base/'jobs'/binding['batch'];directory.mkdir(parents=True,exist_ok=True)
    matrix=directory/'matrix.csv'
    with matrix.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(job['rows'][0]));writer.writeheader();writer.writerows(job['rows'])
    from cloud_archive import digest
    check(digest(matrix)==binding['matrixSha256'],'job matrix hash mismatch')
    result=base/'results'/binding['batch'];check(not result.exists(),'result directory already exists');result.mkdir(parents=True)
    (base/'topology.json').write_text(json.dumps(job['topology'],indent=2)+'\n')
    (result/'matrix.csv').write_bytes(matrix.read_bytes());(result/'binding.json').write_text(json.dumps(binding,sort_keys=True)+'\n')
    (result/'job.json').write_text(json.dumps(job,sort_keys=True,indent=2)+'\n')
    tool=base/'bundle/tools/integration.py';records=[];code=0;stop=False
    def request_stop(*_):
        nonlocal stop
        stop=True
    signal.signal(signal.SIGTERM,request_stop);signal.signal(signal.SIGINT,request_stop)
    try:
        for index,row in enumerate(job['rows']):
            if stop:break
            single=directory/('input-%06d.csv'%index)
            with single.open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=list(row));writer.writeheader();writer.writerow(row)
            out=directory/('case-%06d'%index)
            command=[sys.executable,str(tool),'batch','--mode','ec2','--execute','--bundle',str(base/'bundle'),
                     '--matrix',str(single),'--output',str(out),'--limit','1',
                     '--topology',str(base/'topology.json'),'--identity','/home/kacheexp/.ssh/id_ed25519',
                     '--known-hosts',str(base/'known_hosts'),'--timeout',str(job['caseTimeoutSeconds'])]
            command.extend('--plan-arg='+arg for arg in job['planArguments']+per_row[index])
            log_file=directory/('controller-%06d.log'%index)
            with log_file.open('w') as log:
                process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                # Finish the current case and its cleanup on TERM; never signal a
                # waiting batch parent in a way that kills cleanup in its child.
                while process.poll() is None:time.sleep(.5)
            case=result/('row-%06d'%index)
            if (out/'row-000000').exists():shutil.move(str(out/'row-000000'),case)
            else:case.mkdir()
            shutil.copyfile(log_file,case/'batch-driver.log')
            source=out/'batch-results.json'
            if source.exists():
                raw=json.loads(source.read_text());check(len(raw)==1,'unexpected single-case result')
                record=raw[0];(case/'original-batch-record.json').write_text(json.dumps(record,indent=2)+'\n')
            else:record=dict(status='RUN_ERROR',exitCode=process.returncode,result=None,cleanup=None)
            if process.returncode and record['status']=='PASS':record.update(status='RUN_ERROR',exitCode=process.returncode)
            record.update(row=index,selectors=row,path=str(case));records.append(record)
            temporary=result/'batch-results.json.tmp';temporary.write_text(json.dumps(records,indent=2)+'\n');temporary.replace(result/'batch-results.json')
            if record['status']!='PASS':code=1
            if record['status']=='CLEANUP_INCOMPLETE' or not source.exists():break
    except BaseException:
        code=1;(result/'worker-failure.log').write_text(traceback.format_exc())
    (result/'worker-result.json').write_text(json.dumps(dict(controllerExitCode=code,interrupted=stop,completedUtcNs=time.time_ns()))+'\n')
    for retry in range(3):
        try:
            pointer=upload(result,binding,directory/('snapshot-'+str(retry)),Aws())
            print(json.dumps(dict(uploaded=True,pointer=pointer)),flush=True)
            return code
        except Exception:
            (directory/('upload-error-'+str(retry)+'.log')).write_text(traceback.format_exc())
            if retry<2:time.sleep(5)
    raise RuntimeError('S3 upload failed; results remain on EBS, deletion must stay blocked')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True)
    args=p.parse_args();path=Path(args.job);raise SystemExit(execute(json.loads(path.read_text()),path))


if __name__=='__main__':main()
