"""Actual detached-process/JVM validation with file-backed S3; never connects to AWS/systemd."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time
from types import SimpleNamespace

from raft_ec2_job import prepare,HERE,atomic_json


def validate_detached(out,lib,dist):
    out.mkdir(parents=True,exist_ok=False)
    args=SimpleNamespace(out=out/'prepared',dist=dist,account_id='123456789012',region='ap-northeast-2',
        bucket='offline-validation',instances='i-aaa,i-bbb,i-ccc',coordinator=None,data_root='/mnt/openkache-data',
        remote_python='/usr/bin/python3',max_hours=1,study='validation',repeat=1,core_repeat=1,
        detectors=['FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'],scenarios=['healthy','crash'])
    job=prepare(args);root=out/'coordinator';root.mkdir();(root/'source').mkdir()
    for name in ('job.json','dist.tar'):shutil.copyfile(args.out/name,root/name)
    with tarfile.open(args.out/'source.tar') as archive:
        for member in archive.getmembers():
            (root/'source'/member.name).write_bytes(archive.extractfile(member).read())
    bin_dir=out/'bin';bin_dir.mkdir()
    target=bin_dir/'aws';shutil.copyfile(HERE/'raft_file_aws.py',target);target.chmod(0o755)
    env={**os.environ,'PATH':str(bin_dir)+':'+os.environ['PATH'],'OPENKACHE_VALIDATION_S3':str(out/'s3')}
    argv=[sys.executable,str(root/'source'/'raft_ec2_worker.py'),'--job',str(root/'job.json'),
          '--local-validation-lib',str(lib.resolve())]
    # This short-lived launcher models the laptop connection. It exits before the campaign completes.
    helper=('import json,subprocess,sys\n'
            'log=open(sys.argv[2],"a")\n'
            'p=subprocess.Popen(json.loads(sys.argv[1]),stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)\n'
            'print(p.pid)\n')
    parent=subprocess.run([sys.executable,'-c',helper,json.dumps(argv),str(root/'service.log')],
                          capture_output=True,text=True,env=env,check=True,timeout=5)
    pid=int(parent.stdout);os.kill(pid,0)
    print('Launcher exited; independent coordinator PID',pid,flush=True)
    status=out/'s3'/job['bucket']/job['campaign']/'control'/'status.json'
    deadline=time.monotonic()+300;last=None
    while time.monotonic()<deadline:
        try:state=json.loads(status.read_text())
        except (FileNotFoundError,json.JSONDecodeError):time.sleep(.2);continue
        mark=(state['state'],state['passed'])
        if mark!=last:print('Coordinator',mark,flush=True);last=mark
        if state['state']=='completed':break
        if state['state'] in ('failed','interrupted','publication-failed'):
            raise RuntimeError((root/'coordinator.log').read_text()[-10000:])
        time.sleep(.2)
    else:raise TimeoutError('independent coordinator did not complete; inspect '+str(root))
    summary=json.loads((root/'results'/'summary.json').read_text())
    assert summary['complete'] and summary['passed']==6
    archive=out/'s3'/job['bucket']/job['campaign']
    assert json.loads((archive/'archive-status.json').read_text())['state']=='complete'
    assert len(list((archive/'checkpoints').glob('*/result.json')))==6
    assert json.loads((root/'worker-state.json').read_text())['autoStop']['phase']=='skipped-local-validation'
    attempts=sorted((root/'results').glob('*/attempt-*'))
    subprocess.run(argv+['--resume'],env=env,check=True,timeout=60)
    assert attempts==sorted((root/'results').glob('*/attempt-*'))
    record=dict(awsMode='file-backed S3 only; no AWS or host systemd calls',campaign=job['campaign'],
                launcherExitedBeforeCompletion=True,passed=summary['passed'],planned=summary['planned'],
                checkpointResults=6,completedResumeCreatedNoAttempts=True,
                autoStopSkippedForLocalValidation=True,
                coordinator=str(root),archive=str(archive))
    atomic_json(out/'validation-summary.json',record)
    print(json.dumps(record,indent=2),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True);parser.add_argument('--lib',type=Path,required=True)
    parser.add_argument('--dist',type=Path,required=True)
    args=parser.parse_args();validate_detached(args.out.resolve(),args.lib,args.dist)
