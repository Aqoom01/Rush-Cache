"""Long-lived coordinator owned by systemd, independent of SSM and the laptop."""
import argparse
from contextlib import redirect_stdout,redirect_stderr
import fcntl
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
import traceback
from types import SimpleNamespace

from ec2_ssm import AWS
from raft_campaign_plan import matrix
from raft_ec2_job import atomic_json,validate,prefix,sha,unit
from run_raft_campaign import run,report,SSM
from raft_ec2_stop import check_stop_permission,stop_after_success


def instance_role_environment():
    # Do not depend on a copied SSO session, laptop profile or static access key.
    for key in ('AWS_PROFILE','AWS_DEFAULT_PROFILE','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY',
                'AWS_SESSION_TOKEN','AWS_SECURITY_TOKEN','AWS_WEB_IDENTITY_TOKEN_FILE','AWS_ROLE_ARN',
                'AWS_CONTAINER_CREDENTIALS_FULL_URI','AWS_CONTAINER_CREDENTIALS_RELATIVE_URI','AWS_EC2_METADATA_DISABLED'):
        os.environ.pop(key,None)
    os.environ['AWS_CONFIG_FILE']='/dev/null';os.environ['AWS_SHARED_CREDENTIALS_FILE']='/dev/null'


class Publisher:
    def __init__(self,root,job,aws):
        self.root=root;self.job=job;self.aws=aws;self.lock=threading.RLock();self.upload_lock=threading.Lock()
        self.state=dict(campaign=job['campaign'],coordinator=job['coordinator'],pid=os.getpid(),
                        state='starting',phase='initializing',activeCase=None,startedAtEpochNs=time.time_ns(),
                        planned=len(matrix(job['repeat'],job['detectors'],job['scenarios'],job['study'],job['coreRepeat'])),
                        passed=0,failed=0,pending=0,resultPrefix=prefix(job),
                        autoStop=dict(enabled=job['stopOnSuccess'],phase='pending' if job['stopOnSuccess'] else 'disabled'))
        self.state['pending']=self.state['planned']
    def update(self,**values):
        with self.lock:
            self.state.update(values);atomic_json(self.root/'worker-state.json',self.state)
    def upload(self):
        with self.upload_lock:
            with self.lock:
                record={**self.state,'heartbeatEpochNs':time.time_ns()}
            try:
                summary=json.loads((self.root/'results'/'summary.json').read_text())
                record.update(passed=summary['passed'],failed=sum(c['status']=='failed' for c in summary['cases']),
                              pending=sum(c['status']=='pending' for c in summary['cases']))
            except (FileNotFoundError,json.JSONDecodeError): pass
            file=self.root/'status-upload.json';atomic_json(file,record)
            self.aws.call('s3','cp',str(file),prefix(self.job)+'/control/status.json','--only-show-errors')
            log=self.root/'coordinator.log'
            if log.exists(): self.aws.call('s3','cp',str(log),prefix(self.job)+'/control/coordinator.log','--only-show-errors')
    def progress(self,event,manifest,details):
        values=dict(phase=event)
        if event=='backend-ready': values['state']='running'
        if event in ('case-started','case-ready'):
            values['activeCase']=details
        if event=='case-finished':
            values.update(activeCase=None,lastFinishedCase=details['case'])
        self.update(**values)
        if event=='case-finished':
            case=details['case']
            for name in ('result.json','outcome.json'):
                path=self.root/'results'/case/name
                if path.exists():
                    self.aws.call('s3','cp',str(path),prefix(self.job)+'/checkpoints/'+case+'/'+name,'--only-show-errors')
        self.upload()


def run_job(job_file,resume=False,local_validation_lib=None,aws=None,heartbeat_seconds=30):
    job_file=Path(job_file).resolve();root=job_file.parent
    job=validate(json.loads(job_file.read_text()))
    for name,digest in job['sourceSha256'].items():
        if sha(root/'source'/name)!=digest: raise ValueError('coordinator source changed: '+name)
    if sha(root/'dist.tar')!=job['distSha256']: raise ValueError('coordinator dist changed')
    out=root/'results'
    # flock is held for the entire process. Duplicate launch cannot duplicate a campaign.
    with (root/'coordinator.lock').open('a') as lease:
        try: fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise RuntimeError('coordinator already running') from None
        if out.exists() and not resume: raise RuntimeError('results exist; use explicit resume')
        previous=json.loads((root/'worker-state.json').read_text()) if (root/'worker-state.json').exists() else {}
        if resume and previous.get('activeCase'):
            raise RuntimeError('previous coordinator stopped during an active case; inspect remote commands and fault cleanup before resuming')
        if local_validation_lib is None: instance_role_environment()
        aws=aws or AWS(profile=None,region=job['region'])
        publisher=Publisher(root,job,aws);stop=threading.Event();thread=None
        args=SimpleNamespace(local=local_validation_lib is not None,lib=Path(local_validation_lib) if local_validation_lib else None,
                             out=out,dist=root/'dist.tar',instances=','.join(job['instances']),bucket=job['bucket'],
                             data_root=job['dataRoot'],profile=None,aws_bin='aws',region=job['region'],
                             study=job['study'],repeat=job['repeat'],core_repeat=job['coreRepeat'],
                             detectors=job['detectors'],scenarios=job['scenarios'],resume=resume and (out/'manifest.json').exists(),
                             plan_only=False,plot_python=sys.executable,campaign_id=job['campaign'],progress=publisher.progress,
                             execution_controller=dict(mode='local-detached-validation' if local_validation_lib else 'ec2-systemd',
                                                       instance=job['coordinator'],unit=unit(job)))
        def heartbeat():
            while not stop.wait(heartbeat_seconds):
                try: publisher.upload()
                except Exception as error: print('Heartbeat upload failed:',error,flush=True)
        def interrupted(*_): raise KeyboardInterrupt('coordinator service stopped')
        old_handlers={s:signal.signal(s,interrupted) for s in (signal.SIGTERM,signal.SIGINT)}
        result=1
        with (root/'coordinator.log').open('a',buffering=1) as log,redirect_stdout(log),redirect_stderr(log):
            try:
                publisher.update();publisher.upload()
                if local_validation_lib is None:check_stop_permission(job,aws)
                thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
                print('Coordinator start',job['campaign'],'resume=',args.resume,flush=True)
                run(args)
                # The local validation backend exercises the same final archive path via a file-backed AWS adapter.
                if local_validation_lib:
                    archive=SSM.__new__(SSM);archive.aws=aws;archive.s3=prefix(job);archive.publish(out,full=True)
                archive=json.loads((out/'archive-status.json').read_text())
                if archive.get('state')!='complete': raise RuntimeError('final S3 archive incomplete')
                publisher.update(state='completed',phase='completed',activeCase=None,finishedAtEpochNs=time.time_ns(),
                    autoStop=dict(enabled=job['stopOnSuccess'],phase='disabled' if not job['stopOnSuccess'] else
                                  'skipped-local-validation' if local_validation_lib else 'pending'))
                result=0
            except BaseException as error:
                traceback.print_exc()
                publisher.update(state='interrupted' if isinstance(error,KeyboardInterrupt) else 'failed',
                                 phase='stopped',error=repr(error),finishedAtEpochNs=time.time_ns())
                if (out/'manifest.json').exists():
                    try:
                        report(args)
                        archive=SSM.__new__(SSM);archive.aws=aws;archive.s3=prefix(job);archive.publish(out,full=True)
                    except Exception as upload_error: print('Partial report/archive failed:',upload_error,flush=True)
            finally:
                stop.set()
                if thread is not None: thread.join(timeout=125)
                try: publisher.upload()
                except Exception as error:
                    publisher.update(state='publication-failed',error=repr(error));result=1
                    print('Final status upload failed:',error,flush=True)
                for s,handler in old_handlers.items(): signal.signal(s,handler)
            if result==0 and job['stopOnSuccess'] and local_validation_lib is None:
                try:stop_after_success(job,out,aws,publisher)
                except Exception as error:
                    # Scientific completion stays distinct from a failed shutdown request.
                    print('Auto-stop failed; inspect/retry:',error,flush=True)
                    publisher.update(phase='auto-stop-failed',autoStop=dict(enabled=True,phase='failed',error=repr(error)))
                    try:publisher.upload()
                    except Exception as upload_error:print('Auto-stop failure upload failed:',upload_error,flush=True)
                    result=1
        return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--job',type=Path,required=True)
    parser.add_argument('--resume',action='store_true')
    # Offline integration only: never used by the EC2 launcher.
    parser.add_argument('--local-validation-lib',type=Path,help=argparse.SUPPRESS)
    args=parser.parse_args()
    raise SystemExit(run_job(args.job,args.resume,args.local_validation_lib))
