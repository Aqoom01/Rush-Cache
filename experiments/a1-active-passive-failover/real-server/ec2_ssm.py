"""Small AWS CLI/SSM adapter; credentials stay in the caller's configured profile."""
import base64
import json
import shlex
import subprocess
import time


class AWS:
    def __init__(self,binary='aws',profile=None,region='ap-northeast-2'):
        self.binary,self.profile,self.region=binary,profile,region

    def call(self,*args):
        argv=[self.binary,*args,'--region',self.region,'--no-cli-pager','--output','json']
        if self.profile:
            argv+=['--profile',self.profile]
        result=subprocess.run(argv,capture_output=True,text=True,timeout=120)
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def path(self,path):
        if self.binary.lower().endswith('.exe'):
            return subprocess.check_output(['wslpath','-w',str(path.resolve())],text=True).strip()
        return str(path.resolve())

    def shell(self,instance,script,timeout=120):
        encoded=base64.b64encode(script.encode()).decode()
        command='printf %s '+shlex.quote(encoded)+' | base64 -d | bash -se'
        response=self.call('ssm','send-command','--instance-ids',instance,'--document-name','AWS-RunShellScript',
                           '--parameters',json.dumps({'commands':[command],'executionTimeout':[str(timeout)]}),
                           '--timeout-seconds','60')
        command_id=response['Command']['CommandId']
        deadline=time.monotonic()+timeout+90
        while time.monotonic()<deadline:
            try:
                result=self.call('ssm','get-command-invocation','--command-id',command_id,'--instance-id',instance)
            except RuntimeError as error:
                if 'InvocationDoesNotExist' not in str(error):
                    raise
                time.sleep(1)
                continue
            if result['Status'] in ('Success','Failed','TimedOut','Cancelled'):
                if result['Status']!='Success':
                    raise RuntimeError(f'SSM {command_id} {instance}: {result}')
                return result['StandardOutputContent']
            time.sleep(1)
        raise TimeoutError(f'SSM command {command_id} still running; inspect before retrying')
