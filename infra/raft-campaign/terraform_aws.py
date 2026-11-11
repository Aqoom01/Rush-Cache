"""Run Terraform with short-lived CLI credentials in process environment only; never print keys."""
import argparse
import json
import os
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--aws-bin',required=True);parser.add_argument('--profile',default='openkache')
parser.add_argument('--terraform',required=True);args,command=parser.parse_known_args()
if not command:parser.error('Terraform command required')
result=subprocess.run([args.aws_bin,'configure','export-credentials','--profile',args.profile,'--format','process'],capture_output=True,text=True,check=True)
credentials=json.loads(result.stdout);env=os.environ.copy()
env.pop('AWS_PROFILE',None);env.pop('AWS_DEFAULT_PROFILE',None)
env.update(AWS_ACCESS_KEY_ID=credentials['AccessKeyId'],AWS_SECRET_ACCESS_KEY=credentials['SecretAccessKey'],AWS_SESSION_TOKEN=credentials['SessionToken'],AWS_REGION='ap-northeast-2')
raise SystemExit(subprocess.run([args.terraform,*command],env=env,cwd=Path(__file__).resolve().parent).returncode)
