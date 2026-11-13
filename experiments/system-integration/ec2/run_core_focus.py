#!/usr/bin/env python3
"""Reproducible core A1×A2 screening; reuses the hash-verified native batch controller."""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import random
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--execute',action='store_true')
    args = parser.parse_args()
    bundle=Path(args.bundle).resolve();output=Path(args.output).resolve()
    rows=[]
    for a1,a2,seed in itertools.product(('PB','PB_FENCED','RAFT','VR','CHAIN','CRAQ','ROSE'),
                                      ('O00','V01','V02','V03','V04','V05','V06','V07'),(1,2)):
        for read in (('ADAPTIVE','ROUND_ROBIN') if a2=='V06' else ('ADAPTIVE',)):
            rows.append(dict(a1=a1,a2=a2,read_selection=read,fd='F3',distribution='RANGE',split='NONE',
                             placement='NONE',scaler='fixed',scheduler='fifo',copy='PRE',
                             drain='preflight-all',scenario='NORMAL',workload='HOT_RANGE',seed=seed,key_mode='DEFAULT'))
    random.Random(20261113).shuffle(rows)
    assert len(rows)==126
    if not args.execute:
        print(json.dumps(dict(execute=False,runs=len(rows),bundle=str(bundle),output=str(output),
                              rate=1000,measurementSeconds=30,warmupSeconds=5,firstRows=rows[:3]),indent=2))
        return
    output.mkdir(parents=True,exist_ok=False)
    matrix=output/'a1-a2-screen.csv'
    with matrix.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    command=[sys.executable,str(bundle/'tools/integration.py'),'batch','--matrix',str(matrix),
             '--bundle',str(bundle),'--output',str(output/'a1-a2-screen'),'--limit','126',
             '--execute','--timeout','120']
    common=['--groups','2','--initial-groups','2','--keys','128','--value-bytes','1024','--read','80',
            '--concurrency','64','--requests','30000','--rate','1000','--warmup','5000']
    command.extend('--plan-arg='+arg for arg in common)
    manifest=dict(startedUtcNs=time.time_ns(),startedMonotonicNs=time.monotonic_ns(),command=command,
                  controllerSha256=hashlib.sha256((bundle/'tools/integration.py').read_bytes()).hexdigest(),
                  driverSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  libraryReceipt=(bundle/'BUNDLE_SHA256SUMS').read_text(),matrixSha256=hashlib.sha256(matrix.read_bytes()).hexdigest(),
                  measuredSecondsPerRun=30,warmupSecondsPerRun=5,rate=1000,environment='local-shared-host',
                  notes=['Randomized order; no concurrent experiment cases. Same workload/seed/rate must have the same request tape.',
                         'Screening at an overloaded baseline; not admitted requests are retained. Not a maximum-throughput benchmark or EC2 result.'])
    (output/'campaign-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(dict(stage='a1-a2-screen',runs=126,output=str(output),started=True)),flush=True)
    started=time.monotonic()
    with (output/'screen-console.log').open('w') as log:
        result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
    (output/'screen-process-complete.json').write_text(json.dumps(dict(exitCode=result.returncode,wallSeconds=time.monotonic()-started,completedUtcNs=time.time_ns()),indent=2)+'\n')
    print(json.dumps(dict(stage='a1-a2-screen',exitCode=result.returncode,wallSeconds=time.monotonic()-started)),flush=True)
    raise SystemExit(result.returncode)


if __name__=='__main__':
    main()
