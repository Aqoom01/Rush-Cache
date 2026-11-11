"""Deploy and measure PB_FENCED through SSM on exactly three existing dedicated EC2 nodes.

No instances are created, started, stopped, or deleted. Each case uses a fresh directory.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import subprocess
import time
import uuid

HERE = Path(__file__).resolve().parent
FILES = ('pb_control.py', 'pb_bench.py', 'fenced_bench.py', 'fenced_fault.py')
TERMINAL = {'Success', 'Cancelled', 'TimedOut', 'Failed', 'Cancelling'}


def node_properties(hosts, node, detector, ports=(7000, 7001, 7002)):
    return '\n'.join([
        'protocol=PB_FENCED', f'node.id={node}',
        *[f'peer.{i}={ip}' for i, ip in enumerate(hosts)],
        f'client.port={ports[0]}', f'peer.port={ports[1]}', f'failure.port={ports[2]}',
        'admin.port=0', 'heartbeat.ms=50', 'timeout.ms=1000', 'tick.ms=10',
        f'failure.detector={detector}', 'failure.startup.grace.ms=5000',
        'failure.probe.interval.ms=200', 'failure.direct.timeout.ms=50',
        'failure.indirect.timeout.ms=50', 'failure.suspicion.ms=500',
        'failure.lifeguard.awareness.max.multiplier=8',
        'failure.lifeguard.suspicion.max.multiplier=2',
        'failure.lifeguard.max.confirmations=1', ''])


class AWS:
    def __init__(self, binary, profile, region):
        self.binary, self.profile, self.region = binary, profile, region

    def call(self, *args):
        argv = [self.binary, *args, '--region', self.region, '--no-cli-pager', '--output', 'json']
        if self.profile:
            argv += ['--profile', self.profile]
        p = subprocess.run(argv, capture_output=True, text=True, timeout=120)
        if p.returncode:
            raise RuntimeError(p.stderr.strip())
        return json.loads(p.stdout) if p.stdout.strip() else {}

    def shell(self, instance, script, timeout=120):
        # One encoded shell program avoids quoting/newline differences across CLI platforms.
        encoded = base64.b64encode(script.encode()).decode()
        cmd = 'printf %s ' + shlex.quote(encoded) + ' | base64 -d | bash -se'
        response = self.call('ssm', 'send-command', '--instance-ids', instance,
                             '--document-name', 'AWS-RunShellScript', '--parameters',
                             json.dumps({'commands': [cmd], 'executionTimeout': [str(timeout)]}),
                             '--timeout-seconds', '60')
        command_id = response['Command']['CommandId']
        deadline = time.monotonic() + timeout + 90
        while time.monotonic() < deadline:
            try:
                invocation = self.call('ssm', 'get-command-invocation', '--command-id', command_id,
                                       '--instance-id', instance)
            except RuntimeError as error:
                if 'InvocationDoesNotExist' not in str(error):
                    raise
                time.sleep(1)
                continue
            if invocation['Status'] in TERMINAL:
                if invocation['Status'] != 'Success':
                    raise RuntimeError(f'SSM {command_id} {instance}: {invocation}')
                return invocation['StandardOutputContent']
            time.sleep(1)
        raise TimeoutError(f'SSM command still running: {command_id}; inspect before retrying')

    def local_path(self, path):
        if self.binary.lower().endswith('.exe'):
            return subprocess.check_output(['wslpath', '-w', str(path.resolve())], text=True).strip()
        return str(path.resolve())


def execute(a):
    aws = AWS(a.aws_bin, a.profile, a.region)
    ids = a.instances.split(',')
    if len(ids) != 3 or len(set(ids)) != 3 or any(not re.fullmatch(r'i-[0-9a-f]+', n) for n in ids):
        raise ValueError('--instances requires three distinct instance IDs in node order')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]+', a.bucket):
        raise ValueError('invalid result bucket')
    if a.repeat < 1 or not 0 < a.duration <= 60:
        raise ValueError('repeat >= 1 and duration in (0, 60] required')
    # Identity and instance/agent checks happen before any external writes.
    identity = aws.call('sts', 'get-caller-identity')
    described = aws.call('ec2', 'describe-instances', '--instance-ids', *ids)
    nodes = {n['InstanceId']: n for r in described['Reservations'] for n in r['Instances']}
    for instance in ids:
        n = nodes[instance]
        if n['State']['Name'] != 'running' or n['InstanceType'] != 'm6gd.medium':
            raise RuntimeError('requires existing running m6gd.medium experiment nodes')
        tags = {t['Key']: t['Value'] for t in n.get('Tags', [])}
        if not any('openkache' in v.lower() for v in tags.values()):
            raise RuntimeError('instance lacks OpenKache experiment tag')
    if len({nodes[n]['VpcId'] for n in ids}) != 1:
        raise RuntimeError('nodes must share one experiment VPC')
    hosts = [nodes[n]['PrivateIpAddress'] for n in ids]
    if not all(ipaddress.ip_address(h).is_private for h in hosts):
        raise RuntimeError('private IPs required')
    agents = aws.call('ssm', 'describe-instance-information', '--filters',
                      json.dumps([{'Key': 'InstanceIds', 'Values': ids}]))
    online = {n['InstanceId'] for n in agents['InstanceInformationList'] if n['PingStatus'] == 'Online'}
    if set(ids) - online:
        raise RuntimeError('SSM agent unavailable')
    a.out.mkdir(parents=True, exist_ok=False)
    campaign = 'a1-a3-' + time.strftime('%Y%m%d-%H%M%S', time.gmtime()) + '-' + uuid.uuid4().hex[:8]
    remote = '/opt/openkache-a1-a3/' + campaign
    prefix = campaign
    dist_hash = hashlib.sha256(a.dist.read_bytes()).hexdigest()
    manifest = dict(schema='a1-a3-fenced-campaign-1', campaign=campaign, instances=ids, hosts=hosts,
                    account=identity['Account'], region=a.region, distSha256=dist_hash,
                    scriptsSha256={n: hashlib.sha256((HERE / n).read_bytes()).hexdigest() for n in FILES},
                    detectors=a.detectors, scenarios=a.scenarios, repeat=a.repeat, duration=a.duration,
                    ssdStallCovered=False, clockAssumption='continuous monotonic clocks; lease drift bound from fencing-local.md',
                    resultPrefix=f's3://{a.bucket}/{prefix}/')
    (a.out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    parallel = lambda f: list(concurrent.futures.ThreadPoolExecutor(max_workers=3).map(f, range(3)))
    preflight = '''
command -v java
java -version
command -v python3
command -v aws
command -v tc
command -v ip
python3 - <<'PY'
import socket
for kind,port in [(socket.SOCK_STREAM,7000),(socket.SOCK_STREAM,7001),(socket.SOCK_DGRAM,7002)]:
    with socket.socket(type=kind) as s: s.bind(('0.0.0.0',port))
PY
'''
    environment = parallel(lambda i: aws.shell(ids[i], preflight))
    (a.out / 'environment.json').write_text(json.dumps(environment, indent=2))
    # Upload only experiment binaries and scripts, never AWS credentials.
    aws.call('s3', 'cp', aws.local_path(a.dist), f's3://{a.bucket}/{prefix}/dist.tar')
    for filename in FILES:
        aws.call('s3', 'cp', aws.local_path(HERE / filename), f's3://{a.bucket}/{prefix}/{filename}')
    def deploy(i):
        script = f'mkdir -p {remote}/dist\n'
        script += f'aws s3 cp s3://{a.bucket}/{prefix}/dist.tar {remote}/dist.tar --region {a.region}\n'
        script += f'echo {dist_hash}  {remote}/dist.tar | sha256sum -c -\n'
        script += f'tar -xf {remote}/dist.tar -C {remote}/dist --strip-components=1\n'
        for name in FILES:
            script += f'aws s3 cp s3://{a.bucket}/{prefix}/{name} {remote}/{name} --region {a.region}\n'
        return aws.shell(ids[i], script)
    parallel(deploy)
    completed = []
    try:
        for detector in a.detectors:
            for scenario in a.scenarios:
                for repeat in range(1, a.repeat + 1):
                    case = f'{detector.lower()}-{scenario}-r{repeat}'
                    work = remote + '/' + case
                    def configure(i):
                        content = base64.b64encode(node_properties(hosts, i, detector).encode()).decode()
                        return aws.shell(ids[i], f'mkdir {work}\nln -s {remote}/dist {work}/dist\n'
                                         f'cp {remote}/*.py {work}/\nprintf %s {content} | base64 -d > {work}/node.properties\n')
                    parallel(configure)
                    try:
                        for i in (1, 2):
                            aws.shell(ids[i], f'python3 {work}/pb_control.py start')
                        benchmark = (f'python3 {work}/fenced_bench.py --hosts {",".join(hosts)}'
                                     f' --out {work}/result.json --detector {detector} --scenario {scenario}'
                                     f' --duration {a.duration} --repeat {repeat} --startup-grace 5')
                        # Evidence is uploaded even when the benchmark fails.
                        script = f'''set +e
{benchmark}
bench_rc=$?
aws s3 cp {work}/result.json s3://{a.bucket}/{prefix}/{case}/result.json --region {a.region}
upload_rc=$?
exit $((bench_rc || upload_rc))
'''
                        print(case, flush=True)
                        output = aws.shell(ids[0], script, timeout=int(a.duration + 120))
                        print(output.strip(), flush=True)
                        aws.call('s3', 'cp', f's3://{a.bucket}/{prefix}/{case}/result.json', aws.local_path(a.out / f'{case}.json'))
                        result = json.loads((a.out / f'{case}.json').read_text())
                        if not result.get('ok'):
                            raise RuntimeError(f'{case}: failed result')
                        completed.append(case)
                    finally:
                        # Only our exact PID and fresh case directory are touched.
                        def cleanup(i):
                            return aws.shell(ids[i], f'python3 {work}/pb_control.py stop\n'
                                             f'aws s3 cp {work}/server.log s3://{a.bucket}/{prefix}/{case}/node-{i}.log --region {a.region}\n')
                        parallel(cleanup)
    finally:
        (a.out / 'completed.json').write_text(json.dumps(completed, indent=2) + '\n')
    print('RESULTS', a.out, flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--instances', required=True, help='node0,node1,node2 EC2 IDs')
    p.add_argument('--bucket', required=True)
    p.add_argument('--dist', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--aws-bin', default='aws')
    p.add_argument('--profile')
    p.add_argument('--region', default='ap-northeast-2')
    p.add_argument('--detectors', nargs='+', choices=['SWIM', 'SWIM_LIFEGUARD'], default=['SWIM', 'SWIM_LIFEGUARD'])
    p.add_argument('--scenarios', nargs='+', choices=['healthy', 'crash', 'loss', 'delay', 'cpu-pause', 'partition'],
                   default=['healthy', 'crash', 'loss', 'delay', 'cpu-pause', 'partition'])
    p.add_argument('--repeat', type=int, default=3)
    p.add_argument('--duration', type=float, default=8)
    execute(p.parse_args())
