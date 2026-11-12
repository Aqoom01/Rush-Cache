"""Prepare/execute a reproducible final cohort. Default: plan only, NO SSH or AWS action.

--execute requires explicit hosts, SSH key and pinned known_hosts. Does not create
or stop EC2. Only starts/stops PbServerMain in /home/ubuntu/a1-final-validation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random
import shlex
import shutil
import subprocess
import time
import uuid

HERE = Path(__file__).resolve().parent
REMOTE = '/home/ubuntu/a1-final-validation'
SCRIPTS = ('pb_control.py', 'pb_bench.py', 'election_bench.py', 'chain_bench.py',
           'craq_bench.py', 'final_bench.py', 'run_final_validation.py', 'analyze_final_validation.py')


def matrix(quick=False, group='all'):
    cases = []
    variants = [('PB', 'INCREMENTAL'), ('VR', 'INCREMENTAL'),
                ('VR', 'INCREMENTAL_CATCHUP'), ('RAFT', 'INCREMENTAL')]
    repeats = range(1, 2 if quick else 6)
    for repeat in repeats:
        block = []
        for value in ([64] if quick else [64, 8192]):
            for protocol, mode in variants:
                block.append(dict(scenario='crash', protocol=protocol, vr_mode=mode,
                                  value=value, clients=1, repeat=repeat))
                for clients in ([1, 4] if quick else [1, 4, 8]):
                    block.append(dict(scenario='write', protocol=protocol, vr_mode=mode,
                                      value=value, clients=clients, repeat=repeat))
            for mode, target in [('TAIL_ONLY', 'tail'), ('TAIL_ONLY', 'spread'), ('CRAQ', 'spread')]:
                for clients in ([1, 4] if quick else [1, 4, 8]):
                    block.append(dict(scenario='read', protocol='CHAIN', read_mode=mode,
                                      read_target=target, value=value, clients=clients, repeat=repeat))
        random.Random(20261109 + repeat).shuffle(block)
        cases.extend(c for c in block if group == 'all' or c['scenario'] == group)
    for i, case in enumerate(cases):
        case.update(caseId=f'case-{i:04d}', timeout=500,
                    seconds=1 if quick else 30, warmup_seconds=.1 if quick else 5,
                    count=16 if quick and case['scenario'] == 'write' else 0)
    return cases


def configuration(case, node, hosts):
    fields = {'protocol': case['protocol'], 'vr.mode': case.get('vr_mode', 'INCREMENTAL'),
              'chain.read.mode': case.get('read_mode', 'TAIL_ONLY'), 'retry.ms': 200,
              'node.id': node, 'client.port': 7000, 'peer.port': 7001, 'admin.port': 0,
              'heartbeat.ms': 50, 'timeout.ms': case['timeout'], 'tick.ms': 10,
              'election.seed': case['repeat']}
    fields.update({f'peer.{i}': hosts[i]['private'] for i in range(3)})
    return ''.join(f'{k}={v}\n' for k, v in fields.items())


def main():
    global REMOTE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--dist', type=Path, required=True)
    parser.add_argument('--hosts', type=Path)
    parser.add_argument('--key', type=Path)
    parser.add_argument('--known-hosts', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--quick', action='store_true')
    parser.add_argument('--group', choices=['all', 'crash', 'write', 'read'], default='all')
    args = parser.parse_args()
    REMOTE = REMOTE + '/' + uuid.uuid4().hex[:12]
    cases = matrix(args.quick, args.group)
    if not args.dist.is_file():
        parser.error('build :cache-server:distTar first; --dist must exist')
    if args.execute and not all(p and p.is_file() for p in (args.hosts, args.key, args.known_hosts)):
        parser.error('--execute requires --hosts, --key and --known-hosts files')
    args.out.mkdir(parents=True, exist_ok=False)
    scripts = args.out / 'runtime-scripts'
    scripts.mkdir()
    for name in SCRIPTS:
        shutil.copy2(HERE / name, scripts / name)
    shutil.copy2(args.dist, args.out / 'executed-dist.tar')
    manifest = {'schema': 'a1-final-plan-1', 'createdEpochMs': int(time.time() * 1000),
                'executed': False, 'quick': args.quick, 'group': args.group, 'cases': cases, 'remoteDirectory': REMOTE,
                'distSha256': hashlib.sha256(args.dist.read_bytes()).hexdigest(),
                'scriptsSha256': {n: hashlib.sha256((scripts / n).read_bytes()).hexdigest() for n in SCRIPTS},
                'scope': 'write windows restart each case, read-only scaling timed; no EC2 lifecycle actions'}
    manifest_path = args.out / 'manifest.json'
    def save():
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    save()
    if not args.execute:
        print(f'PREPARED {len(cases)} cases; no remote connections: {args.out}')
        return
    hosts = json.loads(args.hosts.read_text(encoding='utf-8-sig'))
    if len(hosts) != 3 or len({h['private'] for h in hosts}) != 3 or len({h['public'] for h in hosts}) != 3:
        raise ValueError('exactly three distinct dedicated hosts required')
    # Host data is kept only in this private run directory.
    (args.out / 'hosts.json').write_text(json.dumps(hosts, indent=2), encoding='utf-8')
    opts = ['-i', str(args.key.resolve()), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
            '-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={args.known_hosts.resolve()}']
    def command(argv):
        p = subprocess.run(argv, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600)
        if p.returncode:
            raise RuntimeError(f'exit={p.returncode}: {p.stdout[-3000:]} {p.stderr[-3000:]}')
        return p.stdout
    def ssh(i, command_text):
        return command(['ssh', *opts, f"ubuntu@{hosts[i]['public']}", command_text])
    def put(i, source, target):
        command(['scp', *opts, str(source), f"ubuntu@{hosts[i]['public']}:{target}"])
    def get(i, source, target):
        command(['scp', *opts, f"ubuntu@{hosts[i]['public']}:{source}", str(target)])
    def parallel(fn):
        with ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(fn, range(3)))
    def stop(i):
        code = (
            f"import sys,time; sys.path.insert(0,{REMOTE!r}); import pb_control as c\n"
            "pid=int(c.PID.read_text()) if c.PID.exists() else None\n"
            "c.stop()\n"
            "end=time.monotonic()+5\n"
            "while pid is not None and c.alive(pid) and time.monotonic()<end: time.sleep(.05)\n"
            "assert not c.PID.exists() and (pid is None or not c.alive(pid)), 'scoped JVM still alive'\n"
        )
        return ssh(i, 'python3 -c ' + shlex.quote(code))
    cleanup = []
    configured = set()
    try:
        def deploy(i):
            check = "import socket; sockets=[]; " + '; '.join(
                f"s=socket.socket(); s.bind(({hosts[i]['private']!r},{port})); sockets.append(s)" for port in (7000, 7001))
            ssh(i, 'python3 -c ' + shlex.quote(check))
            ssh(i, f'mkdir -p {REMOTE}/dist {REMOTE}/results')
            # Do not replace the old experiment directory or touch unrelated JVMs.
            put(i, scripts / 'pb_control.py', f'{REMOTE}/pb_control.py')
            configured.add(i)
            stop(i)
            put(i, args.out / 'executed-dist.tar', f'{REMOTE}/dist.tar')
            for name in SCRIPTS:
                put(i, scripts / name, f'{REMOTE}/{name}')
            # Fresh per-run directory avoids stale jars without recursive deletion.
            ssh(i, f'tar -xf {REMOTE}/dist.tar -C {REMOTE}/dist --strip-components=1')
            remote_sha = ssh(i, f'sha256sum {REMOTE}/dist.tar').split()[0]
            if remote_sha != manifest['distSha256']:
                raise RuntimeError('remote deployment hash mismatch')
            return ssh(i, 'uname -a; lscpu; java -version 2>&1; python3 --version; df -h')
        manifest['environment'] = parallel(deploy)
        manifest['executed'] = True
        save()
        for case in cases:
            parallel(stop)
            name = case['caseId']
            def start(i):
                path = args.out / f'{name}-node-{i}.properties'
                path.write_text(configuration(case, i, hosts), encoding='utf-8')
                put(i, path, f'{REMOTE}/node.properties')
                ssh(i, f': > {REMOTE}/server.log')
                ssh(i, f'python3 {REMOTE}/pb_control.py start')
            parallel(start)
            controller = 2
            if case['scenario'] == 'crash':
                found = json.loads(ssh(2, f"python3 {REMOTE}/election_bench.py --discover --protocol {case['protocol']} --hosts {','.join(h['private'] for h in hosts)}"))
                controller = found['leader']
            remote_result = f'{REMOTE}/results/{name}.json'
            flags = ['--hosts', ','.join(h['private'] for h in hosts), '--out', remote_result, '--controller', str(controller)]
            for key, value in case.items():
                if key != 'caseId':
                    flags.extend(['--' + key.replace('_', '-'), str(value)])
            try:
                print(ssh(controller, f'python3 {REMOTE}/final_bench.py ' + ' '.join(shlex.quote(x) for x in flags)).strip(), flush=True)
            except Exception as error:
                (args.out / f'{name}-controller-error.txt').write_text(str(error), encoding='utf-8')
                raise
            finally:
                # Recover failed-run evidence too; a failed run never becomes a success row.
                get(controller, remote_result, args.out / f'{name}.json')
                for i in range(3):
                    get(i, f'{REMOTE}/server.log', args.out / f'{name}-node-{i}.log')
            case['resultSha256'] = hashlib.sha256((args.out / f'{name}.json').read_bytes()).hexdigest()
            result = json.loads((args.out / f'{name}.json').read_text())
            if not result['ok']:
                raise RuntimeError(f'{name} failed')
            save()
    finally:
        for i in sorted(configured):
            try:
                stop(i)
                absent = ssh(i, f'test ! -f {REMOTE}/pb.pid && echo CLEAN').strip() == 'CLEAN'
                cleanup.append({'node': i, 'ok': absent})
            except Exception as error:
                cleanup.append({'node': i, 'ok': False, 'error': str(error)})
        manifest['cleanup'] = cleanup
        manifest['complete'] = len(cleanup) == 3 and all(r['ok'] for r in cleanup) and all('resultSha256' in c for c in cases)
        save()
    if not manifest['complete']:
        raise RuntimeError('cohort incomplete or cleanup failed; inspect manifest')
    print(f'COMPLETE {args.out}; EC2 still running', flush=True)


if __name__ == '__main__':
    main()
