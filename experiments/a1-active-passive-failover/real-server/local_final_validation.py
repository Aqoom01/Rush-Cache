"""Run the exact final bench against three local JVMs. No SSH/AWS and no shared PID files."""
import argparse
import json
from pathlib import Path
import subprocess

import final_bench
from run_final_validation import matrix, configuration
from analyze_final_validation import validate_result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--java', default='java')
    p.add_argument('--lib', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--group', choices=['all', 'crash', 'write', 'read'], default='all')
    p.add_argument('--full-count', action='store_true', help='use real bounded write budgets, not quick 16')
    p.add_argument('--seconds', type=float)
    p.add_argument('--case-limit', type=int)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    hosts = [{'private': f'127.0.0.{i+1}'} for i in range(3)]
    cases = matrix(True, args.group)
    if args.full_count:
        cases = [dict(c, value=value, count=0, clients=8 if c['clients'] == 4 else 1) for value in (64, 8192) for c in cases]
    if args.seconds is not None:
        cases = [dict(c, seconds=args.seconds, warmup_seconds=5) for c in cases]
    if args.case_limit:
        cases = cases[:args.case_limit]
    report = []
    for index, case in enumerate(cases):
        folder = args.out / f'local-{index:03d}'
        folder.mkdir()
        processes = []
        logs = []
        try:
            for node in range(3):
                config = folder / f'node-{node}.properties'
                config.write_text(configuration(case, node, hosts), encoding='utf-8')
                log = (folder / f'node-{node}.log').open('wb')
                logs.append(log)
                processes.append(subprocess.Popen([args.java, '-Xms128m', '-Xmx512m', '-cp', str(args.lib.resolve() / '*'),
                    'io.openkache.server.replication.PbServerMain', str(config.resolve())], stdout=log, stderr=subprocess.STDOUT))
            flags = ['--hosts', ','.join(h['private'] for h in hosts), '--out', str(folder / 'result.json')]
            for key, value in case.items():
                if key != 'caseId':
                    flags.extend(['--' + key.replace('_', '-'), str(value)])
            bench_args = final_bench.parser().parse_args(flags)
            result = final_bench.run(bench_args, lambda node: processes[node].kill())
            validate_result(case, result)
            report.append({'case': case, 'ok': True})
            print(f'PASS {index+1}/{len(cases)} {case}', flush=True)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            for log in logs:
                log.close()
            (args.out / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
