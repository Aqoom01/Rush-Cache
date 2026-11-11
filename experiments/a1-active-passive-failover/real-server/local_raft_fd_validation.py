"""Run three real JVMs per Raft detector mode and audit leader crash/re-election."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys

from election_bench import discover
from local_fencing_validation import free_port, HOSTS
from pb_bench import Client

HERE=Path(__file__).resolve().parent


def properties(hosts,node,detector,ports,seed=1):
    return '\n'.join([
        'protocol=RAFT', f'node.id={node}', f'election.seed={seed}',
        *[f'peer.{i}={host}' for i,host in enumerate(hosts)],
        f'client.port={ports[0]}', f'peer.port={ports[1]}', f'failure.port={ports[2]}',
        'admin.port=0', 'heartbeat.ms=50', 'timeout.ms=500', 'tick.ms=10',
        f'failure.detector={detector}', 'failure.startup.grace.ms=1000',
        'failure.probe.interval.ms=200', 'failure.direct.timeout.ms=50',
        'failure.indirect.timeout.ms=50', 'failure.suspicion.ms=500',
        'failure.lifeguard.awareness.max.multiplier=8',
        'failure.lifeguard.suspicion.max.multiplier=2',
        'failure.lifeguard.max.confirmations=1', ''])


def run(a):
    a.out.mkdir(parents=True,exist_ok=False)
    summaries=[]
    for detector in a.detectors:
        for repeat in range(1,a.repeat+1):
            root=a.out/f'{detector.lower()}-r{repeat}'
            root.mkdir()
            ports=[free_port(False),free_port(False),free_port(True)]
            while len(set(ports))!=3:
                ports=[free_port(False),free_port(False),free_port(True)]
            processes=[]
            with ExitStack() as stack:
                try:
                    for node in range(3):
                        work=root/f'node-{node}'
                        work.mkdir()
                        (work/'dist').symlink_to(a.lib.resolve().parent,target_is_directory=True)
                        for filename in ('pb_control.py','pb_bench.py','election_bench.py'):
                            shutil.copy2(HERE/filename,work/filename)
                        (work/'node.properties').write_text(properties(HOSTS,node,detector,ports,repeat))
                        log=stack.enter_context((work/'server.log').open('ab'))
                        # Same command line as pb_control; /proc identity checks still apply.
                        p=subprocess.Popen([a.java,'-Xms64m','-Xmx256m','-cp',str(a.lib.resolve()/'*'),
                                            'io.openkache.server.replication.PbServerMain',str((work/'node.properties').resolve())],
                                           stdout=log,stderr=subprocess.STDOUT)
                        processes.append(p)
                        (work/'pb.pid').write_text(str(p.pid))
                    client=Client(HOSTS,client_port=ports[0],peer_port=ports[1])
                    try:
                        leader,_,_=discover(client,'RAFT')
                    finally:
                        client.close()
                    work=root/f'node-{leader}'
                    result=(root/'result.json').resolve()
                    subprocess.run([sys.executable,str(work/'election_bench.py'),'--protocol','RAFT',
                                    '--hosts',','.join(HOSTS),'--controller',str(leader),'--out',str(result),
                                    '--client-port',str(ports[0]),'--peer-port',str(ports[1]),
                                    '--count','32','--repeat',str(repeat)],check=True,timeout=60)
                    data=json.loads(result.read_text())
                    if detector!='FIXED_TIMEOUT' and data['electionOrigin']!='SWIM_DEAD':
                        raise AssertionError('FD smoke used fallback rather than DEAD; inspect result')
                    summaries.append({k:data.get(k) for k in ('failureDetector','originalLeader','newLeader',
                                      'electionOrigin','failoverNs','promotionNs','preservedSuccesses','ok')})
                finally:
                    for p in processes:
                        if p.poll() is None:
                            p.terminate()
                        p.wait(timeout=10)
    (a.out/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
    print(json.dumps(summaries,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--lib',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--java',default='java')
    p.add_argument('--repeat',type=int,default=1)
    p.add_argument('--detectors',nargs='+',choices=['FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'],
                   default=['FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'])
    run(p.parse_args())
