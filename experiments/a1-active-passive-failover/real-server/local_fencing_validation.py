"""Run the fenced PB and the SWIM UDP detector in three local Java processes."""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import socket
import struct
import subprocess
import time

from pb_bench import Client


HOSTS = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
SERVER_MAIN = 'io.openkache.server.replication.PbServerMain'


def free_port(peer):
    for _ in range(20):
        try:
            with ExitStack() as stack:
                first = stack.enter_context(socket.socket())
                first.bind((HOSTS[0], 0))
                port = first.getsockname()[1]
                for host in HOSTS[1:]:
                    stack.enter_context(socket.socket()).bind((host, port))
                if peer:
                    for host in HOSTS:
                        stack.enter_context(socket.socket(type=socket.SOCK_DGRAM)).bind((host, port))
                return port
        except OSError:
            continue
    raise RuntimeError('no common free loopback port')


def until(operation, description, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if result := operation():
                return result
        except (OSError, EOFError):
            pass
        time.sleep(.025)
    raise AssertionError(description)


def get(client, node, operation, key, epoch):
    return client.request(node, 1, struct.pack('!QQQII', 17, operation, epoch, 1000, len(key)) + key)


def validate(java, lib, output):
    output.mkdir(parents=True, exist_ok=False)
    client_port, peer_port = free_port(False), free_port(True)
    while client_port == peer_port:
        peer_port = free_port(True)
    client = Client(HOSTS, client_port=client_port, peer_port=peer_port)
    processes = []
    commands = []
    result = {'ok': False}
    try:
        with ExitStack() as stack:
            for node in range(3):
                props = output / f'node-{node}.properties'
                props.write_text('\n'.join([
                    'protocol=PB_FENCED', f'node.id={node}', f'client.port={client_port}', f'peer.port={peer_port}',
                    'admin.port=0', 'heartbeat.ms=50', 'timeout.ms=500', 'tick.ms=10',
                    *[f'peer.{i}={host}' for i, host in enumerate(HOSTS)],
                ]) + '\n', encoding='utf-8')
                log = stack.enter_context((output / f'node-{node}.log').open('wb'))
                args = [java, '-cp', str(lib.resolve() / '*'), SERVER_MAIN, str(props.resolve())]
                commands.append(args)
                processes.append(subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT))

            until(lambda: client.status(0)['fenceState'] == 'SERVING', 'original primary never acquired a lease')
            keys = [(f'before-{i}'.encode(), f'value-{i}'.encode()) for i in range(16)]
            for i, (key, value) in enumerate(keys, 1):
                reply = client.set(0, i, key, value, epoch=1)
                assert reply['status'] == 0 and reply['epoch'] == 1, reply

            killed_at = time.monotonic_ns()
            processes[0].kill()  # SIGKILL on POSIX, TerminateProcess on Windows
            processes[0].wait(timeout=5)

            def first_write():
                reply = client.set(1, 100, b'after', b'new', epoch=2)
                return reply if reply['status'] == 0 else None

            assert until(first_write, 'no successful write after SWIM detected primary death')['epoch'] == 2
            result['firstSuccessAfterKillMs'] = (time.monotonic_ns() - killed_at) / 1_000_000
            for i, (key, value) in enumerate(keys, 200):
                reply = get(client, 1, i, key, 2)
                assert reply['status'] == 0 and reply['value'] == value, reply
            assert get(client, 1, 300, b'after', 1)['status'] == 5
            redirect = get(client, 2, 301, b'after', 2)
            assert redirect['status'] == 4 and redirect['detail'] == 'redirect-node=1', redirect
            status = client.status(1)
            assert status['promotionVotes'] == 2 and status['epoch'] == 2, status
            assert -1 <= status['suspectNs'] <= status['detectedNs'], status
            assert 0 <= status['detectedNs'] <= status['quorumNs'] <= status['promotionAuthorizedNs'] <= status['readyNs'], status
            assert 0 <= status['peerReadyNs'] <= status['readyNs'], status
            assert client.status(2)['epoch'] == 2

            restart = subprocess.run(commands[0], capture_output=True, text=True, timeout=10)
            assert restart.returncode != 0 and 'restart requires state recovery' in restart.stderr, restart.stderr
            result.update(ok=True, preserved=len(keys), node1=status, restartRejected=True)
    finally:
        client.close()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({key: value for key, value in result.items() if key != 'node1'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--lib', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    validate(args.java, args.lib, args.out)
