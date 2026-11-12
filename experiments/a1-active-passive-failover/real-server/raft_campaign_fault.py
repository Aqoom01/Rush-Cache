"""Apply and restore a bounded fault to one exact experiment JVM, emitting ground truth."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import threading
import sys


def execute(root,spec,hosts,node,ports=(7000,7001,7002),local_model=False):
    root=Path(root).resolve()
    pid=int((root/'pb.pid').read_text())
    cmd=Path(f'/proc/{pid}/cmdline').read_bytes()
    if b'io.openkache.server.replication.PbServerMain' not in cmd or str(root/'node.properties').encode() not in cmd:
        raise RuntimeError('target PID/config identity mismatch')
    events=[]; paused=False; iface=None; qdisc=False; rules=[]; fio=None
    cpu_workers=[]; affinity={};cpu_logs=[]
    io_stop=threading.Event();io_thread=None;io_errors=[];io_stats={'writes':0}
    def event(kind,**extra):
        row=dict(event=kind,nanoTime=time.monotonic_ns(),wallEpochNs=time.time_ns(),node=node,**extra)
        events.append(row)
        (root/'fault-events.json').write_text(json.dumps(events,indent=2)+'\n')
    def command(*args): return subprocess.check_output(args,text=True,timeout=15)
    def interrupt(*_): raise InterruptedError('fault cancelled; restoring')
    signal.signal(signal.SIGTERM,interrupt);signal.signal(signal.SIGINT,interrupt)
    duration=float(spec['seconds'])
    if not 0<=duration<=30: raise ValueError('fault duration out of bounds')
    peers=[h for i,h in enumerate(hosts) if i!=node]
    try:
        kind=spec['kind'];event('requested',fault=kind)
        if kind=='crash':
            os.kill(pid,signal.SIGKILL)
        elif kind=='cpu-pause':
            os.kill(pid,signal.SIGSTOP);paused=True
        elif kind=='cpu-contention':
            workers=spec.get('workers',8)
            if not isinstance(workers,int) or not 1<=workers<=16 or duration<=0:
                raise ValueError('CPU contention requires 1..16 workers and positive duration')
            # Restrict only the verified experiment JVM's threads. No host scheduler/cgroup changes.
            # Workers share one allowed CPU with it, and terminate by their own bounded deadline.
            cpu=min(os.sched_getaffinity(pid))
            for task in Path(f'/proc/{pid}/task').iterdir():
                tid=int(task.name)
                try:
                    affinity[tid]=sorted(os.sched_getaffinity(tid));os.sched_setaffinity(tid,{cpu})
                except ProcessLookupError: pass
            code=('import os,time,json,sys\n'
                  'os.sched_setaffinity(0,{int(sys.argv[1])})\n'
                  'print("READY",flush=True)\n'
                  'start=time.process_time(); end=time.monotonic()+float(sys.argv[2]); x=1\n'
                  'while time.monotonic()<end:\n'
                  ' for _ in range(10000): x=(x*1664525+1013904223)&0xffffffff\n'
                  'print(json.dumps({"cpuSeconds":time.process_time()-start}),flush=True)\n')
            for i in range(workers):
                log=(root/f'cpu-worker-{i}.log').open('w+');cpu_logs.append(log)
                cpu_workers.append(subprocess.Popen([sys.executable,'-u','-c',code,str(cpu),str(duration+2)],
                                                     stdout=log,stderr=subprocess.STDOUT,start_new_session=True))
            deadline=time.monotonic()+5
            while True:
                ready=0
                for log in cpu_logs:
                    log.seek(0);ready+=log.read().startswith('READY')
                if ready==workers: break
                if any(p.poll() is not None for p in cpu_workers) or time.monotonic()>deadline:
                    raise RuntimeError('CPU workers did not become ready')
                time.sleep(.02)
            event('cpu-contention-configured',cpu=cpu,workers=workers,threadAffinities=affinity,
                  workerPids=[p.pid for p in cpu_workers],
                  model='same-CPU busy processes; shared-host interference remains in local mode' if local_model else
                        'same-CPU busy processes on dedicated experiment instance')
        elif kind=='io-stall':
            # Inject an I/O completion stall into the KV worker that owns the real NVMe store.
            # This is a deterministic software fault, not a claim of physically stalled hardware.
            marker=root/'io-fault.properties'
            temp=root/'io-fault.tmp'
            temp.write_text(f'kind=io-stall\nuntil.epoch.ms={int((time.time()+duration)*1000)}\n')
            temp.replace(marker)
        elif kind=='fio':
            runtime=json.loads((root/'runtime.json').read_text())
            directory=Path(runtime['OPENKACHE_PB_STORAGE_DIR']).parent
            if local_model:
                def write_load():
                    try:
                        with (directory/'local-contention.dat').open('xb',buffering=0) as file:
                            block=b'x'*4096;offset=0
                            while not io_stop.is_set():
                                file.seek(offset);file.write(block);os.fsync(file.fileno())
                                offset=(offset+4096)%(16*1024*1024);io_stats['writes']+=1
                    except Exception as error: io_errors.append(repr(error))
                io_thread=threading.Thread(target=write_load);io_thread.start()
                event('storage-device-evidence',model='local-filesystem-fsync-contention')
            else:
                evidence=json.loads(command('lsblk','-J','-o','NAME,MODEL,MOUNTPOINTS'))
                event('storage-device-evidence',lsblk=evidence,
                      mount=command('findmnt','-J','-T',str(directory)))
                output=root/'fio.json'
                fio=subprocess.Popen(['fio','--name=raft-fd-contention','--filename='+str(directory/'fio.dat'),
                                      '--size=256m','--ioengine=libaio','--rw=randrw','--rwmixread=50',
                                      '--bs=4k','--iodepth=32','--direct=1','--time_based=1',
                                      '--runtime='+str(int(duration)),'--group_reporting=1','--eta=never',
                                      '--output-format=json','--output='+str(output)])
        elif local_model and kind in ('loss','delay','partition'):
            marker=root/'io-fault.properties';temp=root/'network-fault.tmp'
            temp.write_text(f'kind={kind}\nscope={spec.get("scope","udp")}\nuntil.epoch.ms={int((time.time()+duration)*1000)}\n')
            temp.replace(marker)
            event('local-network-model',model='application-message-drop-delay; not kernel netem')
        elif kind=='partition':
            # Match only the dedicated replication/detection ports and exact peer IPs.
            for peer in peers:
                for direction,address in [('OUTPUT','-d'),('INPUT','-s')]:
                    for protocol,port in [('tcp',ports[1]),('udp',ports[2])]:
                        rule=[direction,address,peer,'-p',protocol,'--dport',str(port),
                              '-m','comment','--comment',f'raft-fd-{pid}','-j','DROP']
                        command('iptables','-w','-I',*rule);rules.append(rule)
        elif kind in ('loss','delay'):
            iface=json.loads(command('ip','-j','route','get',peers[0]))[0]['dev']
            old=json.loads(command('tc','-j','qdisc','show','dev',iface))
            if any(q.get('root') and (q.get('kind') not in ('mq','fq_codel','noqueue') or q.get('handle')!='0:') for q in old):
                raise RuntimeError('root qdisc already configured; refusing to overwrite')
            command('tc','qdisc','replace','dev',iface,'root','handle','71:','prio','bands','3','priomap',*(['0']*16));qdisc=True
            impairment=['loss','30%'] if kind=='loss' else ['delay','250ms','50ms']
            command('tc','qdisc','add','dev',iface,'parent','71:3','handle','73:','netem',*impairment)
            protocols=[(17,ports[2])]+([(6,ports[1])] if spec.get('scope')=='both' else [])
            priority=1
            for peer in peers:
                for protocol,port in protocols:
                    command('tc','filter','add','dev',iface,'protocol','ip','parent','71:','prio',str(priority),'u32',
                            'match','ip','dst',peer+'/32','match','ip','protocol',str(protocol),'0xff',
                            'match','ip','dport',str(port),'0xffff','flowid','71:3');priority+=1
            event('netem-configured',evidence=command('tc','-s','qdisc','show','dev',iface))
        elif kind!='healthy':
            raise ValueError('unsupported fault')
        event('applied',pid=pid,mode='software-io-completion-stall' if kind=='io-stall' else kind)
        if duration: time.sleep(duration)
        if cpu_workers:
            # /proc stat utime/stime prove actual CPU consumption, rather than assuming stress ran.
            ticks=0
            for process in cpu_workers:
                raw=Path(f'/proc/{process.pid}/stat').read_text().rsplit(')',1)[1].split()
                ticks+=int(raw[11])+int(raw[12])
            if ticks<=0: raise RuntimeError('CPU contention workers consumed no CPU')
            event('cpu-contention-verified',workerCpuTicks=ticks,clockTicksPerSecond=os.sysconf('SC_CLK_TCK'))
        if io_thread is not None:
            io_stop.set();io_thread.join(timeout=10)
            if io_thread.is_alive() or io_errors or io_stats['writes']==0: raise RuntimeError('local I/O contention failed: '+repr(io_errors))
            event('local-io-verified',**io_stats)
        if fio is not None:
            code=fio.wait(timeout=15)
            data=json.loads((root/'fio.json').read_text())
            if code or any(j.get('error',0) for j in data['jobs']): raise RuntimeError('fio failed')
            ios=sum(j['read']['total_ios']+j['write']['total_ios'] for j in data['jobs'])
            if ios<1: raise RuntimeError('fio performed no I/O')
            event('fio-verified',totalIos=ios,evidence=data)
    except Exception as error:
        event('error',error=repr(error));raise
    finally:
        for process in cpu_workers:
            if process.poll() is None: process.terminate()
        for process in cpu_workers:
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill();process.wait(timeout=5)
        for log in cpu_logs: log.close()
        # Threads created during contention inherit the restricted mask; restore them as well.
        if affinity:
            try: tasks=list(Path(f'/proc/{pid}/task').iterdir())
            except FileNotFoundError: tasks=[]
            for task in tasks:
                tid=int(task.name)
                try: os.sched_setaffinity(tid,set(affinity.get(tid,affinity.get(pid,next(iter(affinity.values()))))))
                except ProcessLookupError: pass
            event('cpu-affinity-restored',threads=len(tasks))
        if paused:
            try: os.kill(pid,signal.SIGCONT)
            except ProcessLookupError: pass
        if fio is not None and fio.poll() is None: fio.terminate();fio.wait(timeout=10)
        io_stop.set()
        if io_thread is not None: io_thread.join(timeout=10)
        (root/'io-fault.properties').unlink(missing_ok=True)
        for rule in reversed(rules): command('iptables','-w','-D',*rule)
        if qdisc:
            state=json.loads(command('tc','-j','qdisc','show','dev',iface))
            if any(q.get('root') and q.get('handle')=='71:' for q in state):
                event('netem-final',evidence=command('tc','-s','qdisc','show','dev',iface))
                command('tc','qdisc','del','dev',iface,'root','handle','71:')
        event('released')
    return events


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True)
    p.add_argument('--spec',required=True);p.add_argument('--hosts',required=True);p.add_argument('--node',type=int,required=True)
    p.add_argument('--ports',default='7000,7001,7002')
    p.add_argument('--local-model',action='store_true')
    a=p.parse_args();execute(a.root,json.loads(a.spec),a.hosts.split(','),a.node,tuple(map(int,a.ports.split(','))),a.local_model)
