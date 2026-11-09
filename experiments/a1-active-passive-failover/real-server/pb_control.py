"""전용 실험 디렉터리의 PB JVM 하나만 관리한다. 다른 프로세스/EC2는 중단하지 않는다."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parent
PID=ROOT/"pb.pid"

def alive(pid):
    try:
        command=Path(f"/proc/{pid}/cmdline").read_bytes()
        return b"io.openkache.server.replication.PbServerMain" in command and str(ROOT/"node.properties").encode() in command
    except FileNotFoundError:
        return False

def start():
    if PID.exists() and alive(int(PID.read_text())):
        raise RuntimeError("PB already running")
    log=(ROOT/"server.log").open("ab")
    p=subprocess.Popen(["java","-Xms128m","-Xmx512m","-cp",str(ROOT/"dist/lib/*"),
                        "io.openkache.server.replication.PbServerMain",str(ROOT/"node.properties")],
                       cwd=ROOT,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    log.close()
    PID.write_text(str(p.pid))
    return p

def stop():
    if not PID.exists():
        return
    pid=int(PID.read_text())
    if alive(pid):
        os.kill(pid,signal.SIGTERM)
        end=time.monotonic()+8
        while alive(pid) and time.monotonic()<end:
            time.sleep(.05)
        if alive(pid):
            os.kill(pid,signal.SIGKILL)
    PID.unlink(missing_ok=True)

if __name__=="__main__":
    if sys.argv[1]=="start":
        print(start().pid)
    elif sys.argv[1]=="stop":
        stop()
    else:
        raise ValueError("start|stop")
