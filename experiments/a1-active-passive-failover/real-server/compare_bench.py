"""Primary 호스트의 controller에서 실제 프로세스 종료와 client 응답을 같은 perf_counter로 잰다."""
import argparse
import json
import math
from pathlib import Path
import platform
import signal
import socket
import struct
import time
import uuid
import pb_control

class Client:
    def __init__(self,hosts,on_sent=None):
        self.hosts=hosts
        self.on_sent=on_sent
        self.sockets={}
        self.sequence=0
    def close(self):
        for sock in self.sockets.values():
            sock.close()
        self.sockets.clear()
    def request(self,node,opcode,body=b"",peer=False):
        port=7001 if peer else 7000
        key=(node,port)
        self.sequence+=1
        try:
            if key not in self.sockets:
                self.sockets[key]=socket.create_connection((self.hosts[node],port),timeout=1.5)
                self.sockets[key].setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            sock=self.sockets[key]
            header=struct.pack("!HBBBBHQQ",0x4f4b,1,3 if peer else 1,opcode,0,0,self.sequence,0)
            sock.sendall(struct.pack("!I",24+len(body))+header+body)
            if self.on_sent is not None:
                self.on_sent(node,opcode,peer,self.sequence)
            size=struct.unpack("!I",self.exact(sock,4))[0]
            if size<48 or size>2097152:
                raise RuntimeError("invalid response length")
            data=self.exact(sock,size)
            magic,version,cls,op,flags,status,request_id,routing=struct.unpack("!HBBBBHQQ",data[:24])
            if (magic,version,cls,op,flags,request_id)!=(0x4f4b,1,2,opcode,0,self.sequence):
                raise RuntimeError("invalid response header")
            epoch,value_version,detail_len=struct.unpack("!QQI",data[24:44])
            pos=44
            detail=data[pos:pos+detail_len].decode()
            pos+=detail_len
            value_len=struct.unpack("!I",data[pos:pos+4])[0]
            value=data[pos+4:]
            if len(value)!=value_len:
                raise RuntimeError("invalid response body")
            return {"status":status,"epoch":epoch,"version":value_version,"detail":detail,"value":value}
        except (OSError,EOFError):
            sock=self.sockets.pop(key,None)
            if sock:
                sock.close()
            raise
    @staticmethod
    def exact(sock,n):
        chunks=bytearray()
        while len(chunks)<n:
            part=sock.recv(n-len(chunks))
            if not part:
                raise EOFError("peer closed")
            chunks.extend(part)
        return bytes(chunks)
    def set(self,node,operation,key,value,epoch=1,deadline=1000):
        body=struct.pack("!QQQII",17,operation,epoch,deadline,len(key))+key+struct.pack("!I",len(value))+value
        return self.request(node,2,body)
    def status(self,node):
        response=self.request(node,35,peer=True)
        assert response["status"]==0
        return json.loads(response["value"])
    def local(self,node,key):
        return self.request(node,36,key,peer=True)

def quantile(values,q):
    return sorted(values)[math.ceil(len(values)*q)-1] if values else 0

def run(args):
    initial=0 if args.protocol=="VR" else 1
    hosts=args.hosts.split(",")
    client=Client(hosts)
    primary=None
    result={"schema":"a1-real-compare-1","protocol":args.protocol,"timeDomain":"wall-clock","durationUnit":"ns",
            "runId":uuid.uuid4().hex,"utcEpochMs":int(time.time()*1000),
            "valueBytes":args.value,"timeoutMs":args.timeout,"repeat":args.repeat,
            "heartbeatMs":50,"tickMs":10,"clientPlacement":"original-primary-host","normalConcurrency":1,
            "normalRequests":args.count,"seed":1,"platform":platform.platform(),
            "startupGate":"backup failure detection starts after first primary heartbeat",
            "failureModel":"original primary JVM SIGKILL after acknowledged unique-key SETs; no in-flight write at injection",
            "normalSamplesNs":[],"attempts":[]}
    try:
        primary=pb_control.start()
        end=time.monotonic()+20
        while True:
            try:
                states=[client.status(i) for i in range(3)]
                if all(s["epoch"]==initial for s in states) and all(s["heartbeatSeen"] for s in states[1:]) and all(s.get("state","NORMAL")=="NORMAL" for s in states):
                    break
            except (OSError,EOFError):
                pass
            if time.monotonic()>end:
                raise RuntimeError("startup readiness failed")
            time.sleep(.05)
        value=bytes((i*17+3)%256 for i in range(args.value))
        retained=[]
        for i in range(30):
            response=client.set(0,100+i,f"warm-{i}".encode(),value,initial)
            if response["status"]!=0:
                raise RuntimeError(f"warmup failed: {response}")
        result["statesBefore"]=states
        result["normalBytesBegin"]=[client.status(i)["sentBytes"] for i in range(3)]
        begin=time.perf_counter_ns()
        for i in range(args.count):
            key=f"pre-{i}".encode()
            before=time.perf_counter_ns()
            response=client.set(0,1000+i,key,value,initial)
            after=time.perf_counter_ns()
            if response["status"]!=0:
                raise RuntimeError(f"normal SET failed: {response}")
            retained.append(key)
            result["normalSamplesNs"].append(after-before)
        normal_end=time.perf_counter_ns()
        result["normalElapsedNs"]=normal_end-begin
        result["normalP50Ns"]=quantile(result["normalSamplesNs"],.5)
        result["normalP99Ns"]=quantile(result["normalSamplesNs"],.99)
        result["observedSingleClientOpsPerSecond"]=args.count*1e9/(normal_end-begin)
        result["normalBytesEnd"]=[client.status(i)["sentBytes"] for i in range(3)]
        # Wait for the second VR backup too: this cohort measures settled idle crash.
        time.sleep(.2)
        for node in (1,2):
            for key in retained:
                r=client.local(node,key)
                assert r["status"]==0 and r["value"]==value,"pre-crash replica mismatch"
        result["preCrashVerifiedCopies"]=len(retained)*2
        result["faultCallBeginNs"]=time.perf_counter_ns()
        primary.send_signal(signal.SIGKILL)
        result["faultCallReturnNs"]=time.perf_counter_ns()
        primary.wait(timeout=5)
        route=0
        epoch=initial
        budget=time.monotonic()+15
        while time.monotonic()<budget:
            attempt={"node":route,"epoch":epoch,"startNs":time.perf_counter_ns()}
            try:
                r=client.set(route,100000,b"post-first",value,epoch)
                attempt.update(status=r["status"],responseEpoch=r["epoch"],detail=r["detail"],endNs=time.perf_counter_ns())
                epoch=max(epoch,r["epoch"])
                result["attempts"].append(attempt)
                if r["status"]==0:
                    assert route==1 and epoch==initial+1,"success did not come from new Primary"
                    result["firstNewPrimaryOkNs"]=attempt["endNs"]
                    break
                if r["detail"].startswith("redirect-node="):
                    route=int(r["detail"].split("=")[1])
                elif r["status"] not in (5,8,6):
                    route=(route+1)%3
            except (OSError,EOFError) as error:
                attempt.update(status="CLIENT_FAILURE",error=type(error).__name__,endNs=time.perf_counter_ns())
                result["attempts"].append(attempt)
                route=(route+1)%3
            time.sleep(.01)
        else:
            raise RuntimeError("failover did not resume within 15 seconds")
        result["failoverNs"]=result["firstNewPrimaryOkNs"]-result["faultCallBeginNs"]
        result["faultInvocationNs"]=result["faultCallReturnNs"]-result["faultCallBeginNs"]
        states=[client.status(i) for i in (1,2)]
        result["statesAfter"]=states
        result["promotionNs"]=states[0]["readyNs"]-states[0]["detectedNs"]
        assert states[0]["readyNs"]>=states[0]["detectedNs"]>=0
        copies=0
        for node in (1,2):
            for key in retained+[b"post-first"]:
                r=client.local(node,key)
                assert r["status"]==0 and r["value"]==value,"post-crash successful write missing"
                copies+=1
        result["preservedSuccesses"]=len(retained)
        result["postCrashVerifiedCopies"]=copies
        result["staleEpochStatus"]=client.set(1,100001,b"stale",value,initial)["status"]
        assert result["staleEpochStatus"]==5
        backup=client.set(2,100002,b"backup-direct",value,initial+1)
        result["backupWriteStatus"]=backup["status"]
        assert backup["status"]==4 and backup["detail"]=="redirect-node=1"
        result["ok"]=True
    except Exception as error:
        result.update(ok=False,error=repr(error))
        raise
    finally:
        client.close()
        if primary is not None and primary.poll() is None:
            primary.terminate()
            primary.wait(timeout=10)
        Path(args.out).write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({k:result[k] for k in ("protocol","runId","ok","valueBytes","timeoutMs","repeat","normalP99Ns","failoverNs","promotionNs","preservedSuccesses")}))

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--protocol",choices=["PB","VR"],required=True)
    p.add_argument("--hosts",required=True)
    p.add_argument("--out",required=True)
    p.add_argument("--value",type=int,default=64)
    p.add_argument("--timeout",type=int,default=500)
    p.add_argument("--repeat",type=int,default=1)
    p.add_argument("--count",type=int,default=128)
    run(p.parse_args())
