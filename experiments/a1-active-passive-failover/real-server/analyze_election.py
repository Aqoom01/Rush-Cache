"""PB/VR/Raft 공통 cohort 36회 원본 검증·CSV·그래프. 접속 정보는 산출하지 않는다."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def validate(rows):
    expected={(p,v,t,r) for p in ('PB','VR','RAFT') for v in (64,8192) for t in (500,1000) for r in (1,2,3)}
    assert len(rows)==36
    assert {(r['protocol'],r['valueBytes'],r['timeoutMs'],r['repeat']) for r in rows}==expected
    assert len({r['runId'] for r in rows})==36
    for r in rows:
        assert r['schema']=='a1-real-election-1' and r['ok'] and r['timeDomain']=='wall-clock'
        assert r['heartbeatMs']==50 and r['tickMs']==10 and r['normalConcurrency']==1
        assert r['normalRequests']==r['preservedSuccesses']==len(r['normalSamplesNs'])==128
        samples=sorted(r['normalSamplesNs'])
        assert min(samples)>0 and r['normalP50Ns']==samples[63] and r['normalP99Ns']==samples[126]
        assert r['preCrashVerifiedCopies']==256 and r['postCrashVerifiedCopies']==258
        assert r['staleEpochStatus']==5 and r['backupWriteStatus']==4
        assert r['failoverNs']==r['firstNewPrimaryOkNs']-r['faultCallBeginNs']>0
        assert r['faultInvocationNs']==r['faultCallReturnNs']-r['faultCallBeginNs']>=0
        initial=r['initialEpoch']; new=r['newEpoch']
        assert r['originalLeader']!=r['newLeader'] and new>initial
        s=next(s for s in r['statesAfter'] if s['node']==r['newLeader'])
        assert s['epoch']==new and s['protocol']==r['protocol']
        assert r['promotionNs']==s['readyNs']-s['detectedNs']>=0
        assert r['attempts'][-1]['node']==r['newLeader'] and r['attempts'][-1]['status']==0
        if r['protocol']=='RAFT':
            assert s['role']=='LEADER' and s['electionTerm']==new and s['applied']==s['commit']==s['logLength']>=161
            assert s['detectedNs']>r['statesBefore'][r['newLeader']]['detectedNs']
            assert r['electionTimeoutRangeMs']==[r['timeoutMs'],2*r['timeoutMs']]
            assert r['electionSeed']==r['repeat']
        else:
            assert r['originalLeader']==0 and r['newLeader']==1
            assert initial==(0 if r['protocol']=='VR' else 1) and new==initial+1
        assert math.isclose(r['observedSingleClientOpsPerSecond'],128e9/r['normalElapsedNs'])
        assert len(r['normalBytesBegin'])==len(r['normalBytesEnd'])==3
        assert all(end>=start for start,end in zip(r['normalBytesBegin'],r['normalBytesEnd']))


def main():
    p=argparse.ArgumentParser(); p.add_argument('raw',type=Path)
    p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent)
    args=p.parse_args()
    paths=sorted(args.raw.glob('*-v*-t*-r*.json'))
    rows=[json.loads(path.read_text(encoding='utf-8')) for path in paths]
    validate(rows)
    output=args.output; data=output/'election-results'; data.mkdir(parents=True,exist_ok=True)
    (output/'figures').mkdir(exist_ok=True)
    fields=('protocol','valueBytes','timeoutMs','repeat','normalP50Ns','normalP99Ns',
            'observedSingleClientOpsPerSecond','failoverNs','promotionNs','preservedSuccesses','postCrashVerifiedCopies','normalPeerBytes','postAuditObservationApplyLag','originalLeader','newLeader','initialEpoch','newEpoch','electionSeed')
    for r in rows:
        r['normalPeerBytes']=sum(r['normalBytesEnd'])-sum(r['normalBytesBegin'])
        s=next(s for s in r['statesAfter'] if s['node']==r['newLeader'])
        r['postAuditObservationApplyLag']=s.get('commit',0)-s.get('applied',0)
    with (data/'runs.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n'); w.writeheader(); w.writerows(rows)
    summary=[]
    for protocol in ('PB','VR','RAFT'):
        for size in (64,8192):
            for timeout in (500,1000):
                g=[r for r in rows if (r['protocol'],r['valueBytes'],r['timeoutMs'])==(protocol,size,timeout)]
                summary.append(dict(protocol=protocol,valueBytes=size,timeoutMs=timeout,
                    p50MedianMs=statistics.median(r['normalP50Ns']/1e6 for r in g),
                    p99MedianMs=statistics.median(r['normalP99Ns']/1e6 for r in g),
                    opsMedian=statistics.median(r['observedSingleClientOpsPerSecond'] for r in g),
                    failoverMedianMs=statistics.median(r['failoverNs']/1e6 for r in g),
                    failoverMinMs=min(r['failoverNs']/1e6 for r in g),failoverMaxMs=max(r['failoverNs']/1e6 for r in g),
                    transitionMedianMs=statistics.median(r['promotionNs']/1e6 for r in g),
                    peerBytesPerWriteMedian=statistics.median(r['normalPeerBytes']/128 for r in g)))
    with (data/'summary.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(summary[0]),lineterminator='\n'); w.writeheader(); w.writerows(summary)
    metadata={'runCount':36,'normalSuccesses':4608,'preservedSuccesses':4608,'postCrashVerifiedCopies':9288,
              'postAuditObservationLagRuns':sum(r['postAuditObservationApplyLag']>0 for r in rows),
              'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
              'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths},
              'scope':'same cohort: warmup30/normal128, settled idle SIGKILL, closed-loop concurrency1; no SSD/max throughput claim'}
    (data/'sources.json').write_text(json.dumps(metadata,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    plt.rcParams.update({'font.size':10,'figure.dpi':160})
    conditions=[(64,500),(64,1000),(8192,500),(8192,1000)]
    def panel(ax,field,divisor,title,ylabel):
        for protocol,offset,color in [('PB',-.2,'#497eb8'),('VR',0,'#d29041'),('RAFT',.2,'#459a76')]:
            for x,(size,timeout) in enumerate(conditions):
                values=[r[field]/divisor for r in rows if (r['protocol'],r['valueBytes'],r['timeoutMs'])==(protocol,size,timeout)]
                median=statistics.median(values)
                ax.errorbar(x+offset,median,yerr=[[median-min(values)],[max(values)-median]],fmt='o',capsize=5,color=color,label=protocol if x==0 else None)
                ax.scatter([x+offset-.025,x+offset,x+offset+.025],values,color=color,s=14,alpha=.5)
        ax.set_xticks(range(4),['64B\n500ms','64B\n1000ms','8KiB\n500ms','8KiB\n1000ms'])
        ax.set_title(title); ax.set_ylabel(ylabel); ax.set_xlabel('Value / timeout base (Raft range: base to 2 x base)')
        ax.set_ylim(bottom=0); ax.grid(axis='y',alpha=.2); ax.legend()
    fig,axes=plt.subplots(1,2,figsize=(11,4.8))
    panel(axes[0],'failoverNs',1e6,'SIGKILL call -> first new-primary SET OK','Controller elapsed ms')
    panel(axes[1],'promotionNs',1e6,'Local detection/election -> write-ready','Same-JVM elapsed ms')
    fig.suptitle('PB / VR / Raft: 3 EC2 nodes, 36 settled idle-crash runs')
    fig.tight_layout(); fig.savefig(output/'figures/election-failover.png'); plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(11,4.8))
    panel(axes[0],'normalP99Ns',1e6,'Run p99: 128 sequential SET samples','ms (3 runs: median and min/max)')
    panel(axes[1],'normalPeerBytes',128,'Peer frame bytes / measured SET','Bytes (includes heartbeat traffic)')
    fig.suptitle('Same warmup30 / normal128 cohort; controller on original Primary')
    fig.tight_layout(); fig.savefig(output/'figures/election-normal.png'); plt.close(fig)
    write_report(output,summary,rows)
    print(json.dumps(summary,indent=2))


def write_report(output,summary,rows):
    lines=['# PB·VR·Raft 동일 배포본 실서버 비교','',
           '관련 이슈: S15P31D111-186. 기존 결과와 분리한 새 36회 cohort다.','',
           '## 실험 조건','',
           '- 같은 AZ의 m6gd.medium 3대, Ubuntu24.04 arm64, Java21, 노드별 인메모리 KV·private TCP.',
           '- PB 전원 적용 ACK·고정 승계 / VR INCREMENTAL 과반수·view change / Raft 과반수·투표 선출.',
           '- 방식3 × 값64B/8KiB × timeout 기준500/1000ms × 반복3 =36회. warmup30건·정상 SET128건, HB50ms·tick10ms.',
           '- PB/VR은 고정 감지 timeout. Raft는 선거 timeout을 [기준,2×기준)에서 뽑는다. 동일한 실제 감지 지연을 강제한 비교가 아니며 난수 범위도 프로토콜 조건으로 보고한다.',
           '- Raft 노드별 난수 seed=반복번호+104729×노드ID. 값 payload는 세 방식 모두 같은 생성식이다. 실제 네트워크·스케줄 순서는 결정론적이지 않다.',
           '- 초기 리더 선출·현재 로그 적용 완료를 기다린다. 실제 선출된 리더 EC2에서 Python controller를 실행하므로 controller는 항상 장애 전 리더와 같은 호스트다. Raft 리더 ID를0으로 고정하지 않는다.',
           '- 양쪽 Backup 값 검증 후 원래 리더 JVM만 SIGKILL. 진행 중 쓰기 없음. 같은 논리 SET 재시도, 간격10ms·deadline1초·socket timeout1.5초.',
           '- closed-loop 동시성1. 측정128건은 현재2MiB frame 한도에 맞춘 것으로 checkpoint·분할 전송은 없다.','',
           '## 결과','',
           '각 셀은 실행3회 중앙값, failover 괄호는 최소~최대다. p99는128표본 nearest-rank로 운영 환경의 p99 추정이 아니다.','',
           '|방식|값 B|timeout 기준 ms|SET p50 ms|SET p99 ms|failover ms (범위)|감지/선거 후 준비 ms|peer B/SET|',
           '|---|---:|---:|---:|---:|---:|---:|---:|']
    for s in summary:
        lines.append(f"|{s['protocol']}|{s['valueBytes']}|{s['timeoutMs']}|{s['p50MedianMs']:.2f}|{s['p99MedianMs']:.2f}|{s['failoverMedianMs']:.2f} ({s['failoverMinMs']:.2f}~{s['failoverMaxMs']:.2f})|{s['transitionMedianMs']:.2f}|{s['peerBytesPerWriteMedian']:.0f}|")
    leaders=[(r['originalLeader'],r['newLeader']) for r in rows if r['protocol']=='RAFT']
    lines+=['','![failover](figures/election-failover.png)','','![normal](figures/election-normal.png)','',
            '## 검증·해석 범위','',
            '- 정상4608건 OK, 기존 성공 쓰기4608건 보존. 장애 후 생존 두 노드의 기존128개+첫 새 쓰기 총9288개 값 복사본 일치. stale epoch·Backup 직접 쓰기 거절36회 확인.',
            '- 실제 Raft 리더 이동(실행 순서의 이전→이후): '+', '.join(f'{a}→{b}' for a,b in leaders)+'.',
            '- 전체 failover는 controller의 SIGKILL 호출 직전→새 리더 첫 OK까지다. 전환은 승격 노드 내부 시계만 차감한다. 서로 다른 호스트의 nanoTime을 직접 빼지 않는다.',
            '- Raft 전환 지표는 **승리한 최근 로컬 선거의 후보 시작→현재 term no-op 포함 적용 준비 완료**다. 초기 선거 누적값을 쓰지 않도록 선거별 진단을 추가했다. 선행 실패 선거/다른 노드 감지 대기는 전체 failover에만 포함되므로 전환 지표를 프로토콜 전체 선거 비용으로 해석하지 않는다.',
            '- timeout 범위·리더 배치·정상 전송 패턴이 다르므로 이 구현과 조건의 결과다. 장애 감지 후 전환만으로 가용성/안전성 우열을 판단하지 않는다.',
            '- 정상 peer bytes는 누적 전송 frame counter의 차이로 heartbeat·재전송·상태 전달 fallback과 관측 경계 오차를 포함한다. opcode별 원인 분해는 없다.',
            '- 실제 KV 값 검증을 보존 판정으로 사용한다. OK 완료 후 observation publish 순서 때문에 즉시 진단 snapshot은 적용 위치가 잠시 뒤처질 수 있다.',
            '- 각 조건3회·짧은 warmup·동시성1·PB→VR→Raft 순서 실행이다. 최대 처리량·SSD·논문 전체 시스템 성능·partition·재시작 보장을 주장하지 않는다. Raft term/vote/log는 메모리이며 crash/restart durability는 미구현이다.',
            '- 이번 실험은 양쪽 Backup 반영 후 idle crash다. 정족수만 반영된 순간의 crash나 미완료 쓰기 검증은 아니다. 기존 PB42회·PB/VR24회 결과와 섞지 않았다.','',
            '## 재현·근거','',
            '`run_election_ec2.py` 본 실행 후 `analyze_election.py <원본폴더>`. --quick은64B/500ms 각 방식1회로 본집계에서 제외한다. 실행tar·script·환경·원본은 로컬 Git제외 폴더에 보존한다.',
            '[실행별 CSV](election-results/runs.csv) · [조건별 CSV](election-results/summary.csv) · [SHA-256 근거](election-results/sources.json)','']
    (output/'election-results.md').write_text('\n'.join(lines),encoding='utf-8')


if __name__=='__main__':
    main()
