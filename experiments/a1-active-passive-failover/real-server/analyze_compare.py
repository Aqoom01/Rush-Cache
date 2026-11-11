"""PB/VR 공통 cohort 24회 원본 검증·CSV·그래프. 접속 정보는 산출하지 않는다."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def validate(rows,detector):
    protocols=('PB','VR') if detector=='FIXED_TIMEOUT' else ('PB',)
    expected={(p,v,t,r) for p in protocols for v in (64,8192) for t in (500,1000) for r in (1,2,3)}
    assert len(rows)==len(expected)
    assert {(r['protocol'],r['valueBytes'],r['timeoutMs'],r['repeat']) for r in rows}==expected
    assert len({r['runId'] for r in rows})==24
    for r in rows:
        assert r['schema']=='a1-real-compare-1' and r['ok'] and r['timeDomain']=='wall-clock'
        assert r.get('failureDetector','FIXED_TIMEOUT')==detector
        assert r['heartbeatMs']==50 and r['tickMs']==10 and r['normalConcurrency']==1
        assert r['normalRequests']==r['preservedSuccesses']==len(r['normalSamplesNs'])==128
        samples=sorted(r['normalSamplesNs'])
        assert min(samples)>0 and r['normalP50Ns']==samples[63] and r['normalP99Ns']==samples[126]
        assert r['preCrashVerifiedCopies']==256 and r['postCrashVerifiedCopies']==258
        assert r['staleEpochStatus']==5 and r['backupWriteStatus']==4
        assert r['failoverNs']==r['firstNewPrimaryOkNs']-r['faultCallBeginNs']>0
        assert r['faultInvocationNs']==r['faultCallReturnNs']-r['faultCallBeginNs']>=0
        epoch=1 if r['protocol']=='VR' else 2
        s=r['statesAfter'][0]
        assert s['node']==1 and s['epoch']==epoch and s['protocol']==r['protocol']
        assert r['promotionNs']==s['readyNs']-s['detectedNs']>=0
        if r['protocol']=='VR':
            # OK completion precedes publish() in the owner callback. The immediate
            # immutable observation may still show the last SET as unapplied;
            # value preservation is checked separately by actual local KV reads.
            assert s['state']=='NORMAL' and s['logLength']==s['commit']==159
            assert s['applied'] in (158,159)
        assert r['attempts'][-1]['node']==1 and r['attempts'][-1]['status']==0
        assert math.isclose(r['observedSingleClientOpsPerSecond'],128e9/r['normalElapsedNs'])
        assert len(r['normalBytesBegin'])==len(r['normalBytesEnd'])==3
        assert len(r.get('normalFdBytesBegin',[0,0,0]))==len(r.get('normalFdBytesEnd',[0,0,0]))==3
        assert all(end>=start for start,end in zip(r['normalBytesBegin'],r['normalBytesEnd']))


def main():
    p=argparse.ArgumentParser(); p.add_argument('raw',type=Path)
    p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent)
    args=p.parse_args()
    manifest=json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))
    detector=manifest.get('failureDetector','FIXED_TIMEOUT')
    protocols=('PB','VR') if detector=='FIXED_TIMEOUT' else ('PB',)
    paths=sorted(args.raw.glob('*-v*-t*-r*.json'))
    rows=[json.loads(path.read_text(encoding='utf-8')) for path in paths]
    validate(rows,detector)
    output=args.output; data=output/'compare-results'; data.mkdir(parents=True,exist_ok=True)
    (output/'figures').mkdir(exist_ok=True)
    fields=('protocol','failureDetector','valueBytes','timeoutMs','repeat','normalP50Ns','normalP99Ns',
            'observedSingleClientOpsPerSecond','failoverNs','promotionNs','preservedSuccesses','postCrashVerifiedCopies',
            'normalPeerBytes','normalFdBytes','immediateObservationApplyLag')
    for r in rows:
        r['failureDetector']=r.get('failureDetector','FIXED_TIMEOUT')
        r['normalPeerBytes']=sum(r['normalBytesEnd'])-sum(r['normalBytesBegin'])
        r['normalFdBytes']=sum(r.get('normalFdBytesEnd',[0,0,0]))-sum(r.get('normalFdBytesBegin',[0,0,0]))
        s=r['statesAfter'][0]
        r['immediateObservationApplyLag']=s.get('commit',0)-s.get('applied',0)
    with (data/'runs.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n'); w.writeheader(); w.writerows(rows)
    summary=[]
    for protocol in protocols:
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
                    peerBytesPerWriteMedian=statistics.median(r['normalPeerBytes']/128 for r in g),
                    fdBytesPerWriteMedian=statistics.median(r['normalFdBytes']/128 for r in g)))
    with (data/'summary.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(summary[0]),lineterminator='\n'); w.writeheader(); w.writerows(summary)
    metadata={'runCount':len(rows),'failureDetector':detector,'normalSuccesses':128*len(rows),'preservedSuccesses':128*len(rows),'postCrashVerifiedCopies':258*len(rows),
              'immediateObservationLagRuns':sum(r['immediateObservationApplyLag']>0 for r in rows),
              'distributionSha256':manifest['distSha256'],
              'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths},
              'scope':'same cohort: warmup30/normal128, settled idle SIGKILL, closed-loop concurrency1; no SSD/max throughput claim'}
    (data/'sources.json').write_text(json.dumps(metadata,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    plt.rcParams.update({'font.size':10,'figure.dpi':160})
    conditions=[(64,500),(64,1000),(8192,500),(8192,1000)]
    def panel(ax,field,divisor,title,ylabel):
        series=[('PB',0 if len(protocols)==1 else -.13,'#497eb8')]
        if 'VR' in protocols: series.append(('VR',.13,'#d29041'))
        for protocol,offset,color in series:
            for x,(size,timeout) in enumerate(conditions):
                values=[r[field]/divisor for r in rows if (r['protocol'],r['valueBytes'],r['timeoutMs'])==(protocol,size,timeout)]
                median=statistics.median(values)
                ax.errorbar(x+offset,median,yerr=[[median-min(values)],[max(values)-median]],fmt='o',capsize=5,color=color,label=protocol if x==0 else None)
                ax.scatter([x+offset-.025,x+offset,x+offset+.025],values,color=color,s=14,alpha=.5)
        ax.set_xticks(range(4),['64B\n500ms','64B\n1000ms','8KiB\n500ms','8KiB\n1000ms'])
        ax.set_title(title); ax.set_ylabel(ylabel); ax.set_xlabel('Value / failure detection timeout')
        ax.set_ylim(bottom=0); ax.grid(axis='y',alpha=.2); ax.legend()
    fig,axes=plt.subplots(1,2,figsize=(11,4.8))
    panel(axes[0],'failoverNs',1e6,'SIGKILL call -> first new-primary SET OK','Controller elapsed ms')
    panel(axes[1],'promotionNs',1e6,'Detection -> write-ready','Same-JVM elapsed ms')
    fig.suptitle(f'{detector}: 3 EC2 nodes, {len(rows)} settled idle-crash runs')
    fig.tight_layout(); fig.savefig(output/'figures/compare-failover.png'); plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(11,4.8))
    panel(axes[0],'normalP99Ns',1e6,'Run p99: 128 sequential SET samples','ms (3 runs: median and min/max)')
    panel(axes[1],'normalPeerBytes',128,'Peer frame bytes / measured SET','Bytes (includes heartbeat traffic)')
    fig.suptitle('Same warmup30 / normal128 cohort; controller on original Primary')
    fig.tight_layout(); fig.savefig(output/'figures/compare-normal.png'); plt.close(fig)
    title='PB·VR 동일 조건 실제 서버 비교' if detector=='FIXED_TIMEOUT' else f'PB {detector} 실제 서버 자동 failover'
    lines=[f'# {title}','',
           f'같은 배포본·요청 수에서 failure detector `{detector}`를 사용한 독립 cohort다.','',
           '## 조건','',
           '- 같은 AZ의 m6gd.medium 3대, Ubuntu 24.04 arm64, Java21, 노드별 인메모리 KV·private TCP. SWIM 계열은 private UDP 7002.',
           '- PB 전원 적용 ACK. FIXED_TIMEOUT cohort의 VR은 INCREMENTAL 과반수 commit + view change.',
           f'- {len(protocols)}방식 × 값64B/8KiB × 감지500/1000ms × 3회 = **{len(rows)}회**. HB50ms·tick10ms.',
           '- warmup30건 후 고유 key SET128건. 원래 Primary 호스트의 Python client, closed-loop 동시성1.',
           '- 양쪽 Backup의 측정 key 값 검증 후 Primary JVM SIGKILL. 진행 중 쓰기·인위적 네트워크 장애 없음.',
           '- 같은 요청 재시도, 간격10ms·deadline1초·socket1.5초. EC2 자체는 종료하지 않음.','',
           '## 결과','',
           '각 수치는 실행 3회의 중앙값이다. failover 범위는 최소~최대다. p99는 실행당 128개 표본의 nearest-rank이며 운영 p99 추정은 아니다.','',
           '|방식|값|감지 ms|SET p50 ms|SET p99 ms|failover ms (범위)|전환 ms|peer B/SET|FD B/SET|',
           '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summary:
        lines.append(f"|{s['protocol']}|{s['valueBytes']}|{s['timeoutMs']}|{s['p50MedianMs']:.2f}|{s['p99MedianMs']:.2f}|{s['failoverMedianMs']:.2f} ({s['failoverMinMs']:.2f}~{s['failoverMaxMs']:.2f})|{s['transitionMedianMs']:.2f}|{s['peerBytesPerWriteMedian']:.0f}|{s['fdBytesPerWriteMedian']:.0f}|")
    lines+=['','![failover](figures/compare-failover.png)','','![normal](figures/compare-normal.png)','',
            '## 검증과 해석 범위','',
            f'- 정상 SET **{128*len(rows)}건** 전부 OK. 장애 후 생존 두 노드에서 기존128건+첫 새 쓰기 총 **{258*len(rows)}개 값 복사본** 일치. stale epoch·Backup 직접 쓰기 거절을 {len(rows)}회 확인.',
            '- 전환 시간은 새 Primary 자신의 readyNs−detectedNs만 사용한다. 전체 failover는 controller의 같은 perf_counter 기준이다. 서로 다른 호스트 시각을 빼지 않는다.',
            '- 실제 TCP/UDP·JVM 스케줄링 비용을 포함한다. 표본3회·동시성1이므로 순위 일반화나 최대 처리량 결론을 내리지 않는다.',
            '- 정상 peer bytes는 노드3개의 비동기 누적 counter 차이이며 heartbeat와 관측 경계 오차를 포함한다. 순수 payload 비용이나 crash 중 상태 전달량은 아니다.',
            '- FD bytes는 UDP payload만 집계하며 IP/UDP overhead와 손실된 datagram은 포함하지 않는다.',
            '- 두 Backup의 값을 확인한 뒤 종료한 settled idle crash다. 미완료 쓰기, partition·재시작 안전성을 이 결과로 보장하지 않는다.','']
    if 'VR' in protocols:
        lines += ['- VR의 view change는 전체 로그를 보내며 PB와 보장 범위가 다르다. 이 결과는 원 논문/제품 전체 구현 성능 비교가 아니다.','']
    lines += [
            '## 재현과 근거','',
            '`run_compare_ec2.py`로 새 원본 폴더에 실행한 후 `analyze_compare.py <원본폴더>`를 실행한다. 본 실험은 --quick 없이 수행했다. 실행 tar·스크립트·환경·원본 JSON은 Git 제외 로컬 폴더에 보존한다.',
            '[실행별 CSV](compare-results/runs.csv) · [조건별 CSV](compare-results/summary.csv) · [SHA-256 근거](compare-results/sources.json)','']
    (output/'compare-results.md').write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
