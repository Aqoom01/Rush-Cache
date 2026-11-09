"""Chain 위치별 crash 36회 원본 검증·CSV·그래프. 접속 정보는 산출하지 않는다."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

POSITIONS=('head','middle','tail')
CONDITIONS=[(64,500),(64,1000),(8192,500),(8192,1000)]


def validate(rows,full=True):
    if full:
        assert len(rows)==36
        assert {(r['position'],r['valueBytes'],r['timeoutMs'],r['repeat']) for r in rows}=={(p,v,t,n) for p in POSITIONS for v in (64,8192) for t in (500,1000) for n in (1,2,3)}
    assert len({r['runId'] for r in rows})==len(rows)
    for r in rows:
        assert r['schema']=='a1-real-chain-2' and r['preCrashDigestsEqual'] and r['repairPolls']>=1 and r['ok'] and r['protocol']=='CHAIN' and r['readMode']=='TAIL_ONLY' and r['timeDomain']=='wall-clock'
        assert r['crashedNode']==POSITIONS.index(r['position']) and r['normalConcurrency']==1
        assert r['normalRequests']==len(r['normalSamplesNs'])==len(r['normalBusyRetries'])==128 and r['tailReads']==len(r['tailReadSamplesNs'])==128
        s=sorted(r['normalSamplesNs']);assert min(s)>0 and r['normalP50Ns']==s[63] and r['normalP99Ns']==s[126]
        t=sorted(r['tailReadSamplesNs']);assert min(t)>0 and r['tailReadP50Ns']==t[63] and r['tailReadP99Ns']==t[126]
        assert r['normalBusyTotal']==sum(r['normalBusyRetries'])
        assert r['headReadStatus']==4 and r['headReadDetail']=='redirect-node=2'
        assert r['preCrashVerifiedCopies']==474 and r['postCrashVerifiedCopies']==318 and r['preservedSuccesses']==158
        assert all(x['logLength']==x['applied']==x['acknowledged']==158 and x['epoch']==1 and x['chain']==[0,1,2] for x in r['statesBefore'])
        survivors=[n for n in (0,1,2) if n!=r['crashedNode']]
        assert r['newHead']==survivors[0] and r['newTail']==survivors[-1] and r['newEpoch']==2 and r['repairedLength']==158
        assert r['freezeReplies']==['applied=158','applied=158']
        assert r['failoverNs']==r['firstNewHeadOkNs']-r['faultCallBeginNs']>0
        assert r['failoverNs']==r['detectWaitNs']+r['freezeNs']+r['repairNs']+r['openNs']+r['firstWriteNs']
        assert r['repairNs']==r['repairControlNs']+r['repairPollNs']+r['repairDigestNs']
        assert all(x['state']=='REPAIRING' and x['logLength']==x['applied']==x['acknowledged']==158 for x in r['repairLastPoll'])
        assert r['detectWaitNs']>=r['timeoutMs']*1_000_000
        assert all(x['epoch']==2 and x['state']=='RUNNING' and x['chain']==survivors and x['logLength']==x['applied']==x['acknowledged']==159 for x in r['statesAfter'])
        assert r['promotionNs']==max(x['readyNs']-x['detectedNs'] for x in r['statesAfter'])>=0
        assert r['staleEpochStatus']==5 and r['nonHeadWriteStatus']==4 and r['nonHeadWriteDetail']==f"redirect-node={r['newHead']}"
        assert r['attempts'][-1]['status']==0 and r['attempts'][-1]['node']==r['newHead']


def summarize(r):
    before=r['statesBefore'];initial=r['statesInitial']
    peer=sum(x['peerTx']['totalBytes'] for x in before)-sum(x['peerTx']['totalBytes'] for x in initial)
    return dict(position=r['position'],valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],
                normalP50Ms=r['normalP50Ns']/1e6,normalP99Ms=r['normalP99Ns']/1e6,normalBusyTotal=r['normalBusyTotal'],
                tailReadP50Ms=r['tailReadP50Ns']/1e6,tailReadP99Ms=r['tailReadP99Ns']/1e6,
                failoverMs=r['failoverNs']/1e6,detectWaitMs=r['detectWaitNs']/1e6,freezeMs=r['freezeNs']/1e6,repairMs=r['repairNs']/1e6,
                repairControlMs=r['repairControlNs']/1e6,repairPollMs=r['repairPollNs']/1e6,repairDigestMs=r['repairDigestNs']/1e6,repairPolls=r['repairPolls'],
                preCrashDigestMs=r['preCrashDigestNs']/1e6,openMs=r['openNs']/1e6,firstWriteMs=r['firstWriteNs']/1e6,promotionMs=r['promotionNs']/1e6,
                peerBytesWarmAndNormal=peer,peerBytesPerSet=peer/158,newHead=r['newHead'],newTail=r['newTail'],preservedSuccesses=r['preservedSuccesses'],
                postCrashVerifiedCopies=r['postCrashVerifiedCopies'])


def main():
    p=argparse.ArgumentParser();p.add_argument('raw',type=Path);p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent);args=p.parse_args()
    paths=sorted(args.raw.glob('CHAIN-*-v*-t*-r*.json'));rows=[json.loads(x.read_text(encoding='utf-8')) for x in paths];validate(rows)
    out=args.output;data=out/'chain-results';data.mkdir(parents=True,exist_ok=True);(out/'figures').mkdir(exist_ok=True)
    runs=[summarize(r) for r in rows];summary=[]
    for position in POSITIONS:
        for size,timeout in CONDITIONS:
            g=[x for x in runs if (x['position'],x['valueBytes'],x['timeoutMs'])==(position,size,timeout)]
            med=lambda k:statistics.median(x[k] for x in g)
            summary.append(dict(position=position,valueBytes=size,timeoutMs=timeout,runs=len(g),
                                normalP50MsMedian=med('normalP50Ms'),normalP99MsMedian=med('normalP99Ms'),busyRetriesTotal=sum(x['normalBusyTotal'] for x in g),
                                tailReadP50MsMedian=med('tailReadP50Ms'),tailReadP99MsMedian=med('tailReadP99Ms'),
                                failoverMsMedian=med('failoverMs'),failoverMsMin=min(x['failoverMs'] for x in g),failoverMsMax=max(x['failoverMs'] for x in g),
                                detectWaitMsMedian=med('detectWaitMs'),freezeMsMedian=med('freezeMs'),repairMsMedian=med('repairMs'),repairControlMsMedian=med('repairControlMs'),
                                repairPollMsMedian=med('repairPollMs'),repairDigestMsMedian=med('repairDigestMs'),openMsMedian=med('openMs'),
                                firstWriteMsMedian=med('firstWriteMs'),promotionMsMedian=med('promotionMs'),peerBytesPerSetMedian=med('peerBytesPerSet')))
    for name,records in [('runs',runs),('summary',summary)]:
        with (data/f'{name}.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(records[0]),lineterminator='\n');w.writeheader();w.writerows(records)
    meta={'runCount':len(rows),'normalSuccesses':128*len(rows),'tailReads':128*len(rows),'verifiedCopiesPre':474*len(rows),'verifiedCopiesPost':318*len(rows),
          'busyRetriesTotal':sum(x['normalBusyTotal'] for x in runs),
          'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
          'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
    (data/'sources.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    labels=['64B\n500ms','64B\n1000ms','8KiB\n500ms','8KiB\n1000ms'];colors={'head':'#497eb8','middle':'#d29041','tail':'#459a76'}
    fig,axes=plt.subplots(1,3,figsize=(16,5))
    for pi,position in enumerate(POSITIONS):
        for ci,(size,timeout) in enumerate(CONDITIONS):
            g=[x for x in runs if (x['position'],x['valueBytes'],x['timeoutMs'])==(position,size,timeout)]
            xs=[ci+(pi-1)*.25]*len(g)
            axes[0].scatter(xs,[x['failoverMs'] for x in g],color=colors[position],label=position if ci==0 else None)
            axes[1].scatter(xs,[x['promotionMs'] for x in g],color=colors[position],label=position if ci==0 else None)
            axes[2].scatter(xs,[x['normalP99Ms'] for x in g],color=colors[position],marker='o',label=f'SET p99 ({position} run)' if ci==0 else None)
            axes[2].scatter(xs,[x['tailReadP99Ms'] for x in g],color=colors[position],marker='^',label=f'tail GET p99 ({position} run)' if ci==0 else None)
    axes[0].set_title('Controller failover: SIGKILL → first OK at new head\n(includes fixed detection wait = timeout.ms)');axes[0].set_ylabel('ms')
    axes[1].set_title('Server promotion: FREEZE received → RUNNING (max over survivors)');axes[1].set_ylabel('ms')
    axes[2].set_title('Normal SET p99 (incl. BUSY retries) and tail GET p99');axes[2].set_ylabel('ms')
    for ax in axes:
        ax.set_xticks(range(4),labels);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2);ax.legend(fontsize=7)
    fig.suptitle('Chain (TAIL_ONLY) on 3 EC2 nodes: positional crash, controller-as-master, 3 runs per condition')
    fig.tight_layout();fig.savefig(out/'figures/chain-failover.png');plt.close(fig)
    lines=['# Chain 실서버: 정상 쓰기·tail 읽기·위치별 crash','',
           '관련 이슈: S15P31D111-187 후속(9단계). 기존 `ChainEngine`(head 쓰기·tail 확정·역방향 Ack)을 실제 TCP에 연결하고, 정확한 영구 crash를 아는 **외부 master 역할을 실험 controller가 수행**했다. 이 결과는 master 합의·실제 failure detector·분단 fencing을 포함하지 않는다.','',
           '## 실험 조건','',
           '- 전용 EC2 세 대, 같은 AZ·m6gd.medium·Ubuntu24.04 arm64·Java21·인메모리 KV·private TCP. 초기 사슬 0(head)→1→2(tail), epoch1, 재전송·제어 간격 retry.ms=200.',
           '- crash 위치 head/middle/tail × 값64B/8KiB × 감지 대기500/1000ms × 3회 = 36회. controller는 crash 대상 호스트에서 실행해 그 JVM에 SIGKILL을 보낸다.',
           '- 정상 구간: warm30 + 측정128건 SET을 head에 보낸다. head는 직전 쓰기의 역방향 Ack가 오기 전 새 쓰기에 BUSY를 돌려주므로 client가 같은 요청을 재시도하며, 지연은 재시도를 포함한 값이다. tail GET 128건과 head GET의 redirect를 확인한다.',
           '- failover: SIGKILL → controller가 timeout.ms만큼 **고정 대기**(실제 failure detector가 아님) → 생존자에 FREEZE(epoch2, 생존 사슬) → REPAIR → 생존자 전부 REPAIRING·길이/적용/Ack 일치·로그 SHA-256 digest 일치 확인 → OPEN → 새 head 첫 SET OK. 모든 시각은 같은 controller의 perf_counter다. digest의 JCA 초기화·JIT 비용이 failover에 섞이지 않도록 crash 전에 세 노드 digest를 한 번 계산해 일치를 확인한다(첫 cohort에서는 이 warm-up이 없어 repair에 70~330ms의 콜드 digest 비용이 포함됐고, 그 결과는 별도 원본으로만 보존한다).',
           '- 서버 promotion은 FREEZE 수신(detectedNs)→RUNNING(readyNs)을 같은 JVM에서 뺀 값으로, 생존자 중 최대다. 호스트 간 nanoTime은 차감하지 않는다.','',
           '## 결과 (3회 중앙값; failover는 최소~최대 병기)','',
           '|위치|값 B|대기 ms|SET p50/p99 ms|BUSY 재시도 합|tail GET p50/p99 ms|failover ms|대기/freeze/repair(제어+확인+digest)/open/첫쓰기 ms|promotion ms|peer B/SET|',
           '|---|---:|---:|---:|---:|---:|---:|---|---:|---:|']
    for s in summary:
        lines.append(f"|{s['position']}|{s['valueBytes']}|{s['timeoutMs']}|{s['normalP50MsMedian']:.2f}/{s['normalP99MsMedian']:.2f}|{s['busyRetriesTotal']}|{s['tailReadP50MsMedian']:.2f}/{s['tailReadP99MsMedian']:.2f}|"
                     f"{s['failoverMsMedian']:.1f} ({s['failoverMsMin']:.1f}~{s['failoverMsMax']:.1f})|{s['detectWaitMsMedian']:.0f}/{s['freezeMsMedian']:.1f}/{s['repairMsMedian']:.1f} ({s['repairControlMsMedian']:.1f}+{s['repairPollMsMedian']:.1f}+{s['repairDigestMsMedian']:.1f})/{s['openMsMedian']:.1f}/{s['firstWriteMsMedian']:.1f}|{s['promotionMsMedian']:.1f}|{s['peerBytesPerSetMedian']:.0f}|")
    lines+=['','![chain](figures/chain-failover.png)','',
            f"36회 모두 정상128건 OK·tail GET128건 OK, crash 전 값 복사본 {474*len(rows):,}개(158키×3노드×36)와 crash 후 생존자 복사본 {318*len(rows):,}개가 일치했다. 모든 실행에서 FREEZE 응답 applied=158, 복구 길이158, 생존자 로그 digest 일치, epoch1 쓰기 STALE_EPOCH, 비head 쓰기 redirect를 확인했다. BUSY 재시도 합계 {meta['busyRetriesTotal']}회.",'',
            '## 해석','',
            '- failover의 대부분은 controller의 고정 감지 대기(timeout.ms)다. freeze/repair/open은 controller↔노드 제어 왕복·상태 polling·digest 비교를 포함한 master 절차 비용이며, 이 값은 PB/VR/Raft의 heartbeat 기반 감지·자체 선출과 직접 비교할 수 없다. 같은 timeout.ms 조건에서 "감지 이후" 비용만 비교할 때 참고한다.',
            '- head crash는 새 head(1)의 client 연결 변경, middle crash는 head→tail 직결, tail crash는 새 tail(1)의 확정 책임 이동이 각각 필요하지만, 세 경우 모두 master 절차가 같아 위치에 따른 복구 비용 차이는 작다.',
            '- SET 지연에는 BUSY 재시도가 포함된다. 이 연구 구현의 head는 tail 결과 중계로 client에 응답한 뒤 역방향 Ack가 도착해야 다음 쓰기를 받으므로, 단일 client가 연속 쓰기를 보내면 짧은 BUSY 구간이 생긴다. 파이프라인 쓰기는 구현하지 않았다.',
            '- 단일 client·128표본·조건별 3회이며 최대 처리량·운영 p99·SSD 측정이 아니다. peer byte는 warm+정상158건 구간의 세 노드 송신 합이다.','',
            '[실행별 CSV](chain-results/runs.csv) · [조건별 요약 CSV](chain-results/summary.csv) · [SHA 근거](chain-results/sources.json)','']
    (out/'chain-results.md').write_text('\n'.join(lines),encoding='utf-8');print(json.dumps({k:v for k,v in meta.items() if k!='rawSha256'},indent=2))


if __name__=='__main__':main()
