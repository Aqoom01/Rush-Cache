"""같은 배포본·부하에서 번갈아 측정한 VR INCREMENTAL과 INCREMENTAL_CATCHUP의 전송량·원인·지연을 조건별로 대조한다."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

OPCODES={'48':'PREPARE','49':'DELTA_PREPARE','50':'COMMIT','51':'PREPARE_OK','52':'START_VIEW_CHANGE',
         '53':'DO_VIEW_CHANGE','54':'START_VIEW','55':'GET_STATE','56':'VIEW_CHANGE_ACK'}
REASONS=('GET_STATE_COMMIT_GAP','GET_STATE_DELTA_PREFIX_GAP','GET_STATE_DELTA_COMMIT_GAP','GET_STATE_VIEW_STATE',
         'GET_STATE_RECEIVED','SNAPSHOT_GET_STATE_REQUEST','SNAPSHOT_STALE_VIEW_REQUEST','SNAPSHOT_VIEW_INSTALL_REQUEST',
         'SNAPSHOT_LATE_CHANGE_REQUEST','SNAPSHOT_RETRY_REQUEST','SNAPSHOT_THROTTLED','SNAPSHOT_UPGRADE',
         'TRANSFER_RESEND','GET_STATE_SEND','SNAPSHOT_SEND','DELTA_SEND','COMMIT_SEND',
         'COMMIT_BOUNDED','DELTA_CATCHUP_REQUEST','DELTA_CATCHUP_THROTTLED')
MODES=('INCREMENTAL','INCREMENTAL_CATCHUP')
CONDITIONS=[(64,500),(64,1000),(8192,500),(8192,1000)]


def difference(before,after):
    ops={op:{'frames':0,'bytes':0} for op in OPCODES}; reasons={r:0 for r in REASONS}
    for b,a in zip(before,after):
        assert b['node']==a['node']
        for op in OPCODES:
            for field in ('frames','bytes'):
                delta=a['peerTx']['opcodes'].get(op,{}).get(field,0)-b['peerTx']['opcodes'].get(op,{}).get(field,0)
                assert delta>=0;ops[op][field]+=delta
        for name in REASONS:
            delta=a['vrTraffic'][name]-b['vrTraffic'][name];assert delta>=0;reasons[name]+=delta
    return ops,reasons


def validate(rows,full=True):
    if full:
        assert len(rows)==24
        assert {(r['vrMode'],r['valueBytes'],r['timeoutMs'],r['repeat']) for r in rows}=={(m,v,t,n) for m in MODES for v in (64,8192) for t in (500,1000) for n in (1,2,3)}
    assert len({r['runId'] for r in rows})==len(rows)
    for r in rows:
        assert r['ok'] and r['schema']=='a1-real-vr-traffic-2' and r['faultsInjected'] is False and r['vrMode'] in MODES
        assert r['normalRequests']==128 and r['warmupRequests']==30 and r['verifiedCopies']==474
        samples=sorted(r['normalSamplesNs']);assert len(samples)==128 and min(samples)>0
        assert r['normalP50Ns']==samples[63] and r['normalP99Ns']==samples[126]
        for phase in ('before','immediateAfter','settledAfter'):
            assert [s['node'] for s in r[phase]]==[0,1,2]
            for s in r[phase]:
                assert s['epoch']==0 and s['state']=='NORMAL' and s['protocol']=='VR' and s['vrMode']==r['vrMode']
                tx=s['peerTx'];assert set(tx['opcodes'])<=set(OPCODES)
                assert tx['totalBytes']==sum(x['bytes'] for x in tx['opcodes'].values())
                assert tx['totalFrames']==sum(x['frames'] for x in tx['opcodes'].values())
                assert s['transportFailures']==r['before'][s['node']]['transportFailures']
        assert all(s['logLength']==s['commit']==s['applied']==30 for s in r['before'])
        assert all(s['logLength']==s['commit']==s['applied']==158 for s in r['settledAfter'])
        difference(r['before'],r['immediateAfter']);difference(r['immediateAfter'],r['settledAfter'])
        ops,reasons=difference(r['before'],r['settledAfter'])
        for reason,op in [('SNAPSHOT_SEND','54'),('DELTA_SEND','49'),('GET_STATE_SEND','55')]:
            assert reasons[reason]==ops[op]['frames']
        assert reasons['GET_STATE_SEND']==sum(reasons[x] for x in REASONS[:4])
        assert reasons['GET_STATE_RECEIVED']==reasons['GET_STATE_SEND']
        if r['vrMode']=='INCREMENTAL':
            assert reasons['COMMIT_BOUNDED']==reasons['DELTA_CATCHUP_REQUEST']==reasons['DELTA_CATCHUP_THROTTLED']==0
        else:
            # 같은 view의 정상 Backup 요청은 모두 증분 경로로 처리돼야 한다. 전체 로그 요청 원인은 남지 않는다.
            assert reasons['SNAPSHOT_GET_STATE_REQUEST']==0 and reasons['SNAPSHOT_UPGRADE']==0
            assert reasons['DELTA_CATCHUP_REQUEST']==reasons['GET_STATE_RECEIVED']


def summarize(r):
    ops,reasons=difference(r['before'],r['settledAfter']);immediate,_=difference(r['before'],r['immediateAfter'])
    total=sum(x['bytes'] for x in ops.values());snapshot=ops['54']['bytes']
    return dict(vrMode=r['vrMode'],valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],totalBytes=total,
                immediateBytes=sum(x['bytes'] for x in immediate.values()),tailBytes=total-sum(x['bytes'] for x in immediate.values()),
                deltaBytes=ops['49']['bytes'],snapshotBytes=snapshot,snapshotShare=snapshot/total,bytesPerSet=total/128,
                snapshotFrames=ops['54']['frames'],getStateFrames=ops['55']['frames'],commitFrames=ops['50']['frames'],
                commitGap=reasons['GET_STATE_COMMIT_GAP'],commitBounded=reasons['COMMIT_BOUNDED'],
                catchupRequests=reasons['DELTA_CATCHUP_REQUEST'],catchupThrottled=reasons['DELTA_CATCHUP_THROTTLED'],
                transferResends=reasons['TRANSFER_RESEND'],snapshotThrottled=reasons['SNAPSHOT_THROTTLED'],
                p50Ms=r['normalP50Ns']/1e6,p99Ms=r['normalP99Ns']/1e6,elapsedMs=r['normalElapsedNs']/1e6,verifiedCopies=r['verifiedCopies'])


def main():
    p=argparse.ArgumentParser();p.add_argument('raw',type=Path)
    p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent);args=p.parse_args()
    paths=sorted(args.raw.glob('VR-*-v*-t*-r*.json'));rows=[json.loads(x.read_text(encoding='utf-8')) for x in paths];validate(rows)
    out=args.output;data=out/'catchup-results';data.mkdir(parents=True,exist_ok=True);(out/'figures').mkdir(exist_ok=True)
    runs=[summarize(r) for r in rows];opcode_rows=[];reason_rows=[]
    for r in rows:
        for phase,before,after in [('normal',r['before'],r['immediateAfter']),('tail',r['immediateAfter'],r['settledAfter']),('combined',r['before'],r['settledAfter'])]:
            op_delta,reason_delta=difference(before,after)
            for op,counts in op_delta.items():opcode_rows.append(dict(vrMode=r['vrMode'],valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],phase=phase,opcode=op,name=OPCODES[op],**counts))
            for name,count in reason_delta.items():reason_rows.append(dict(vrMode=r['vrMode'],valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],phase=phase,reason=name,count=count))
    summary=[]
    for size,timeout in CONDITIONS:
        for mode in MODES:
            group=[x for x in runs if (x['vrMode'],x['valueBytes'],x['timeoutMs'])==(mode,size,timeout)]
            summary.append(dict(vrMode=mode,valueBytes=size,timeoutMs=timeout,runs=len(group),
                                bytesPerSetMedian=statistics.median(x['bytesPerSet'] for x in group),
                                bytesPerSetMin=min(x['bytesPerSet'] for x in group),bytesPerSetMax=max(x['bytesPerSet'] for x in group),
                                snapshotFrames=sum(x['snapshotFrames'] for x in group),snapshotBytes=sum(x['snapshotBytes'] for x in group),
                                getStateFrames=sum(x['getStateFrames'] for x in group),commitGap=sum(x['commitGap'] for x in group),
                                commitBounded=sum(x['commitBounded'] for x in group),catchupRequests=sum(x['catchupRequests'] for x in group),
                                transferResends=sum(x['transferResends'] for x in group),
                                p50MsMedian=statistics.median(x['p50Ms'] for x in group),p99MsMedian=statistics.median(x['p99Ms'] for x in group),
                                p99MsMin=min(x['p99Ms'] for x in group),p99MsMax=max(x['p99Ms'] for x in group)))
    for name,records in [('runs',runs),('summary',summary),('opcodes',opcode_rows),('reasons',reason_rows)]:
        with (data/f'{name}.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(records[0]),lineterminator='\n');w.writeheader();w.writerows(records)
    totals={mode:{k:sum(x[k] for x in runs if x['vrMode']==mode) for k in ('totalBytes','snapshotBytes','snapshotFrames','getStateFrames','commitGap','commitBounded','catchupRequests','catchupThrottled','transferResends','snapshotThrottled')} for mode in MODES}
    for mode in MODES:totals[mode]['runsWithSnapshot']=sum(x['snapshotFrames']>0 for x in runs if x['vrMode']==mode)
    meta={'runCount':len(rows),'runsPerMode':{m:sum(r['vrMode']==m for r in rows) for m in MODES},'normalSuccesses':128*len(rows),
          'verifiedCopiesIncludingWarmup':474*len(rows),'totals':totals,
          'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
          'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
    (data/'sources.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    fig,axes=plt.subplots(1,3,figsize=(15,5));width=.38
    labels=['64B\n500ms','64B\n1000ms','8KiB\n500ms','8KiB\n1000ms']
    for mi,(mode,shade) in enumerate(zip(MODES,(0,1))):
        xs=[i+(mi-.5)*width for i in range(4)];bottom=[0]*4
        for op,colors in [('49',('#497eb8','#8fb3dc')),('54',('#d29041','#ebc08f')),('control',('#459a76','#9ccbb5'))]:
            values=[]
            for size,timeout in CONDITIONS:
                group=[r for r in rows if (r['vrMode'],r['valueBytes'],r['timeoutMs'])==(mode,size,timeout)]
                ms=[]
                for r in group:
                    ops,_=difference(r['before'],r['settledAfter'])
                    ms.append((sum(x['bytes'] for k,x in ops.items() if k not in ('49','54')) if op=='control' else ops[op]['bytes'])/128/1024)
                values.append(statistics.median(ms))
            axes[0].bar(xs,values,width,bottom=bottom,color=colors[shade],label=f"{OPCODES.get(op,'Other control')} / {'before' if mi==0 else 'after'}")
            bottom=[a+b for a,b in zip(bottom,values)]
    axes[0].set_ylabel('KiB / SET (3-run median, settled boundary)');axes[0].set_title('Peer frame bytes: before (left) vs after (right)');axes[0].legend(fontsize=7)
    for index,(size,timeout) in enumerate(CONDITIONS):
        for mi,mode in enumerate(MODES):
            group=[x for x in runs if (x['vrMode'],x['valueBytes'],x['timeoutMs'])==(mode,size,timeout)]
            axes[1].scatter([index+(mi-.5)*.3]*len(group),[x['snapshotFrames'] for x in group],color='#d29041' if mi==0 else '#2f7d5b',marker='o' if mi==0 else '^',label=f'START_VIEW frames / {"before" if mi==0 else "after"}' if index==0 else None)
            axes[2].scatter([index+(mi-.5)*.3]*len(group),[x['p99Ms'] for x in group],color='#497eb8' if mi==0 else '#2f7d5b',marker='o' if mi==0 else '^',label=f'normal p99 / {"before" if mi==0 else "after"}' if index==0 else None)
    axes[1].set_ylabel('Frames per run (3 observed points)');axes[1].set_title('Full-log transfers per run');axes[1].legend(fontsize=7)
    axes[2].set_ylabel('ms (128 samples, single client)');axes[2].set_title('Normal SET p99 per run');axes[2].legend(fontsize=7)
    for ax in axes:
        ax.set_xticks(range(4),labels);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2)
    fig.suptitle('VR INCREMENTAL (before) vs INCREMENTAL_CATCHUP (after): same dist, interleaved runs, no injected failure')
    fig.tight_layout();fig.savefig(out/'figures/catchup-compare.png');plt.close(fig)
    b,a=totals['INCREMENTAL'],totals['INCREMENTAL_CATCHUP']
    lines=['# VR 전송 경로 개선 전후 비교 (INCREMENTAL → INCREMENTAL_CATCHUP)','',
           '관련 이슈: S15P31D111-187 후속. 7단계에서 확인한 COMMIT gap→GET_STATE→전체 START_VIEW 경로를 줄이는 `INCREMENTAL_CATCHUP` 모드를 추가하고, 기존 `INCREMENTAL`과 같은 배포본·부하·계측으로 번갈아 측정했다. 기존 모드와 이전 결과는 그대로 보존한다.','',
           '## 바꾼 동작','',
           '- Primary는 Backup별로 이미 보낸 로그 길이(확인된 길이와 확인 대기 중 전송의 상한)를 넘지 않는 COMMIT만 통지한다. 통지 값은 항상 실제 commit 이하이므로 Backup이 미확정 위치를 적용하지 않는다. 나머지 확정은 다음 DeltaPrepare나 확인 이후 heartbeat가 전달한다.',
           '- 같은 view에서 PREPARE_OK를 보낸 적 있는 정상 Backup의 GET_STATE는 전체 로그 대신 확인된 위치 이후의 DeltaPrepare로 처리한다. 그 Backup의 로그는 이 view 로그의 prefix이기 때문이다. view가 다르거나 확인 prefix가 없거나 전체 로그 설치가 진행 중이면 기존 전체 로그 경로를 유지한다.',
           '- 전송 창·재전송 backoff(1→4초)·과반수 확정·view change·로그 선택은 바꾸지 않았다. 유실된 DeltaPrepare의 복구는 재전송 timer를 따르므로 기존 모드의 즉시 전체 로그 전달보다 늦을 수 있다. 이 trade-off는 결정론적 테스트로 확인했고 실서버에서는 유실을 주입하지 않았다.','',
           '## 실험 조건','',
           '- 전용 EC2 세 대, 같은 AZ·m6gd.medium·Ubuntu24.04 arm64·Java21·인메모리 KV·private TCP. 장애·인위적 지연·손실 주입 없음.',
           f'- 모드2 × 값64B/8KiB × 감지500/1000ms × 3회 = {len(rows)}회. 조건마다 INCREMENTAL → INCREMENTAL_CATCHUP 순서로 번갈아 실행했다. HB50ms·tick10ms.',
           '- Primary 호스트의 단일 closed-loop client. warm30건을 세 노드가 적용하고 데이터 전송이 안정된 뒤 측정128건. 서버 상태의 vrMode가 요청 모드와 같은지 매 snapshot에서 확인한다.',
           '- byte는 성공한 로컬 TCP write의 length+header+body이며 before→settledAfter 경계의 합계다. 원격 수신·TCP/IP overhead·OS 재전송은 측정하지 않는다.','',
           '## 조건별 결과 (3회 중앙값, 합계는 3회 합)','',
           '|값 B|감지 ms|모드|peer B/SET 중앙값(최소~최대)|전체 로그 frame|전체 로그 byte|GET_STATE|COMMIT gap|COMMIT 제한 통지|증분 catch-up|정상 p99 ms 중앙값(최소~최대)|',
           '|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summary:
        lines.append(f"|{s['valueBytes']}|{s['timeoutMs']}|{s['vrMode']}|{s['bytesPerSetMedian']:.0f} ({s['bytesPerSetMin']:.0f}~{s['bytesPerSetMax']:.0f})|{s['snapshotFrames']}|{s['snapshotBytes']:,}|{s['getStateFrames']}|{s['commitGap']}|{s['commitBounded']}|{s['catchupRequests']}|{s['p99MsMedian']:.2f} ({s['p99MsMin']:.2f}~{s['p99MsMax']:.2f})|")
    lines+=['','![compare](figures/catchup-compare.png)','',
            f"모든 {len(rows)}회에서 정상128건 OK, warmup 포함 값 복사본 474개가 세 노드에서 일치했다(총 {474*len(rows):,}개). view change·transport 실패·전송 제어 재전송은 없었다(TRANSFER_RESEND before {b['transferResends']} / after {a['transferResends']}).",'',
            f"INCREMENTAL 12회: 전체 로그 START_VIEW {b['snapshotFrames']}frame / {b['snapshotBytes']:,}B, GET_STATE {b['getStateFrames']}, COMMIT gap {b['commitGap']}, 전체 로그가 발생한 실행 {b['runsWithSnapshot']}회. 전체 peer byte {b['totalBytes']:,}B 중 전체 로그 비중 {b['snapshotBytes']/b['totalBytes']:.1%}.",'',
            f"INCREMENTAL_CATCHUP 12회: 전체 로그 START_VIEW {a['snapshotFrames']}frame / {a['snapshotBytes']:,}B, GET_STATE {a['getStateFrames']}, COMMIT gap {a['commitGap']}, 범위 제한 COMMIT 통지 {a['commitBounded']}회, 증분 catch-up 요청 {a['catchupRequests']}회(창 대기 {a['catchupThrottled']}회). 전체 peer byte {a['totalBytes']:,}B.",'',
            f"12회 합계 기준 전체 peer byte는 {b['totalBytes']:,}B → {a['totalBytes']:,}B({(1-a['totalBytes']/b['totalBytes']):.1%} 감소)다. 이는 번갈아 실행한 같은 조건 합계의 비교이며, 전체 로그 발생이 실행마다 0 또는 여러 번으로 갈리므로 조건별 중앙값과 최소~최대를 함께 본다.",'',
            '## 안전성 회귀 검증','',
            '- 결정론적 테스트: INCREMENTAL과 같은 안전성·분단·유실·중복·순서 역전 검사 전체를 INCREMENTAL_CATCHUP에도 적용했다(`CatchupVrEngineTest`). 단일 키 이력의 선형화 검사(`RegisterHistoryTest`)와 8시나리오×2지연×5seed 프로토콜 행렬(`VrCatchupTest`)도 통과했다.',
            '- 7단계와 같은 재현 조건(유실·순서 변경 없이 Backup2 ACK만 40ms 지연)에서 INCREMENTAL은 COMMIT gap과 전체 로그가 발생하고 INCREMENTAL_CATCHUP은 둘 다 0이며 12개 값과 view 0을 보존한다.',
            '- 전달 시점마다 통지 commit ≤ Primary commit, Backup applied ≤ Primary commit을 검사했다. DeltaPrepare 한 번 유실 시 전체 로그 없이 재전송 timer로 복구되고, 다른 view·확인 prefix 없는 요청은 전체 로그를 받는다.',
            '- 실제 TCP: `vr.mode` 설정 로드·거절, 상태 JSON의 vrMode, catch-up 모드에서 두 Backup 복제 확인 후 Primary crash→view change→committed 값 보존을 로컬 3노드에서 확인했다(`VrServerTest`).','',
            '## 해석 범위','',
            '- 단일 client·128표본·조건별 3회이며 최대 처리량·운영 p99·SSD 측정이 아니다. p99 차이는 같은 host의 Python controller와 JVM 경합을 포함하므로 전송량 감소만큼 일관된 지연 개선을 주장하지 않는다.',
            '- COMMIT 범위 제한은 뒤처진 Backup이 확정을 알게 되는 시점을 그 Backup의 ACK 왕복 이후로 늦출 수 있다. 과반수 확정·client 응답 시점은 바뀌지 않는다.',
            '- 유실 환경의 복구 지연(재전송 timer 의존)은 실서버에서 측정하지 않았다. 2MiB frame 한도·checkpoint·프로세스 재시작·membership 변경은 범위 밖이다.',
            '- 이전 traffic 12회(7단계)는 계측 목적의 별도 cohort이며 이번 before 12회와 합산하지 않는다.','',
            '[실행별 CSV](catchup-results/runs.csv) · [조건별 요약 CSV](catchup-results/summary.csv) · [opcode별 CSV](catchup-results/opcodes.csv) · [원인별 CSV](catchup-results/reasons.csv) · [SHA 근거](catchup-results/sources.json)','']
    (out/'catchup-results.md').write_text('\n'.join(lines),encoding='utf-8');print(json.dumps(meta,indent=2,default=str)[:2000])


if __name__=='__main__':main()
