"""VR 정상 전송량을 opcode별 byte와 상태 전달 원인으로 분해한다."""
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
         'TRANSFER_RESEND','GET_STATE_SEND','SNAPSHOT_SEND','DELTA_SEND','COMMIT_SEND')


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
        assert len(rows)==12
        assert {(r['valueBytes'],r['timeoutMs'],r['repeat']) for r in rows}=={(v,t,n) for v in (64,8192) for t in (500,1000) for n in (1,2,3)}
    assert len({r['runId'] for r in rows})==len(rows)
    for r in rows:
        assert r['ok'] and r['schema']=='a1-real-vr-traffic-1' and r['faultsInjected'] is False
        assert r['normalRequests']==128 and r['warmupRequests']==30 and r['verifiedCopies']==474
        samples=sorted(r['normalSamplesNs']);assert len(samples)==128 and min(samples)>0
        assert r['normalP50Ns']==samples[63] and r['normalP99Ns']==samples[126]
        for phase in ('before','immediateAfter','settledAfter'):
            assert [s['node'] for s in r[phase]]==[0,1,2]
            for s in r[phase]:
                assert s['epoch']==0 and s['state']=='NORMAL' and s['protocol']=='VR'
                tx=s['peerTx'];assert set(tx['opcodes'])<=set(OPCODES)
                assert tx['totalBytes']==sum(x['bytes'] for x in tx['opcodes'].values())
                assert tx['totalFrames']==sum(x['frames'] for x in tx['opcodes'].values())
                assert s['transportFailures']==r['before'][s['node']]['transportFailures']
        assert all(s['logLength']==s['commit']==s['applied']==30 for s in r['before'])
        assert all(s['logLength']==s['commit']==s['applied']==158 for s in r['settledAfter'])
        difference(r['before'],r['immediateAfter']);difference(r['immediateAfter'],r['settledAfter'])
        ops,reasons=difference(r['before'],r['settledAfter'])
        # Quiescent data/state-request counters: core attempted sends match completed wire writes.
        for reason,op in [('SNAPSHOT_SEND','54'),('DELTA_SEND','49'),('GET_STATE_SEND','55')]:
            assert reasons[reason]==ops[op]['frames']
        assert reasons['GET_STATE_SEND']==sum(reasons[x] for x in REASONS[:4])
        assert reasons['GET_STATE_RECEIVED']==reasons['GET_STATE_SEND']


def main():
    p=argparse.ArgumentParser();p.add_argument('raw',type=Path)
    p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent);args=p.parse_args()
    paths=sorted(args.raw.glob('VR-v*-t*-r*.json'));rows=[json.loads(x.read_text(encoding='utf-8')) for x in paths];validate(rows)
    out=args.output;data=out/'traffic-results';data.mkdir(parents=True,exist_ok=True);(out/'figures').mkdir(exist_ok=True)
    runs=[];opcode_rows=[];reason_rows=[]
    for r in rows:
        ops,reasons=difference(r['before'],r['settledAfter']);immediate,_=difference(r['before'],r['immediateAfter'])
        total=sum(x['bytes'] for x in ops.values());snapshot=ops['54']['bytes']
        runs.append(dict(valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],totalBytes=total,
                         immediateBytes=sum(x['bytes'] for x in immediate.values()),snapshotBytes=snapshot,
                         snapshotShare=snapshot/total,bytesPerSet=total/128,snapshotFrames=ops['54']['frames'],
                         getStateFrames=ops['55']['frames'],commitGap=reasons['GET_STATE_COMMIT_GAP'],
                         transferResends=reasons['TRANSFER_RESEND'],snapshotThrottled=reasons['SNAPSHOT_THROTTLED'],
                         p99Ms=r['normalP99Ns']/1e6,verifiedCopies=r['verifiedCopies']))
        for phase,before,after in [('normal',r['before'],r['immediateAfter']),('tail',r['immediateAfter'],r['settledAfter']),('combined',r['before'],r['settledAfter'])]:
            op_delta,reason_delta=difference(before,after)
            for op,counts in op_delta.items():opcode_rows.append(dict(valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],phase=phase,opcode=op,name=OPCODES[op],**counts))
            for name,count in reason_delta.items():reason_rows.append(dict(valueBytes=r['valueBytes'],timeoutMs=r['timeoutMs'],repeat=r['repeat'],phase=phase,reason=name,count=count))
    for name,records in [('runs',runs),('opcodes',opcode_rows),('reasons',reason_rows)]:
        with (data/f'{name}.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(records[0]),lineterminator='\n');w.writeheader();w.writerows(records)
    meta={'runCount':12,'normalSuccesses':1536,'verifiedCopiesIncludingWarmup':5688,
          'snapshotFrames':sum(r['snapshotFrames'] for r in runs),'getStateFrames':sum(r['getStateFrames'] for r in runs),
          'commitGapRequests':sum(r['commitGap'] for r in runs),'snapshotBytes':sum(r['snapshotBytes'] for r in runs),
          'totalBytes':sum(r['totalBytes'] for r in runs),'transferResends':sum(r['transferResends'] for r in runs),
          'runsWithSnapshot':sum(r['snapshotFrames']>0 for r in runs),'snapshotThrottled':sum(r['snapshotThrottled'] for r in runs),
          'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
          'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
    (data/'sources.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    conditions=[(64,500),(64,1000),(8192,500),(8192,1000)];summary=[]
    fig,axes=plt.subplots(1,2,figsize=(11,5));bottom=[0]*4
    for op,color in [('49','#497eb8'),('54','#d29041'),('control','#459a76')]:
        values=[]
        for size,timeout in conditions:
            group=[r for r in rows if (r['valueBytes'],r['timeoutMs'])==(size,timeout)]
            measurements=[]
            for r in group:
                ops,_=difference(r['before'],r['settledAfter'])
                measurements.append((sum(x['bytes'] for k,x in ops.items() if k not in ('49','54')) if op=='control' else ops[op]['bytes'])/128/1024)
            values.append(statistics.mean(measurements))
        axes[0].bar(range(4),values,bottom=bottom,color=color,label=OPCODES.get(op,'Other control'))
        bottom=[a+b for a,b in zip(bottom,values)]
    axes[0].set_ylabel('KiB / SET (3-run mean, settled boundary)');axes[0].set_title('Completed peer frame bytes by opcode');axes[0].legend()
    for index,(size,timeout) in enumerate(conditions):
        group=[r for r in runs if (r['valueBytes'],r['timeoutMs'])==(size,timeout)]
        gs=[r['getStateFrames'] for r in group];snap=[r['snapshotFrames'] for r in group]
        axes[1].scatter([index-.07]*3,gs,color='#497eb8',label='GET_STATE' if index==0 else None)
        axes[1].scatter([index+.07]*3,snap,color='#d29041',label='START_VIEW' if index==0 else None)
        summary.append(dict(valueBytes=size,timeoutMs=timeout,bytesPerSetMean=statistics.mean(r['bytesPerSet'] for r in group),
                            snapshotShareMin=min(r['snapshotShare'] for r in group),snapshotShareMax=max(r['snapshotShare'] for r in group),
                            snapshotFrames=sum(snap),getStateFrames=sum(gs),commitGap=sum(r['commitGap'] for r in group)))
    axes[1].set_ylabel('Frames per run (3 observed points)');axes[1].set_title('State requests and full-log transfers');axes[1].legend()
    for ax in axes:
        ax.set_xticks(range(4),['64B\n500ms','64B\n1000ms','8KiB\n500ms','8KiB\n1000ms']);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2)
    fig.suptitle('VR normal traffic: no injected failure, warmup30 / normal128')
    fig.tight_layout();fig.savefig(out/'figures/traffic-breakdown.png');plt.close(fig)
    lines=['# VR 정상 전송량 증가 원인 분석','', '관련 이슈: S15P31D111-187. 알고리즘을 바꾸지 않고 원인 카운터와 opcode별 송신 완료량을 추가했다.','',
           '## 실험 조건','',
           '- 전용 EC2 세 대, 같은 AZ·m6gd.medium·Ubuntu24.04 arm64·Java21·인메모리 KV·private TCP.',
           '- VR INCREMENTAL, 값64B/8KiB × 감지500/1000ms ×3회 =12회. HB50ms·tick10ms. 장애·인위적 지연·손실 주입 없음.',
           '- Primary 호스트의 단일 closed-loop client. warm30건을 세 노드가 적용하고 데이터 전송량이 안정된 뒤 측정128건 시작.',
           '- before / 즉시 immediateAfter / 모든 값 검증 및 데이터 전송량 안정 후 settledAfter를 각각 기록한다. 정상 구간과 잔여 전송(tail)을 CSV에서 구분한다.',
           '- byte는 성공한 로컬 TCP write의 length+header+body다. 원격 수신·TCP/IP overhead가 아니다. 안정화 중 heartbeat 비용도 combined에 포함한다.','',
           '## 관측 결과','',
           '|값 B|감지 ms|peer B/SET 평균|START_VIEW byte 비중 최소~최대|전체 로그 frame 합계|GET_STATE 합계|COMMIT gap 합계|',
           '|---|---:|---:|---:|---:|---:|---:|']
    for s in summary:lines.append(f"|{s['valueBytes']}|{s['timeoutMs']}|{s['bytesPerSetMean']:.0f}|{s['snapshotShareMin']:.1%}~{s['snapshotShareMax']:.1%}|{s['snapshotFrames']}|{s['getStateFrames']}|{s['commitGap']}|")
    lines+=['','![breakdown](figures/traffic-breakdown.png)','',
            f"정상1536건 모두 OK. warmup 포함 총5688개 값 복사본이 일치했다. 측정+tail에서 GET_STATE {meta['getStateFrames']}개, COMMIT gap 요청 {meta['commitGapRequests']}개, START_VIEW {meta['snapshotFrames']}개, 전송 제어 재전송 {meta['transferResends']}회를 기록했다.",'',
            f"전체 로그 전송은12회 중 {meta['runsWithSnapshot']}회에서 발생했고, 나머지 {12-meta['runsWithSnapshot']}회에서는0이었다. 전체 로그 byte 합계 {meta['snapshotBytes']:,} B는 전체 peer frame {meta['totalBytes']:,} B의 {meta['snapshotBytes']/meta['totalBytes']:.1%}다. 중복 전체 로그 요청 {meta['snapshotThrottled']}회는 억제됐다. 이는12회 합계의 비중으로, 모든 실행/값 크기에 같은 비중이 발생한다는 뜻은 아니다.",'',
            '## 원인 검증','',
            '1. 정상 경로의 뒤처진 Backup은 자신의 로그 끝보다 앞선 COMMIT을 받으면 GET_STATE를 보낸다. Primary는 전체 로그를 담은 START_VIEW로 응답한다.',
            '2. Primary는 Backup별 확인을 기다리는 전송 창이 하나다. 다른 Backup과 과반수를 충족해 다음 쓰기가 성공해도, ACK가 늦은 Backup에는 다음 DELTA가 아직 전달되지 않을 수 있다. 이때 COMMIT이 해당 로그보다 앞설 수 있다.',
            '3. 결정론적 테스트에서 메시지를 버리거나 순서를 바꾸지 않고 Backup2 ACK만40ms 늦춰 같은 COMMIT gap→GET_STATE→START_VIEW 경로를 재현했다. view change 없이 모든 값은 최종 보존됐다. 순차·빠른 ACK 대조군은 전체 로그 전달이0이었다.',
            '4. 실제 서버에서도 원인 카운터와 opcode frame/byte를 대조한다. GET_STATE 이유 합계 및 송신 성공 횟수가 일치하는지 검증했다. 요청 이유 카운터는 요청 횟수이며, 중복 요청이 throttle될 수 있어 전체 로그 송신 횟수와 같다고 가정하지 않는다.','',
            '## 해석 범위와 후속','',
            '- 이번 결과는 상태 전달 fallback 경로와 비용을 확인한 것이다. ACK40ms는 재현용 조건이며 실제 서버의 ACK 지연이40ms였다는 뜻은 아니다.',
            '- core 카운터는 항목별 원자적 관측, wire opcode/합계는 노드별 lock 안에서 일관된 snapshot이다. 노드 간 동시 snapshot은 아니므로 안정된 데이터/요청 카운터로 대조하고 heartbeat는 원인별 정확한 일대일 매칭을 주장하지 않는다.',
            '- 카운터와 warmup 안정화 절차를 추가했으므로 이전 단계의 지연·전송량 수치와 직접 전후 개선 비교하지 않는다. 최적화·보장수준 변경은 하지 않았다.',
            '- TRANSFER_RESEND는 프로토콜의 전송 제어 재시도다. OS의 TCP 패킷 재전송 횟수는 측정하지 않았다.',
            '- 후속 후보는 Backup별 로그 확인 범위에 맞춘 COMMIT 통지 또는 증분 catch-up이다. 각각 안전성·복구 지연을 검증한 뒤 같은 조건에서 전후 비교해야 한다.',
            '- 큰 로그의2MiB frame 제한·checkpoint 부재·최대 처리량·SSD·프로세스 재시작은 이번 범위 밖이다.','',
            '[실행별 CSV](traffic-results/runs.csv) · [opcode별 정상/tail CSV](traffic-results/opcodes.csv) · [원인별 CSV](traffic-results/reasons.csv) · [SHA 근거](traffic-results/sources.json)','']
    (out/'traffic-results.md').write_text('\n'.join(lines),encoding='utf-8');print(json.dumps(meta,indent=2))


if __name__=='__main__':main()
