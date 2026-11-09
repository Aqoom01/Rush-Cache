"""Chain TAIL_ONLY vs CRAQ 읽기 경로 12회 원본 검증·CSV·그래프."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

MODES=('TAIL_ONLY','CRAQ')
OPCODES={'80':'FORWARD','81':'PROGRESS','82':'ACK','83':'COMPLETED','84':'VERSION_QUERY','85':'VERSION_REPLY'}


def validate(rows,full=True):
    if full:
        assert len(rows)==12 and {(r['readMode'],r['valueBytes'],r['repeat']) for r in rows}=={(m,v,n) for m in MODES for v in (64,8192) for n in (1,2,3)}
    assert len({r['runId'] for r in rows})==len(rows)
    for r in rows:
        assert r['schema']=='a1-real-craq-1' and r['ok'] and r['protocol']=='CHAIN' and r['readMode'] in MODES and r['faultsInjected'] is False
        for node in '012':
            c=r['clean'][node];s=sorted(c['samplesNs']);assert len(s)==128 and min(s)>0 and c['p50Ns']==s[63] and c['p99Ns']==s[126]
            if r['readMode']=='TAIL_ONLY':
                assert c['redirects']==(0 if node=='2' else 128)
            else:
                assert c['redirects']==0
        m=r['mixed'];assert len(m['writeSamplesNs'])==128 and m['monotonicViolations']==0 and 127<=m['maxSeq']<=128 and m['readCount']==len(m['readSequence'])>0
        assert all(-1<=s<=128 for s in m['readSequence'])
        assert m['monotonicViolations']==sum(b<a for a,b in zip(m['readSequence'],m['readSequence'][1:]))
        assert m['maxSeq']==max(m['readSequence']) and m['readCount']==len(m['readSamplesNs'])
        writes=sorted(m['writeSamplesNs']);reads=sorted(m['readSamplesNs'])
        assert m['writeP50Ns']==writes[63] and m['writeP99Ns']==writes[126]
        assert min(reads)>0 and m['readP50Ns']==reads[math.ceil(len(reads)*.5)-1] and m['readP99Ns']==reads[math.ceil(len(reads)*.99)-1]
        clean=r['cleanCounterDelta'];mixed=r['mixedCounterDelta']
        if r['readMode']=='TAIL_ONLY':
            assert all(d['localReads']==d['queryAttempts']==d['dirtyReads']==0 for d in clean+mixed)
            assert m['readRedirects']==m['readCount']
        else:
            assert sum(d['localReads'] for d in clean)==384 and all(d['queryAttempts']==d['dirtyReads']==0 for d in clean)
            assert m['readRedirects']==0 and mixed[1]['localReads']+mixed[1]['dirtyReads']==m['readCount'] and mixed[1]['queryAttempts']>=mixed[1]['dirtyReads']
            assert all(s['pendingReads']==0 for s in r['statesAfter'])
        assert r['verifiedCopies']==90 and all(s['acknowledged']==158 for s in r['statesAfter'])


def peer_delta(r):
    out={op:0 for op in OPCODES}
    for b,a in zip(r['peerBefore'],r['peerAfter']):
        for op in OPCODES:
            out[op]+=a['opcodes'].get(op,{}).get('bytes',0)-b['opcodes'].get(op,{}).get('bytes',0)
    return out


def summarize(r):
    m=r['mixed'];peer=peer_delta(r);d=r['mixedCounterDelta'][1]
    row=dict(readMode=r['readMode'],valueBytes=r['valueBytes'],repeat=r['repeat'])
    for node in '012':
        row[f'cleanP50Ms{node}']=r['clean'][node]['p50Ns']/1e6;row[f'cleanP99Ms{node}']=r['clean'][node]['p99Ns']/1e6;row[f'cleanRedirects{node}']=r['clean'][node]['redirects']
    row.update(mixedWriteP50Ms=m['writeP50Ns']/1e6,mixedWriteP99Ms=m['writeP99Ns']/1e6,mixedWriteBusy=m['writeBusyRetries'],mixedElapsedMs=m['elapsedNs']/1e6,
               mixedReads=m['readCount'],mixedReadP50Ms=m['readP50Ns']/1e6,mixedReadP99Ms=m['readP99Ns']/1e6,mixedReadRedirects=m['readRedirects'],
               mixedLocalReads=d['localReads'],mixedDirtyReads=d['dirtyReads'],mixedQueryAttempts=d['queryAttempts'],
               versionQueryBytes=peer['84']+peer['85'],dataBytes=peer['80']+peer['83'],monotonicViolations=m['monotonicViolations'],verifiedCopies=r['verifiedCopies'])
    return row


def main():
    p=argparse.ArgumentParser();p.add_argument('raw',type=Path);p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent);args=p.parse_args()
    paths=sorted(args.raw.glob('CRAQ-*-v*-r*.json'));rows=[json.loads(x.read_text(encoding='utf-8')) for x in paths];validate(rows)
    out=args.output;data=out/'craq-results';data.mkdir(parents=True,exist_ok=True);(out/'figures').mkdir(exist_ok=True)
    runs=[summarize(r) for r in rows];summary=[]
    for size in (64,8192):
        for mode in MODES:
            g=[x for x in runs if (x['readMode'],x['valueBytes'])==(mode,size)];med=lambda k:statistics.median(x[k] for x in g)
            summary.append(dict(readMode=mode,valueBytes=size,runs=len(g),**{f'cleanP50Ms{n}Median':med(f'cleanP50Ms{n}') for n in '012'},
                                **{f'cleanP99Ms{n}Median':med(f'cleanP99Ms{n}') for n in '012'},cleanRedirects=sum(x[f'cleanRedirects{n}'] for x in g for n in '012'),
                                mixedWriteP99MsMedian=med('mixedWriteP99Ms'),mixedReadsTotal=sum(x['mixedReads'] for x in g),mixedReadP50MsMedian=med('mixedReadP50Ms'),
                                mixedReadP99MsMedian=med('mixedReadP99Ms'),mixedDirtyReads=sum(x['mixedDirtyReads'] for x in g),mixedLocalReads=sum(x['mixedLocalReads'] for x in g),
                                mixedQueryAttempts=sum(x['mixedQueryAttempts'] for x in g),mixedReadRedirects=sum(x['mixedReadRedirects'] for x in g),
                                versionQueryBytesMedian=med('versionQueryBytes'),dataBytesMedian=med('dataBytes')))
    for name,records in [('runs',runs),('summary',summary)]:
        with (data/f'{name}.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(records[0]),lineterminator='\n');w.writeheader();w.writerows(records)
    meta={'runCount':len(rows),'cleanReads':384*len(rows),'mixedWrites':128*len(rows),'mixedReads':sum(x['mixedReads'] for x in runs),'verifiedCopies':90*len(rows),
          'monotonicViolations':sum(x['monotonicViolations'] for x in runs),
          'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
          'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
    (data/'sources.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    fig,axes=plt.subplots(1,3,figsize=(16,5));colors={'TAIL_ONLY':'#d29041','CRAQ':'#2f7d5b'}
    labels=['64B\nhead','64B\nmiddle','64B\ntail','8KiB\nhead','8KiB\nmiddle','8KiB\ntail']
    for mi,mode in enumerate(MODES):
        xs=[];ys=[]
        for si,size in enumerate((64,8192)):
            for ni,node in enumerate('012'):
                g=[x for x in runs if (x['readMode'],x['valueBytes'])==(mode,size)]
                xs+=[si*3+ni+(mi-.5)*.3]*len(g);ys+=[x[f'cleanP99Ms{node}'] for x in g]
        axes[0].scatter(xs,ys,color=colors[mode],label=mode,marker='o' if mi==0 else '^')
    axes[0].set_xticks(range(6),labels);axes[0].set_title('Clean GET p99 by requesting node (TAIL_ONLY follows redirect)');axes[0].set_ylabel('ms')
    for mi,mode in enumerate(MODES):
        for si,size in enumerate((64,8192)):
            g=[x for x in runs if (x['readMode'],x['valueBytes'])==(mode,size)]
            axes[1].scatter([si+(mi-.5)*.3]*len(g),[x['mixedReadP99Ms'] for x in g],color=colors[mode],marker='o' if mi==0 else '^',label=f'{mode} read p99' if si==0 else None)
            axes[1].scatter([si+(mi-.5)*.3]*len(g),[x['mixedWriteP99Ms'] for x in g],color=colors[mode],marker='x',label=f'{mode} write p99' if si==0 else None)
            axes[2].scatter([si+(mi-.5)*.3]*len(g),[x['mixedDirtyReads'] for x in g],color=colors[mode],marker='o' if mi==0 else '^',label=f'{mode} dirty reads' if si==0 else None)
            axes[2].scatter([si+(mi-.5)*.3]*len(g),[x['mixedReads'] for x in g],color=colors[mode],marker='+',label=f'{mode} reads' if si==0 else None)
    axes[1].set_xticks(range(2),['64B','8KiB']);axes[1].set_title('Mixed phase: middle-node GET p99 and head SET p99');axes[1].set_ylabel('ms')
    axes[2].set_xticks(range(2),['64B','8KiB']);axes[2].set_title('Mixed phase: reads completed and dirty (tail-version) reads');axes[2].set_ylabel('count per run')
    for ax in axes:
        ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2);ax.legend(fontsize=7)
    fig.suptitle('Chain TAIL_ONLY vs CRAQ on 3 EC2 nodes: same dist, modes interleaved, no injected failure');fig.tight_layout();fig.savefig(out/'figures/craq-reads.png');plt.close(fig)
    direct_tail=[s['cleanP50Ms2Median'] for s in summary if s['readMode']=='TAIL_ONLY']
    craq_local=[s[f'cleanP50Ms{n}Median'] for s in summary if s['readMode']=='CRAQ' for n in (0,1)]
    lines=['# Chain TAIL_ONLY vs CRAQ 실서버 읽기 경로 비교','',
           '관련 이슈: S15P31D111-181 후속(10단계). `feat/craq-reads`의 CRAQ 구현(`ReadMode.CRAQ`, key별 다중 버전, tail 버전 조회)을 실서버 worktree에 통합하고 같은 배포본·부하로 두 읽기 모드를 조건마다 번갈아 측정했다. 가상 실험의 0ns 로컬 읽기를 실서버 성능으로 해석하지 않기 위한 실측이다.','',
           '## 실험 조건','',
           '- 전용 EC2 세 대, 같은 AZ·m6gd.medium·Ubuntu24.04 arm64·Java21·인메모리 KV·private TCP. 사슬 0(head)→1→2(tail), retry.ms=200, 장애 없음. client는 head 호스트.',
           '- 모드2 × 값64B/8KiB × 3회 = 12회. warm30 SET 후 세 노드 Ack 안정화.',
           '- clean 읽기: 노드0/1/2 각각에 128건 GET. TAIL_ONLY에서 head/middle GET은 NOT_PRIMARY redirect를 받고 client가 tail에 다시 요청하므로 지연은 두 요청 합이다. CRAQ는 clean이면 로컬에서 응답한다.',
           '- 혼합 구간: head에 hot key SET 128건(값 앞 4byte가 순번, BUSY 재시도 포함)을 보내는 동안 별도 reader가 middle 노드에 같은 key GET을 연속 요청한다. CRAQ에서 middle의 dirty 읽기는 tail에 버전을 조회해 확정 버전을 로컬에서 돌려준다. 읽기 순번이 뒤로 가지 않는지(단조) 검사한다.',
           '- 서버 카운터(localReads/queryAttempts/dirtyReads)와 peer opcode byte(84/85 버전 조회, 80/83 데이터·결과 중계)를 구간별로 뺀다.','',
           '## 결과 (3회 중앙값, 횟수는 3회 합)','',
           '|값 B|모드|clean GET p50/p99 ms head|middle|tail|clean redirect|혼합 SET p99 ms|혼합 GET 수|혼합 GET p50/p99 ms|로컬/dirty/조회시도|redirect|버전조회 B 중앙값|',
           '|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summary:
        lines.append(f"|{s['valueBytes']}|{s['readMode']}|{s['cleanP50Ms0Median']:.2f}/{s['cleanP99Ms0Median']:.2f}|{s['cleanP50Ms1Median']:.2f}/{s['cleanP99Ms1Median']:.2f}|{s['cleanP50Ms2Median']:.2f}/{s['cleanP99Ms2Median']:.2f}|{s['cleanRedirects']}|"
                     f"{s['mixedWriteP99MsMedian']:.2f}|{s['mixedReadsTotal']}|{s['mixedReadP50MsMedian']:.2f}/{s['mixedReadP99MsMedian']:.2f}|{s['mixedLocalReads']}/{s['mixedDirtyReads']}/{s['mixedQueryAttempts']}|{s['mixedReadRedirects']}|{s['versionQueryBytesMedian']:.0f}|")
    lines+=['','![craq](figures/craq-reads.png)','',
            f"12회 모두 clean GET {384*len(rows):,}건·혼합 SET {128*len(rows):,}건 성공, 혼합 GET {meta['mixedReads']:,}건에서 순번 역행 0, 값 복사본 {90*len(rows)}개 일치. TAIL_ONLY의 head/middle GET은 전부 redirect(각 128건), CRAQ는 redirect 0·clean 로컬 384건/실행이다.",'',
            '## 해석','',
            '- CRAQ의 clean 읽기는 요청 노드에서 바로 응답하므로 head/middle GET이 TAIL_ONLY의 redirect+재요청(두 왕복)보다 짧다. tail GET은 두 모드가 같은 경로다.',
            f'- 직접 tail에 보낸 TAIL_ONLY의 clean p50 조건별 중앙값 범위는{min(direct_tail):.2f}~{max(direct_tail):.2f}ms, CRAQ head/middle은{min(craq_local):.2f}~{max(craq_local):.2f}ms다. CRAQ의 일반적인 지연 우위라고 해석하지 않는다. redirect 경로 제거와 실제 읽기 분산·처리량 효과를 구분해야 한다.',
            '- 혼합 구간에서 CRAQ의 head SET p99가 TAIL_ONLY보다 높다. 버전 조회와 같은 호스트 reader 경합이 함께 포함된 관측이며, 각 원인의 비용을 분리해 계측한 결과는 아니다.',
            '- 혼합 구간의 dirty 읽기는 tail 버전 조회 왕복이 추가되지만 값은 로컬에서 돌려준다. dirty 비율은 reader가 쓰기 진행 중 middle에 도착한 시점에 따라 달라지며 고정 비율이 아니다. TAIL_ONLY는 모든 읽기가 tail로 가므로 dirty 구분 자체가 없다.',
            '- 단일 reader·단일 writer이며 읽기 처리량 상한·다중 reader 분산 효과·GC 대기·장애 중 읽기 가용성은 측정하지 않았다. 가상 실험의 288조건 혼합 부하(`craq-mixed`)는 원래 브랜치에 있으며 이번 실측과 합산하지 않는다.',
            '- 순번 역행0은 단조 읽기 검사다. 호출/완료 시각의 전체 history를 기록하지 않아 선형화 가능성 전체 검증은 아니다. 복사본1080개 검사는 warm key30개×3노드×12회이며, 혼합 hot key의 마지막128은 tail에서만 확인했다. head/middle의 최종 hot 값과 전체 실시간 일관성은 후속 검증 대상이다.',
            '- CRAQ는 빈 KV에서 모든 쓰기가 엔진을 경유해야 하며 COUNT는 tail 전용이다. master 합의·신규 노드 추가는 범위 밖이다.','',
            '[실행별 CSV](craq-results/runs.csv) · [조건별 요약 CSV](craq-results/summary.csv) · [SHA 근거](craq-results/sources.json)','']
    (out/'craq-results.md').write_text('\n'.join(lines),encoding='utf-8');print(json.dumps({k:v for k,v in meta.items() if k!='rawSha256'},indent=2))


if __name__=='__main__':main()
