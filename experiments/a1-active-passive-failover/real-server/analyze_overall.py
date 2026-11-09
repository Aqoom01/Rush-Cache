"""실서버 cohort 요약 CSV를 모아 PB/VR/Raft/Chain/Rosé를 같은 축으로 정리한다. 새 측정은 하지 않으며 각 cohort의 조건 차이를 표에 명시한다."""
import argparse
import csv
import json
from pathlib import Path

HERE=Path(__file__).resolve().parent


def rows(name):
    with (HERE/name).open(encoding='utf-8') as f:
        return [{k:(float(v) if v.replace('.','',1).replace('-','',1).isdigit() else v) for k,v in r.items()} for r in csv.DictReader(f)]


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=HERE);args=p.parse_args()
    election=rows('election-results/summary.csv');chain=rows('chain-results/summary.csv');rose=rows('rose-results/summary.csv')
    catchup=rows('catchup-results/summary.csv');craq=rows('craq-results/summary.csv')
    direct_tail=[r['cleanP50Ms2Median'] for r in craq if r['readMode']=='TAIL_ONLY']
    craq_local=[r[f'cleanP50Ms{n}Median'] for r in craq if r['readMode']=='CRAQ' for n in (0,1)]
    sources={name:json.loads((HERE/name/'sources.json').read_text(encoding='utf-8')).get('distributionSha256') for name in ('election-results','chain-results','rose-results','catchup-results','craq-results')}
    table=[]
    for size in (64,8192):
        for protocol in ('PB','VR','RAFT'):
            e=next(r for r in election if r['protocol']==protocol and r['valueBytes']==size and r['timeoutMs']==500)
            table.append(dict(method={'PB':'PB (crash-only 고정 승계)','VR':'VR INCREMENTAL','RAFT':'Raft'}[protocol],valueBytes=size,
                              normal=f"{e['p50MedianMs']:.2f}/{e['p99MedianMs']:.2f}",peer=f"{e['peerBytesPerWriteMedian']:.0f}",
                              failover=f"{e['failoverMedianMs']:.0f} ({e['failoverMinMs']:.0f}~{e['failoverMaxMs']:.0f})",after=f"{e['transitionMedianMs']:.1f}",
                              detect={'PB':'heartbeat timeout 500ms','VR':'heartbeat timeout 500ms','RAFT':'선거 timeout [500,1000)ms 난수'}[protocol],
                              crash='settled idle 리더 SIGKILL',cohort='election 36회'))
        c=next(r for r in catchup if r['vrMode']=='INCREMENTAL_CATCHUP' and r['valueBytes']==size and r['timeoutMs']==500)
        table.append(dict(method='VR INCREMENTAL_CATCHUP',valueBytes=size,normal=f"{c['p50MsMedian']:.2f}/{c['p99MsMedian']:.2f}",peer=f"{c['bytesPerSetMedian']:.0f}",
                          failover='미측정',after='미측정',detect='heartbeat timeout 500ms(동일 엔진)',crash='장애 미주입',cohort='catchup 24회(전송 비교)'))
        for position in ('head','middle','tail'):
            h=next(r for r in chain if r['position']==position and r['valueBytes']==size and r['timeoutMs']==500)
            table.append(dict(method=f'Chain TAIL_ONLY ({position} crash)',valueBytes=size,normal=f"{h['normalP50MsMedian']:.2f}/{h['normalP99MsMedian']:.2f} (BUSY 재시도 포함)",peer=f"{h['peerBytesPerSetMedian']:.0f}",
                              failover=f"{h['failoverMsMedian']:.0f} ({h['failoverMsMin']:.0f}~{h['failoverMsMax']:.0f})",after=f"{h['promotionMsMedian']:.1f}",
                              detect='controller 고정 대기 500ms(감지기 없음)',crash='settled idle, 외부 master FREEZE/REPAIR/OPEN',cohort='chain 36회'))
        for limit in (0,4,-1):
            r=next(x for x in rose if x['limit']==limit and x['valueBytes']==size)
            bound='∞' if limit<0 else str(limit)
            table.append(dict(method=f'Rosé 변형 L={bound}',valueBytes=size,normal=f"{r['normalP50MsMedian']:.2f}/{r['normalP99MsMedian']:.2f} (client 8개 동시)",peer='미집계',
                              failover=f"{r['failoverMsMedian']:.0f} ({r['failoverMsMin']:.0f}~{r['failoverMsMax']:.0f})",after=f"{r['promotionMsMedian']:.1f}",
                              detect='controller 고정 대기 500ms(감지기 없음)',crash=f"쓰기 진행 중 Primary SIGKILL, OK 유실 최대 {int(r['lostOkWritesMax'])}건(상한 {bound})",cohort='rose 30회'))
    lines=['# A1 실서버 종합 비교 (PB·VR·Raft·Chain·CRAQ·Rosé 변형)','',
           '12단계. 새 측정 없이 각 cohort의 요약 CSV를 같은 축으로 모았다. **cohort마다 부하·장애 모델·감지 모델이 다르므로 한 표의 숫자를 순위로 읽지 않는다.** 각 행의 조건 열을 함께 본다. 모든 cohort는 전용 EC2 m6gd.medium 3대(같은 AZ)·인메모리 KV·private TCP·heartbeat50ms·tick10ms이며 아래 값은 FD/대기 500ms 조건의 3회 중앙값이다.','',
           '## 같은 축으로 모은 표 (값64B / 8KiB, 500ms 조건)','',
           '|방식|값 B|정상 SET p50/p99 ms|peer B/SET|failover ms (최소~최대)|감지 이후 전환 ms|감지 모델|장애·보장 조건|cohort|',
           '|---|---:|---|---:|---|---:|---|---|---|']
    for t in table:
        lines.append(f"|{t['method']}|{t['valueBytes']}|{t['normal']}|{t['peer']}|{t['failover']}|{t['after']}|{t['detect']}|{t['crash']}|{t['cohort']}|")
    cr=[(r['valueBytes'],r['readMode'],r['cleanP99Ms0Median'],r['cleanP99Ms1Median'],r['cleanP99Ms2Median'],r['mixedReadP99MsMedian'],r['mixedDirtyReads']) for r in craq]
    lines+=['','## CRAQ 읽기 경로 (별도 cohort, 장애 없음)','','|값 B|모드|clean GET p99 ms head/middle/tail|혼합 GET p99 ms|dirty 읽기 합|','|---:|---|---|---:|---:|']
    for v,m,a,b,c,d,e in cr:lines.append(f"|{int(v)}|{m}|{a:.2f}/{b:.2f}/{c:.2f}|{d:.2f}|{int(e)}|")
    lines+=['','## 보장·전제 비교','',
            '|방식|쓰기 응답 조건|리더/구성 crash 시 성공 쓰기|감지·승계 주체|이번 실측에서 맞춘 축|맞추지 않은 축|',
            '|---|---|---|---|---|---|',
            '|PB|두 Backup 적용 ACK 후|보존(단일 영구 crash 전제)|Backup heartbeat timeout, 고정 승계|부하·값·FD·settled idle crash|분단 안전성·재시작·exactly-once 없음|',
            '|VR|과반수 PREPARE_OK 후|보존|heartbeat timeout, view change|부하·값·FD·settled idle crash|checkpoint·2MiB 로그 한도|',
            '|Raft|과반수 복제 후 현재 term commit|보존|난수 선거 timeout, 선출|부하·값·FD 기준값|선거 timeout 범위가 PB/VR과 다름, 영속화 없음|',
            '|Chain|tail 적용·결과 중계 후(역방향 Ack 전에는 다음 쓰기 BUSY)|보존(정확한 영구 crash·외부 master 전제)|controller가 master: 고정 대기 후 FREEZE/REPAIR/OPEN|부하·값·대기값·settled idle crash·위치별|실제 감지기·master 합의 없음, 파이프라인 쓰기 없음|',
            '|CRAQ|Chain과 동일, 읽기는 clean 로컬/dirty tail 버전 조회|Chain과 동일|Chain과 동일|읽기 경로만 TAIL_ONLY와 번갈아 비교|crash 중 읽기 가용성 미측정|',
            '|Rosé 변형|L=0 전원 적용 ACK, L>0 로컬 적용 후(lag<L), L=∞ 무제한|L=0 보존, L>0 최대 L건 유실 허용|controller가 master: 고정 대기 후 최장 prefix 승격|쓰기 진행 중 crash·client 8개 동시|단일 파티션·op 상한 변형, 원 논문 재현 아님|','',
            '## 재현 절차','',
            '각 cohort는 `run_<cohort>_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새 폴더>`로 실행하고 `analyze_<cohort>.py <폴더>`로 CSV·그래프·문서를 다시 만든다. 배포본 SHA256은 각 `*-results/sources.json`에 있다. 이 문서는 `analyze_overall.py`가 요약 CSV에서 생성한다.','',
            '|cohort|실행|분석|배포본 SHA256|',
            '|---|---|---|---|']
    for name,run,an in (('election-results','run_election_ec2.py','analyze_election.py'),('catchup-results','run_catchup_ec2.py','analyze_catchup.py'),('chain-results','run_chain_ec2.py','analyze_chain.py'),('craq-results','run_craq_ec2.py','analyze_craq.py'),('rose-results','run_rose_ec2.py','analyze_rose.py')):
        lines.append(f"|{name}|{run}|{an}|{sources[name]}|")
    lines+=['','## 한계','',
            '- 단일 client(Rosé는 8개) closed-loop·128~512표본·조건별 3회다. 최대 처리량·운영 p99·SSD·장기 실행은 측정하지 않았다.',
            '- PB/VR/Raft는 자체 heartbeat/선거 감지, Chain/Rosé는 controller의 고정 대기이므로 failover 절대값을 섞어 순위를 매기지 않는다. 감지 이후 전환 비용도 측정 주체(서버 자체 vs controller 제어 왕복)가 다르다.',
            '- crash 모델이 다르다: PB/VR/Raft/Chain은 양쪽 복제본 값 확인 후 idle crash, Rosé는 쓰기 진행 중 crash다. Rosé의 유실 수는 L의 상한 검증이지 다른 방식의 유실 0과 같은 조건이 아니다.',
            '- 계측·warmup 절차가 cohort마다 조금씩 달라(예: VR traffic/catchup의 안정화 대기) 정상 지연의 소수점 차이를 방식 차이로 해석하지 않는다.',
            f'- CRAQ의 짧은 clean 지연은 head/middle에서 redirect를 따라 tail에 재요청하는 경로와 비교한 결과다. TAIL_ONLY 직접 tail p50 범위는{min(direct_tail):.2f}~{max(direct_tail):.2f}ms, CRAQ head/middle은{min(craq_local):.2f}~{max(craq_local):.2f}ms다. 전체 읽기 처리량 우위는 이번 실험으로 확인하지 않았다.',
            '- CRAQ 실서버 검증은 순번 역행 검사이며 호출/완료 history 기반 선형화 가능성 검증은 아니다. 복사본 검사는 warm key에 적용됐고 혼합 hot key의 최종 값은 tail에서만 확인했다.',
            '- 모든 엔진은 연구 prototype이다. 영속화·재시작·membership 변경·분단 fencing·checkpoint는 구현하지 않았다.','']
    (args.output/'overall-results.md').write_text('\n'.join(lines),encoding='utf-8');print('overall-results.md written')


if __name__=='__main__':main()
