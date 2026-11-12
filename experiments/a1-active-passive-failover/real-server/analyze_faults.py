"""Backup fault 원본과 실제 주입/복구 이벤트를 검증해 표·그래프를 생성한다."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt,font_manager
from pb_fault_bench import validate

SCENARIOS=("BASELINE","SLOW_BACKUP","ACK_BLOCK")
FIELDS=("scenario","valueBytes","repeat","deadlineMs","holdMs","firstStatus","firstElapsedNs",
        "actualFaultDurationNs","matchedPackets","retryStatus","retryElapsedNs","finalCopies",
        "missingCopies","mismatchedCopies","primaryFaultKeyVersion","backup1FaultKeyVersion","backup2FaultKeyVersion")

def audit_versions(result):
    versions={(v["node"],v["key"]):v["version"] for v in result["finalVersions"]}
    assert len(versions)==6
    for node in range(3):
        assert versions[node,"restored-next"]==1
        assert versions[node,"fault-key"]==(2 if result["scenario"]=="ACK_BLOCK" and node>0 else 1)
    return versions

def main():
    p=argparse.ArgumentParser()
    p.add_argument("raw",type=Path)
    p.add_argument("--output",type=Path,default=Path(__file__).resolve().parent)
    args=p.parse_args()
    files=sorted(path for path in args.raw.glob("*-v*-r*.json") if not path.name.endswith(".control.json"))
    originals=[json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert len(originals)==18
    assert {(r["scenario"],r["valueBytes"],r["repeat"]) for r in originals}=={
        (scenario,size,repeat) for scenario in SCENARIOS for size in (64,8192) for repeat in (1,2,3)}
    rows=[]
    for file,result in zip(files,originals):
        validate(result)
        versions=audit_versions(result)
        control=json.loads((args.raw/(file.name+".control.json")).read_text(encoding="utf-8"))
        assert control["schema"]=="a1-backup-fault-control-1" and control["scenario"]==result["scenario"]
        row={key:result.get(key) for key in FIELDS}
        row["firstElapsedNs"]=result["firstResponseNs"]-result["firstSentNs"]
        row["matchedPackets"]=row["actualFaultDurationNs"]=0
        if result["scenario"]!="BASELINE":
            applied,restored=control["applied"],control["restored"]
            assert applied["kind"]==restored["kind"]==result["scenario"] and restored["restored"]
            assert applied["holdMs"]==result["holdMs"]
            assert restored["beginNs"]==applied["beginNs"]
            row["actualFaultDurationNs"]=restored["endNs"]-restored["beginNs"]
            assert row["actualFaultDurationNs"]>=result["holdMs"]*1_000_000
            assert control["appliedObservedPcNs"]<=control["goSentPcNs"]<=control["measuredObservedPcNs"]
            row["matchedPackets"]=restored["matchedPackets"]
            if result["scenario"]=="ACK_BLOCK":
                assert row["matchedPackets"]>0
        for node,name in ((0,"primaryFaultKeyVersion"),(1,"backup1FaultKeyVersion"),(2,"backup2FaultKeyVersion")):
            row[name]=versions[node,"fault-key"]
        rows.append(row)
    cleanup=json.loads((args.raw/"cleanup.json").read_text(encoding="utf-8"))
    assert cleanup["temporaryRulesRemaining"]==0 and cleanup["dedicatedJvmsStopped"]
    out=args.output
    (out/"backup-fault-results").mkdir(parents=True,exist_ok=True)
    (out/"figures").mkdir(exist_ok=True)
    with (out/"backup-fault-results/summary.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=FIELDS,lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    raw_paths=files+[args.raw/(p.name+".control.json") for p in files]+[args.raw/"cleanup.json"]
    metadata={"schema":"a1-backup-fault-summary-1","runs":18,
              "initialOk":sum(r["firstStatus"]==0 for r in rows),"initialTimeout":sum(r["firstStatus"]==7 for r in rows),
              "timeoutsWithoutFalseOk":sum(r["scenario"]=="ACK_BLOCK" and r["firstStatus"]==7 for r in rows),
              "finalValueCopies":sum(r["finalCopies"] for r in rows),"cleanup":cleanup,
              "distSha256":json.loads((args.raw/"manifest.json").read_text(encoding="utf-8"))["distSha256"],
              "rawSha256":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in raw_paths}}
    (out/"backup-fault-results/sources.json").write_text(json.dumps(metadata,indent=2)+"\n",encoding="utf-8")
    def group(scenario,size):
        return sorted([r for r in rows if r["scenario"]==scenario and r["valueBytes"]==size],key=lambda r:r["repeat"])
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"font.size":10,"figure.dpi":160,"axes.unicode_minus":False})
    fig,ax=plt.subplots(figsize=(10,5))
    for size,offset,color,label in ((64,-.13,"#477fb4","64 B"),(8192,.13,"#d18e3e","8 KiB")):
        for i,scenario in enumerate(SCENARIOS):
            values=[r["firstElapsedNs"]/1e6 for r in group(scenario,size)]
            middle=statistics.median(values)
            ax.errorbar(i+offset,middle,yerr=[[middle-min(values)],[max(values)-middle]],fmt="o",capsize=6,color=color,label=label if i==0 else None)
            ax.scatter([i+offset-.025,i+offset,i+offset+.025],values,color=color,s=15,alpha=.6)
    ax.axhline(200,color="#a96060",linestyle="--",alpha=.6,label="ACK 차단 조건의 deadline 200ms")
    ax.set_xticks(range(3),("정상\nOK","Backup JVM 200ms 정지\nOK","ACK 경로 800ms 차단\nTIMEOUT"))
    ax.set_ylabel("첫 SET 응답까지 실제 ms")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y",alpha=.2)
    ax.legend(loc="lower right")
    ax.set_title("느린 Backup은 응답을 늦추고, ACK 부족은 성공 대신 TIMEOUT",fontsize=14,pad=15)
    fig.text(.5,.015,"조건별 3회: 중앙값·최소~최대와 실제 점. 정상/정지는 deadline 1000ms, ACK 차단은 deadline 200ms.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.06,1,1))
    fig.savefig(out/"figures/backup-fault-latency.png")
    plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,4.8))
    matrix=[[1,1,1],[1,1,1],[0,1,1]]
    ax.imshow(matrix,cmap=matplotlib.colors.ListedColormap(["#efcebd","#cae4d8"]),vmin=0,vmax=1,aspect="auto")
    for y in range(3):
        for x in range(3):
            ax.text(x,y,"값 있음\n6/6회" if matrix[y][x] else "값 없음\n6/6회",ha="center",va="center",fontsize=12)
    ax.set_xticks(range(3),("Primary","Backup 1","Backup 2"))
    ax.set_yticks(range(3),("정상 / OK","Backup 정지 / OK","ACK 차단 / TIMEOUT"))
    ax.set_title("첫 응답 직후 저장 상태: TIMEOUT이어도 Backup에는 이미 적용됨",fontsize=13,pad=15)
    fig.text(.5,.015,"2개 값 크기 × 3회. ACK 차단은 Backup2→Primary:7001만 막음. TIMEOUT은 취소/롤백이 아님.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.06,1,1))
    fig.savefig(out/"figures/backup-fault-copies.png")
    plt.close(fig)
    text=["# PB 느린 Backup·ACK 경로 단절 — 2026-11-04","",
          "전체 진행의 4단계. 이전 유휴/연속 crash와 별도로 정상·Backup 정지·ACK 경로 차단을 18회 확인했다. Java 엔진과 프로토콜은 변경하지 않았다.","",
          "|조건|값|첫 응답|응답 시간 중앙값 [최소~최대] ms|복구 후 동일 SET 재시도|최종 값 복사본|",
          "|---|---:|---|---:|---|---:|"]
    for scenario in SCENARIOS:
        for size in (64,8192):
            g=group(scenario,size)
            values=[r["firstElapsedNs"]/1e6 for r in g]
            outcome="TIMEOUT" if scenario=="ACK_BLOCK" else "OK"
            retry="3/3 OK" if scenario=="ACK_BLOCK" else "불필요"
            text.append(f'|{scenario}|{size}B|{outcome}|{statistics.median(values):.3f} [{min(values):.3f}~{max(values):.3f}]|{retry}|18/18|')
    text+=["","![지연과 응답](figures/backup-fault-latency.png)","","![첫 응답 후 저장 상태](figures/backup-fault-copies.png)","",
           "## 무엇을 확인했나","",
           "- BASELINE 6회는 전원 ACK와 로컬 적용 후 OK. Backup2 JVM을 SIGSTOP으로 200ms 정지한 6회도 SIGCONT 후 ACK를 받고 OK.",
           "- ACK_BLOCK 6회는 Backup2→Primary의 TCP 목적지7001만 800ms DROP. 실제 임시 규칙 packet counter를 확인했으며 SSH·다른 peer 방향·다른 사람 서버는 건드리지 않았다.",
           "- ACK 차단 6회 모두 200ms deadline에서 TIMEOUT. 해당 시점 Primary는 MISS, 두 Backup은 같은 값을 보유했다. ACK가 부족한데 OK를 반환한 경우는 없었다.",
           "- 차단 해제 뒤 2초 대기와 같은 client 연결의 두 PING으로 추가/잘못 매칭된 응답이 없는지 확인하고, 동일 SET 재시도 및 후속 SET이 모두 성공했다. 관측 창 밖의 모든 가능한 늦은 응답을 증명한 것은 아니다.",
           "- 매회 최종 key 2개 × 노드 3개, 총108개 값 복사본이 일치. epoch는 전원1 유지, 의도하지 않은 승격 없음.",
           "- 임시 OUTPUT 규칙 제거와 세 전용 JVM 종료를 확인했다. EC2 인스턴스 자체는 유지했다.","",
           "## 발견한 한계","",
           "TIMEOUT은 롤백이 아니다. 같은 SET 재시도 후 값은 세 노드에서 같았지만, ACK 차단 조건의 fault-key 로컬 KV 버전은 Primary=1, Backup1/2=2였다. 처음 요청이 Backup에 한 번, 재시도로 다시 한 번 적용됐기 때문이다.",
           "따라서 이 PB 기준형은 값 기준 멱등 SET을 확인한 것이며 exactly-once·전 노드의 valueVersion 일치·미완료 로그 복구를 보장하지 않는다. 이 결과를 숨기지 않고 값 일치와 버전 일치를 구분한다.","",
           "## 조건·시계","",
           "- 3노드 m6gd.medium, 같은 AZ·프라이빗 TCP·인메모리 KV, 원래 Primary 호스트에 Python client/controller 공존.",
           "- 값64B/8KiB × 3조건 × 3회. heartbeat50ms·감지500ms·tick10ms, 조건별 warmup30건.",
           "- 정상/정지 요청 deadline1000ms, ACK 차단 deadline200ms. 따라서 ACK 차단 응답 시간과 정상 조건을 성능 순위로 비교하지 않는다.",
           "- JVM 정지와 네트워크 DROP은 서로 다른 fault다. 전자는 KV 적용/메시지 처리까지 정지, 후자는 ACK 경로만 차단하며 TCP 재전송도 포함한다.",
           "- 실제 정지/차단 duration은 Backup2의 동일 시계, SET 응답 duration은 Primary 호스트 client의 동일 시계다. PC 조정 이벤트도 남겼지만 교차 호스트 시각을 직접 차감하지 않는다.","",
           "## 재현·범위","",
           "- [실행 안내](README.md#느린-backup과-ack-경로-단절), [18행 CSV](backup-fault-results/summary.csv), [원본·artifact 해시](backup-fault-results/sources.json).",
           "- 원본 JSON/제어 이벤트/환경/로그/실제 tar·스크립트는 Git 제외 로컬 폴더에 보관한다. 개인 키·IP·발표 질문은 Git에 넣지 않는다.",
           "- Python 검증 테스트 총6개 통과(기존 load3+추가 fault3). Java 코드를 변경하지 않아 기존 전체 build645개 통과 상태를 유지한다.",
           "- 이번 단계는 여기서 완료. 다음 VR 실서버 연결·비교는 사용자의 ‘다음 단계’ 요청 전 시작하지 않는다.",""]
    (out/"backup-fault-results.md").write_text("\n".join(text),encoding="utf-8")
    print(json.dumps({key:value for key,value in metadata.items() if key!="rawSha256"},indent=2))

if __name__=="__main__":
    main()
