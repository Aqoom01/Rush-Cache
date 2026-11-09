"""연속 부하 중 client-inflight crash 원본을 검증해 요약과 성공 응답 시계열을 만든다."""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt,font_manager
from pb_load import classify_attempt,validate

FIELDS=("valueBytes","timeoutMs","repeat","preFaultOk","newPrimaryOk","oldPrimaryLateOk",
        "inflightFirstOutcome","inflightAttemptCount","inflightFinalStatus","failoverNs","promotionNs",
        "maxSuccessGapNs","maxLogicalLatencyNs","successes","preservedSuccesses","verifiedCopies",
        "missingCopies","mismatchedCopies","unresolvedLogical","preP99Ns","newPrimaryP99Ns")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("raw",type=Path)
    p.add_argument("--output",type=Path,default=Path(__file__).resolve().parent)
    args=p.parse_args()
    paths=sorted(args.raw.glob("v*-t*-r*.json"))
    rows=[json.loads(path.read_text(encoding="utf-8")) for path in paths]
    assert len(rows)==12
    assert {(r["valueBytes"],r["timeoutMs"],r["repeat"]) for r in rows}=={
        (size,timeout,repeat) for size in (64,8192) for timeout in (500,1000) for repeat in (1,2,3)}
    for row in rows:
        assert row["targetCount"]==300
        validate(row)
    out=args.output
    (out/"load-results").mkdir(parents=True,exist_ok=True)
    (out/"figures").mkdir(exist_ok=True)
    with (out/"load-results/summary.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=FIELDS,extrasaction="ignore",lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    initial=Counter(str(r["inflightFirstOutcome"]) for r in rows)
    replica_copies=Counter(sum(o["matches"] for o in r["candidateReplicaBeforeRetry"]) for r in rows if r["candidateReplicaBeforeRetry"])
    attempts=Counter(str(a["status"]) for r in rows for a in r["attempts"])
    metadata={
        "schema":"a1-real-pb-load-summary-1","runs":12,
        "preFaultOk":sum(r["preFaultOk"] for r in rows),"newPrimaryOk":sum(r["newPrimaryOk"] for r in rows),
        "oldPrimaryLateOk":sum(r["oldPrimaryLateOk"] for r in rows),
        "preservedSuccesses":sum(r["preservedSuccesses"] for r in rows),
        "verifiedCopies":sum(r["verifiedCopies"] for r in rows),
        "initialInflightOutcomes":dict(initial),"candidateObservedCopiesBeforeRetry":dict(replica_copies),
        "attemptStatuses":dict(attempts),
        "distributionSha256":json.loads((args.raw/"manifest.json").read_text(encoding="utf-8"))["distSha256"],
        "rawSha256":{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    }
    (out/"load-results/sources.json").write_text(json.dumps(metadata,indent=2)+"\n",encoding="utf-8")
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"font.size":10,"figure.dpi":160,"axes.unicode_minus":False})
    def group(size,timeout):
        return sorted([r for r in rows if r["valueBytes"]==size and r["timeoutMs"]==timeout],key=lambda r:r["repeat"])
    fig,axes=plt.subplots(1,2,figsize=(11,5))
    for ax,field,title in zip(axes,("failoverNs","maxSuccessGapNs"),("SIGKILL → 새 Primary 첫 SET OK","성공 SET 응답 사이 최대 공백")):
        for size,offset,color,label in ((64,-.13,"#477fb4","64 B"),(8192,.13,"#d18e3e","8 KiB")):
            for i,timeout in enumerate((500,1000)):
                values=[r[field]/1e6 for r in group(size,timeout)]
                middle=statistics.median(values)
                ax.errorbar(i+offset,middle,yerr=[[middle-min(values)],[max(values)-middle]],fmt="o",capsize=6,color=color,label=label if i==0 else None)
                ax.scatter([i+offset-.025,i+offset,i+offset+.025],values,s=13,color=color,alpha=.6)
        ax.set_xticks((0,1),("500ms","1000ms"))
        ax.set_xlabel("감지 timeout")
        ax.set_ylabel("실제 ms")
        ax.set_ylim(bottom=0)
        ax.set_xlim(-.5,1.5)
        ax.set_title(title)
        ax.grid(axis="y",alpha=.2)
    axes[0].legend()
    fig.suptitle("연속 SET 중 Primary crash — 매회 전 300건 + 새 Primary 300건 성공",fontsize=14)
    fig.text(.5,.015,"중앙값과 실측 3회 최소~최대. client sendall 직후 SIGKILL, 응답 전 요청 중첩을 trace로 확인. closed-loop·동시성 1.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.06,1,.92))
    fig.savefig(out/"figures/load-failover.png")
    plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(11,5),sharey=True)
    for ax,timeout in zip(axes,(500,1000)):
        r=group(8192,timeout)[0]
        fault=r["faultCallBeginNs"]
        bins={name:Counter() for name in ("PRE_FAULT_OK","NEW_PRIMARY_OK","OLD_PRIMARY_LATE_OK")}
        for a in r["attempts"]:
            kind=classify_attempt(a,fault)
            if kind in bins:
                bins[kind][(a["endNs"]-fault)//100_000_000]+=1
        keys=sorted({k for counts in bins.values() for k in counts})
        bottom=[0]*len(keys)
        for kind,color,label in (("PRE_FAULT_OK","#477fb4","장애 전 OK"),("NEW_PRIMARY_OK","#41947c","새 Primary OK"),
                                  ("OLD_PRIMARY_LATE_OK","#d18e3e","종료 후 옛 Primary OK")):
            values=[bins[kind][k] for k in keys]
            ax.bar([(k+.5)*.1 for k in keys],values,width=.095,bottom=bottom,color=color,label=label)
            bottom=[a+b for a,b in zip(bottom,values)]
        ax.axvline(0,color="#b95353",linestyle="--")
        first=r["failoverNs"]/1e9
        ax.axvline(first,color="#347e67",linestyle=":")
        ax.set_title(f'8KiB · timeout {timeout}ms · 반복 1')
        ax.set_xlabel("SIGKILL 주입 기준 실제 초")
        ax.grid(axis="y",alpha=.2)
    axes[0].set_ylabel("100ms 구간의 SET 성공 응답 수")
    handles,labels=axes[0].get_legend_handles_labels()
    fig.legend(handles,labels,loc="lower center",bbox_to_anchor=(.5,.05),ncol=3,fontsize=9)
    fig.suptitle("진행 중 요청 오류·재시도 구간에서 성공 응답이 중단됐다가 재개됨",fontsize=14)
    fig.text(.5,.015,"빨간 점선=SIGKILL, 녹색 점선=새 Primary 첫 OK. 시계열의 두 조건은 대표 실행이며 시간축은 controller의 동일 시계.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.14,1,.92))
    fig.savefig(out/"figures/load-timeline.png")
    plt.close(fig)
    lines=["# PB 연속 쓰기 중 Primary crash — 2026-11-04","",
           "실제 EC2 세 대에서 client 요청 전송 직후, 응답을 읽기 전에 Primary JVM을 종료했다. 기존 유휴 시점 crash 12회와 별도 집계한다.","",
           "|값|감지 timeout|첫 새 Primary OK 중앙값 [최소~최대] ms|최대 성공 공백 중앙값 ms|진행 중 요청 재시도 횟수 범위|성공 쓰기 보존|",
           "|---|---:|---:|---:|---:|---:|"]
    for size in (64,8192):
        for timeout in (500,1000):
            g=group(size,timeout)
            durations=[r["failoverNs"]/1e6 for r in g]
            gap=statistics.median(r["maxSuccessGapNs"] for r in g)/1e6
            retry=[r["inflightAttemptCount"]-1 for r in g]
            total=sum(r["successes"] for r in g)
            lines.append(f'|{size}B|{timeout}|{statistics.median(durations):.3f} [{min(durations):.3f}~{max(durations):.3f}]|{gap:.3f}|{min(retry)}~{max(retry)}|{total}/{total}|')
    lines+=["","![부하 중 복구·성공 공백](figures/load-failover.png)","","![성공 응답 시계열](figures/load-timeline.png)","",
            "## 확인한 것","",
            f'- 12회 모두 장애 전 300건과 새 Primary 300건 성공. 총 성공 쓰기 {metadata["preservedSuccesses"]:,}건, 생존 두 KV의 값 복사본 {metadata["verifiedCopies"]:,}개 확인. 유실·값 불일치·미해결 논리 요청 0.',
            f'- 진행 중 요청의 첫 결과 집계: {dict(initial)}. 같은 operation/key/value 재시도로 최종 성공. 옛 Primary의 늦은 OK는 새 Primary 복구로 세지 않았다.',
            f'- 종료 뒤 재시도 전 관측한 후보 key의 생존 복사본 수: {dict(replica_copies)}. 관측 시각과 상태를 원본에 기록하며 원자적 snapshot으로 간주하지 않는다.',
            "- 각 요청의 start/send/end와 SIGKILL 호출 전후 시각을 기록해 장애와 client 요청 구간이 겹쳤는지 확인했다.",
            "- 정상 client write는 중단 없이 closed-loop로 제출하고, 장애 후에는 같은 논리 SET을 최대 15초 예산·10ms 간격으로 재시도했다. 동시성은 1이다.",
            "- 상태 코드와 연결 오류를 모두 원본에 남겼다. STALE_EPOCH/Backup NOT_PRIMARY·Redirect도 매회 확인했다.","",
            "## 보장 범위","",
            "**실제로 검증한 장애 시점은 client-inflight다. 서버가 이미 쓰기를 받아 Backup에 전달한 특정 내부 단계까지 강제한 실험은 아니다.** 복사본이 없던 후보를 재시도로 새로 적용한 경우를 미완료 로그 복구의 증거라고 부르지 않는다.",
            "PB 자체에 client table이나 미완료 쓰기 복구를 추가하지 않았다. 동일 SET 재전송은 값 기준으로 멱등이며 exactly-once·valueVersion 중복 방지를 보장하지 않는다.",
            "타이밍은 같은 controller perf_counter로 측정했다. JVM 내부 전환 duration만 해당 JVM의 nanoTime 차이를 사용한다. EC2 자체·Backup을 종료하지 않았고 인위적 ACK 지연/partition도 주입하지 않았다.",
            "결과는 동일 AZ, m6gd.medium 세 대·인메모리 KV·원래 Primary 호스트에 공존한 client/controller 조건이다. 최대 처리량·SSD 성능 결과가 아니다.","",
            "## 재현·보관","",
            "- [실행 안내](README.md#연속-쓰기-중-crash), [12행 CSV](load-results/summary.csv), [원본 및 artifact SHA-256](load-results/sources.json).",
            "- 원본 JSON에는 요청별 attempt, 논리 완료, 후보 key 관측, 서버 상태가 있다. 개인 IP/키·서버 로그·원본은 Git 제외 로컬 폴더로 수집했다.",
            "- Python 집계 테스트 3개 통과. 이번 단계는 Python 실험/분석 추가이며 Java 프로토콜은 변경하지 않았다. 기존 Java 전체 build 645개 테스트 통과 상태를 유지한다.",
            "- 다음 알고리즘이나 느린 Backup 실험은 이 단계에서 시작하지 않았다.",""]
    (out/"load-results.md").write_text("\n".join(lines),encoding="utf-8")
    print(json.dumps(metadata,indent=2))

if __name__=="__main__":
    main()
