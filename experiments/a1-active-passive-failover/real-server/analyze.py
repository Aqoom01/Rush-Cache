"""실서버 원본 12개를 검증해 CSV·그래프·측정 결과 표를 생성한다. 접속 정보는 출력하지 않는다."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager

FIELDS=("valueBytes","timeoutMs","repeat","normalRequests","normalP50Ns","normalP99Ns",
        "observedSingleClientOpsPerSecond","failoverNs","promotionNs","preservedSuccesses",
        "preCrashVerifiedCopies","postCrashVerifiedCopies","faultInvocationNs","staleEpochStatus","backupWriteStatus")

def validate(rows):
    expected={(size,timeout,repeat) for size in (64,8192) for timeout in (500,1000) for repeat in (1,2,3)}
    assert len(rows)==12
    assert {(r["valueBytes"],r["timeoutMs"],r["repeat"]) for r in rows}==expected
    for r in rows:
        assert r["ok"] and r["schema"]=="a1-real-pb-1" and r["timeDomain"]=="wall-clock"
        assert r["normalRequests"]==len(r["normalSamplesNs"])==r["preservedSuccesses"]==1000
        samples=sorted(r["normalSamplesNs"])
        assert r["normalP50Ns"]==samples[499] and r["normalP99Ns"]==samples[989]
        assert r["preCrashVerifiedCopies"]==2000 and r["postCrashVerifiedCopies"]==2002
        assert r["staleEpochStatus"]==5 and r["backupWriteStatus"]==4
        assert r["failoverNs"]==r["firstNewPrimaryOkNs"]-r["faultCallBeginNs"]>0
        assert r["faultInvocationNs"]==r["faultCallReturnNs"]-r["faultCallBeginNs"]>=0
        status=r["statesAfter"][0]
        assert status["node"]==1 and status["epoch"]==2
        assert r["promotionNs"]==status["readyNs"]-status["detectedNs"]>=0
        assert r["attempts"][-1]["node"]==1 and r["attempts"][-1]["status"]==0
        assert math.isclose(r["observedSingleClientOpsPerSecond"],1e12/r["normalElapsedNs"])

def main():
    p=argparse.ArgumentParser()
    p.add_argument("raw",type=Path)
    p.add_argument("--output",type=Path,default=Path(__file__).resolve().parent)
    args=p.parse_args()
    paths=sorted(args.raw.glob("v*-t*-r*.json"))
    rows=[json.loads(path.read_text(encoding="utf-8")) for path in paths]
    validate(rows)
    output=args.output
    (output/"results").mkdir(parents=True,exist_ok=True)
    (output/"figures").mkdir(exist_ok=True)
    with (output/"results/summary.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=FIELDS,extrasaction="ignore",lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    metadata={
        "runCount":12,"timeDomain":"wall-clock","normalSuccesses":sum(r["normalRequests"] for r in rows),
        "acknowledgedWritesPreserved":sum(r["preservedSuccesses"] for r in rows),
        "preCrashCopies":sum(r["preCrashVerifiedCopies"] for r in rows),
        "postCrashCopies":sum(r["postCrashVerifiedCopies"] for r in rows),
        "distributionSha256":json.loads((args.raw/"manifest.json").read_text(encoding="utf-8"))["distSha256"],
        "rawSha256":{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        "rawLocation":"local ignored run directory; not a public object storage URL"
    }
    (output/"figures/sources.json").write_text(json.dumps(metadata,indent=2)+"\n",encoding="utf-8")
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"font.size":10,"figure.dpi":160,"axes.unicode_minus":False})
    def group(size,timeout):
        return sorted([r for r in rows if r["valueBytes"]==size and r["timeoutMs"]==timeout],key=lambda r:r["repeat"])
    def panel(ax,field,divisor,title,ylabel):
        for size,offset,color,label in ((64,-.13,"#497eb8","64 B"),(8192,.13,"#d29041","8 KiB")):
            for x,timeout in enumerate((500,1000)):
                values=[r[field]/divisor for r in group(size,timeout)]
                median=statistics.median(values)
                ax.errorbar(x+offset,median,yerr=[[median-min(values)],[max(values)-median]],fmt="o",
                            capsize=6,color=color,label=label if x==0 else None)
                ax.scatter([x+offset-.025,x+offset,x+offset+.025],values,color=color,s=15,alpha=.6)
        ax.set_xticks((0,1),("500ms","1000ms"))
        ax.set_xlabel("장애 감지 timeout")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xlim(-.5,1.5)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y",alpha=.2)
    fig,axes=plt.subplots(1,2,figsize=(11,5))
    panel(axes[0],"failoverNs",1e6,"Primary SIGKILL → 새 Primary 첫 SET OK","controller 기준 실제 ms")
    panel(axes[1],"promotionNs",1e6,"감지 후 구성 설치·준비 확인","승격 노드 자체 시계의 실제 ms")
    axes[0].legend()
    fig.suptitle("EC2 세 대에서 PB crash 복구 — 조건별 3회",fontsize=15)
    fig.text(.5,.015,"중앙점=중앙값, 세 점/오차막대=실측 3회와 최소~최대. 서로 다른 호스트의 nanoTime을 차감하지 않음.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.06,1,.92))
    fig.savefig(output/"figures/failover.png")
    plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(11,5))
    panel(axes[0],"normalP99Ns",1e6,"정상 SET의 실행별 p99","실제 ms")
    panel(axes[1],"observedSingleClientOpsPerSecond",1,"단일 client 관측 처리율","SET / 초")
    axes[0].legend()
    fig.suptitle("PB 정상 동기 복제 — 매회 warmup 30건 + 측정 1,000건",fontsize=14)
    fig.text(.5,.015,"client는 원래 Primary EC2에 공존, closed-loop·동시성 1. 최대 처리량·운영 p99·NVMe 성능의 측정이 아님.",ha="center",fontsize=8)
    fig.tight_layout(rect=(0,.06,1,.92))
    fig.savefig(output/"figures/normal-cost.png")
    plt.close(fig)
    lines=["# PB EC2 실측 결과 — 2026-11-04","",
           "m6gd.medium 세 대 · Ubuntu 24.04 arm64 · OpenJDK21 · 같은 AZ · 실제 프라이빗 TCP.",
           "값 2 × 감지 timeout 2 × 반복 3 = 12회. 정상 SET 성공 12,000건, 첫 post-crash SET 12건. 모든 실행 성공.","",
           "|값|timeout|SET p99 중앙값 (ms)|단일 client SET/s 중앙값|첫 쓰기 복구 중앙값 [최소~최대] (ms)|감지 후 전환 중앙값 (ms)|성공 쓰기 보존|",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for size in (64,8192):
        for timeout in (500,1000):
            g=group(size,timeout)
            lat=statistics.median(r["normalP99Ns"] for r in g)/1e6
            ops=statistics.median(r["observedSingleClientOpsPerSecond"] for r in g)
            fail=[r["failoverNs"]/1e6 for r in g]
            promotion=statistics.median(r["promotionNs"] for r in g)/1e6
            lines.append(f'|{size}B|{timeout}|{lat:.3f}|{ops:.1f}|{statistics.median(fail):.3f} [{min(fail):.3f}~{max(fail):.3f}]|{promotion:.3f}|3000/3000|')
    lines+=["","![복구 시간](figures/failover.png)","","![정상 비용](figures/normal-cost.png)","",
            "## 보존과 응답 검증","",
            "- 매회 장애 전 OK 1,000건을 두 Backup에서 확인(총 24,000개 복사본).",
            "- 장애 후 같은 1,000건과 첫 새 쓰기를 생존 두 노드에서 확인(총 24,024개 복사본). 유실/값 불일치 0.",
            "- 12회 모두 epoch1 쓰기를 새 Primary가 STALE_EPOCH로 거절했고, Backup 직접 쓰기는 NOT_PRIMARY + Redirect(1)였다.",
            "- 정상 부하 종료 후 crash를 주입했으므로 진행 중 쓰기 복구나 중복 요청 보장까지 검증한 것은 아니다.","",
            "## 해석","",
            "이번 조건에서는 timeout 500ms에서 약 480~536ms, timeout 1000ms에서 약 989~1041ms에 첫 새 쓰기가 성공했다. 감지 후 전환은 약 11~17ms였다. 감지 timeout이 전체 복구 시간에 큰 비중을 차지한다는 관측이며 다른 프로토콜과의 실측 순위는 아니다.",
            "첫 쓰기 복구는 같은 controller의 SIGKILL 호출 직전~새 Primary OK 수신 구간이다. 서버 전환 시간은 승격 노드 내부 duration이며, 서로 다른 호스트의 monotonic 시각을 직접 비교하지 않았다.","",
            "정상 p99는 매회 1,000개 sample의 p99를 구한 뒤 실행별 중앙값으로 요약했다. client와 Primary의 CPU 공존, 30건 warmup, 3회 반복, 단일 in-flight·동시성 1을 명시한다. CPU·실제 TCP가 포함된 측정이지만 최대 처리량이나 SSD 성능 결과는 아니다.","",
            "현재 PB만 실서버에 연결했다. 연속 SET 중 client 요청과 crash가 겹치는 조건은 [별도 12회 결과](load-results.md)에 있다. VR/Raft/Chain/CRAQ/Rosé 실서버 비교, 서버 내부 처리 단계 고정 crash·ACK 유실/지연·partition·재시작은 후속이다.","",
            "## 재현과 보관","",
            "- [조건·실행 방법](README.md), [12행 CSV](results/summary.csv), [원본별 SHA-256 및 실행 artifact hash](figures/sources.json).",
            "- 원본 JSON·서버 로그·환경·실제 배포 tar는 로컬 Git 제외 실행 폴더에 보관한다. 개인 키·IP·멘토링 자료는 저장소 문서에 넣지 않는다.",
            "- 이 브랜치 변경은 미커밋이다. 기준 develop commit과 배포 artifact hash를 함께 기록하며 기준 commit만으로 새 구현이 포함됐다고 해석하지 않는다.",""]
    (output/"results.md").write_text("\n".join(lines),encoding="utf-8")
    print(json.dumps(metadata,indent=2))

if __name__=="__main__":
    main()
