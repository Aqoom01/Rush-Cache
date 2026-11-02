"""보관된 개선 전후 CSV의 조건·대조군을 확인하고 비교 그림을 만든다."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
from matplotlib.ticker import NullFormatter

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
BEFORE=HERE.parent / "network-cost-comparison/runs/run-2026-10-28T01-36-54.664991200Z-d71e8b8d/repeat-001/summary.csv"
AFTER=HERE / "runs/run-2026-10-28T05-25-57.952704Z-1243c82b/repeat-001/summary.csv"
OUTPUT=HERE / "figures"


def load(path):
    with path.open(encoding="utf-8",newline="") as stream:
        rows=list(csv.DictReader(stream))
    assert len(rows)==216
    indexed={
        ("PB" if r["variant"]=="PB" else "VR",r["scenario"],int(r["history"]),int(r["valueBytes"]),int(r["mibps"]),int(r["timeoutMs"])):r
        for r in rows
    }
    assert len(indexed)==216
    return indexed


def main():
    before,after=load(BEFORE),load(AFTER)
    assert before.keys()==after.keys()
    for key in before:
        if key[0]=="PB":
            assert before[key]==after[key],"PB control changed"
    assert all(r["outcome"]=="OK" for r in after.values())
    formerly_censored=[k for k,r in before.items() if r["outcome"]=="CENSORED"]
    assert len(formerly_censored)==8 and all(after[k]["outcome"]=="OK" for k in formerly_censored)
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"axes.unicode_minus":False,"font.size":10,"figure.dpi":160})
    OUTPUT.mkdir(exist_ok=True)

    fig,axes=plt.subplots(1,2,figsize=(11.5,4.6))
    for dataset,variant,color,marker,label in (
        (before,"VR","#d97706","s","VR 개선 전"),
        (after,"VR","#21856e","o","VR 개선 후"),
        (after,"PB","#2563a6","^","PB 대조군"),
    ):
        values=[int(dataset[(variant,"NORMAL",h,8192,1,2000)]["responseNs"])/1_000_000 for h in (1,16,64)]
        axes[0].plot(range(3),values,color=color,marker=marker,label=label,linewidth=2)
        if variant=="VR":
            for x,value in enumerate(values):
                axes[0].annotate(f"{value:.2f}",(x,value),xytext=(0,8 if dataset is before else -16),textcoords="offset points",ha="center",fontsize=8)
    axes[0].set_ylabel("쓰기 응답 (가상 ms, 로그 축)")
    axes[0].set_yscale("log"); axes[0].set_ylim(5,1000); axes[0].legend(fontsize=9,loc="upper left")
    for dataset,color,marker,label in ((before,"#d97706","s","VR 개선 전"),(after,"#21856e","o","VR 개선 후")):
        values=[int(dataset[("VR","NORMAL",h,8192,1,2000)]["pendingBytes"])/1048576 for h in (1,16,64)]
        axes[1].plot(range(3),values,color=color,marker=marker,label=label,linewidth=2)
    axes[1].set_ylabel("응답 관측 시 미도착 데이터 (MiB, 로그 축)")
    axes[1].set_yscale("log"); axes[1].set_ylim(0.00001,100); axes[1].legend(fontsize=9)
    axes[1].set_yticks([1e-5,1e-4,1e-3,1e-2,1e-1,1,10,100],["0.00001","0.0001","0.001","0.01","0.1","1","10","100"])
    axes[1].yaxis.set_minor_formatter(NullFormatter())
    for axis in axes:
        axis.set_xticks(range(3),("1","16","64")); axis.set_xlim(-0.2,2.2)
        axis.set_xlabel("준비된 성공 연산 수"); axis.grid(axis="y",alpha=0.2)
    fig.suptitle("값 8KiB · 링크 1MiB/s · 초기 감지 2,000ms: 개선 전후",y=0.98)
    fig.text(0.5,0.02,"고정 모델의 단일 조건. 응답까지 관측 구간 길이는 다르며 선은 측정점 연결이다.",ha="center",fontsize=9)
    fig.tight_layout(rect=(0,0.075,1,0.94)); fig.savefig(OUTPUT/"normal-before-after.png"); plt.close(fig)

    fig,axes=plt.subplots(2,2,figsize=(10.5,6.7))
    for row,timeout in enumerate((100,2000)):
        for col,(dataset,label) in enumerate(((before,"개선 전"),(after,"개선 후"))):
            axis=axes[row][col]
            selected=[[dataset[("VR","CRASH",h,8192,b,timeout)] for b in (1,10,100)] for h in (1,16,64)]
            cells=[[int(r["outcome"]=="CENSORED") for r in line] for line in selected]
            axis.imshow(cells,cmap=ListedColormap(["#c9e8dc","#f7c99b"]),vmin=0,vmax=1,aspect="auto")
            for y,line in enumerate(selected):
                for x,r in enumerate(line):
                    text="5s 내 미완료" if r["outcome"]=="CENSORED" else f"{int(r['recoveryNs'])/1_000_000:.0f} ms"
                    axis.text(x,y,text,ha="center",va="center",fontsize=9)
            axis.set_title(f"{label} · 초기 감지 {timeout}ms",fontsize=10)
            axis.set_xticks(range(3),("1","10","100")); axis.set_xlabel("방향별 링크 (MiB/s)")
            axis.set_yticks(range(3),("1","16","64")); axis.set_ylabel("준비된 성공 연산 수")
    fig.suptitle("값 8KiB: crash 후 첫 쓰기 성공까지의 가상 시간",y=0.99)
    fig.legend(handles=[Patch(color="#c9e8dc",label="첫 OK 관측"),Patch(color="#f7c99b",label="5초 관측 상한까지 미완료")],loc="lower center",bbox_to_anchor=(0.5,0.035),ncol=2,fontsize=9)
    fig.text(0.5,0.01,"전송·재전송·전환 대기 정책을 함께 변경했다. 미완료를 복구 시간 5,000ms로 계산하지 않는다.",ha="center",fontsize=9)
    fig.tight_layout(rect=(0,0.095,1,0.95)); fig.savefig(OUTPUT/"crash-before-after.png"); plt.close(fig)
    files=[BEFORE,AFTER,Path(__file__).resolve(),*sorted(OUTPUT.glob("*.png"))]
    (OUTPUT/"sources.json").write_text(json.dumps({"model":"logical-link-fifo-1","sha256":{p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__":
    main()
