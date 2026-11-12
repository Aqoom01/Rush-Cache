"""연속 부하 CSV에서 대표 조건의 완료량·지연된 복제·큐·view를 그린다."""
from __future__ import annotations
import csv
import hashlib
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager
from matplotlib.ticker import MaxNLocator

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
CSV=HERE/"runs/run-2026-10-28T07-07-52.313873900Z-214c4fd3/repeat-001/summary.csv"
OUTPUT=HERE/"figures"


def main():
    with CSV.open(encoding="utf-8",newline="") as stream:
        rows=list(csv.DictReader(stream))
    cases=[r for r in rows if r["kind"]=="case"]
    assert len(cases)==48 and len({(r["variant"],r["scenario"],r["intervalMs"],r["valueBytes"],r["mibps"]) for r in cases})==48
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font)); plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"axes.unicode_minus":False,"font.size":10,"figure.dpi":160})
    OUTPUT.mkdir(exist_ok=True)
    for size,name in ((64,"small-values"),(8192,"large-values")):
        fig,axes=plt.subplots(4,1,figsize=(10.5,9),sharex=True)
        last_time=30; largest_view=0
        for variant,color,label in (("PB","#2563a6","PB"),("VR_INCREMENTAL","#21856e","VR (증분 전송)")):
            selected=sorted((r for r in rows if r["kind"]=="sample" and r["variant"]==variant and r["scenario"]=="SLOW_RECOVER" and r["intervalMs"]=="5" and int(r["valueBytes"])==size and r["mibps"]=="1"),key=lambda r:int(r["timeNs"]))
            assert selected
            t=[int(r["timeNs"])/1_000_000_000 for r in selected]
            last_time=max(last_time,t[-1])
            seconds={int(r["timeNs"])//1_000_000_000:int(r["ok"]) for r in selected if int(r["timeNs"])%1_000_000_000==0}
            x=[s for s in sorted(seconds) if s>0 and s-1 in seconds]
            y=[seconds[s]-seconds[s-1] for s in x]
            axes[0].step([s-0.5 for s in x],y,where="mid",color=color,label=label,linewidth=1.8)
            axes[2].plot(t,[int(r["pendingBytes"])/1048576 for r in selected],color=color,label=label,linewidth=1.5)
            if variant=="VR_INCREMENTAL":
                largest_view=max(int(r["viewAdvance"]) for r in selected)
                axes[1].plot(t,[int(r["applyLag"]) for r in selected],color=color,linewidth=1.5)
                axes[3].step(t,[int(r["viewAdvance"]) for r in selected],where="post",color=color,linewidth=1.5)
        axes[0].set_ylabel("완료 OK / 가상 초")
        axes[0].legend(loc="upper right")
        axes[1].set_ylabel("VR 노드 2\n적용 지연 (연산 수)")
        axes[2].set_ylabel("미도착 데이터 (MiB)"); axes[2].legend(loc="upper left")
        axes[3].set_ylabel("VR 최대 view 증가")
        axes[3].yaxis.set_major_locator(MaxNLocator(integer=True))
        axes[3].set_xlabel("가상 시간 (초)")
        for axis in axes:
            axis.axvspan(10,20,color="#f5d9ab",alpha=0.4)
            axis.axvline(30,color="#555555",linestyle="--",linewidth=1)
            axis.set_xlim(0,last_time+0.5); axis.set_ylim(bottom=0); axis.grid(alpha=0.18)
        axes[3].set_ylim(0,max(1,largest_view+1))
        title="64B" if size==64 else "8KiB"
        fig.suptitle(f"값 {title} · 입력 200회/s · 방향별 1MiB/s\n10~20초 ACK +200ms, 30초 입력 종료",y=0.99)
        fig.text(0.5,0.012,"100ms 샘플. 완료량은 누적 OK의 1초 차분. 각 선은 해당 실행의 관측 종료까지만 표시한다.",ha="center",fontsize=9)
        fig.tight_layout(rect=(0,0.035,1,0.95)); fig.savefig(OUTPUT/(name+".png")); plt.close(fig)
    files=[CSV,Path(__file__).resolve(),*sorted(OUTPUT.glob("*.png"))]
    (OUTPUT/"sources.json").write_text(json.dumps({"model":"sustained-ack-delay-1","sha256":{p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__":
    main()
