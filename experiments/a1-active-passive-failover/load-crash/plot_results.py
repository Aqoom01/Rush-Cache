"""부하 중 crash CSV에서 첫 쓰기 재개와 시계열을 그린다."""
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
from matplotlib.ticker import MaxNLocator

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
CSV=HERE/"runs/run-2026-10-28T08-10-13.129127300Z-c4191cbd/repeat-001/summary.csv"
OUTPUT=HERE/"figures"


def main():
    with CSV.open(encoding="utf-8",newline="") as file:
        rows=list(csv.DictReader(file))
    cases={(r["variant"],r["scenario"],int(r["intervalMs"]),int(r["valueBytes"]),int(r["mibps"])):r for r in rows if r["kind"]=="case"}
    assert len(cases)==48
    assert all(r["preCrashOk"]==r["retainedPreCrashAtEnd"] for r in cases.values())
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font));plt.rcParams["font.family"]="Malgun Gothic"
    plt.rcParams.update({"font.size":10,"figure.dpi":160,"axes.unicode_minus":False})
    OUTPUT.mkdir(exist_ok=True)
    fig,axes=plt.subplots(2,4,figsize=(13,7.2))
    columns=(("PB","CRASH","PB / 정상 Backup"),("PB","SLOW_CRASH","PB / 느린 Backup"),("VR_INCREMENTAL","CRASH","VR / 정상 Backup"),("VR_INCREMENTAL","SLOW_CRASH","VR / 느린 Backup"))
    for y,size in enumerate((64,8192)):
        for x,(variant,scenario,label) in enumerate(columns):
            axis=axes[y][x]
            selected=[[cases[(variant,scenario,i,size,b)] for b in (1,10)] for i in (1,5,20)]
            cells=[[int(int(r["failoverNs"])<0) for r in line] for line in selected]
            axis.imshow(cells,cmap=ListedColormap(["#c9e8dc","#f7c99b"]),vmin=0,vmax=1,aspect="auto")
            for yy,line in enumerate(selected):
                for xx,r in enumerate(line):
                    text="첫 OK 미관측" if int(r["failoverNs"])<0 else f"{int(r['failoverNs'])/1_000_000:,.0f} ms"
                    axis.text(xx,yy,text,ha="center",va="center",fontsize=8)
            axis.set_title(label,fontsize=10)
            axis.set_xticks((0,1),("1","10"));axis.set_xlabel("링크 MiB/s")
            axis.set_yticks(range(3),("1","5","20"))
            if x==0:axis.set_ylabel(("값 64B" if size==64 else "값 8KiB")+"\n제출 간격 ms")
    fig.suptitle("연속 쓰기 중 11초 Primary 종료: 새 쓰기 첫 OK까지",y=0.99)
    fig.legend(handles=[Patch(color="#c9e8dc",label="첫 OK 확인"),Patch(color="#f7c99b",label="이번 제출 요청에서 미관측")],loc="lower center",bbox_to_anchor=(0.5,0.035),ncol=2,fontsize=9)
    fig.text(0.5,0.012,"숫자는 가상 ms. 입력은 30초에 끝나며 미관측을 0이나 관측 상한으로 대입하지 않는다.",ha="center",fontsize=9)
    fig.tight_layout(rect=(0,0.1,1,0.95));fig.savefig(OUTPUT/"failover-matrix.png");plt.close(fig)

    for size,name in ((64,"small-values"),(8192,"large-values")):
        fig,axes=plt.subplots(3,2,figsize=(12,8),sharex=True)
        end_time=30;max_view=1
        for column,scenario in enumerate(("CRASH","SLOW_CRASH")):
            for variant,color,label in (("PB","#2563a6","PB"),("VR_INCREMENTAL","#21856e","VR (증분 전송)")):
                selected=sorted((r for r in rows if r["kind"]=="sample" and r["variant"]==variant and r["scenario"]==scenario and r["intervalMs"]=="5" and int(r["valueBytes"])==size and r["mibps"]=="1"),key=lambda r:int(r["timeNs"]))
                assert selected
                t=[int(r["timeNs"])/1_000_000_000 for r in selected];end_time=max(end_time,t[-1])
                seconds={int(r["timeNs"])//1_000_000_000:int(r["ok"]) for r in selected if int(r["timeNs"])%1_000_000_000==0}
                x=[s for s in sorted(seconds) if s>0 and s-1 in seconds]
                axes[0][column].step([s-0.5 for s in x],[seconds[s]-seconds[s-1] for s in x],where="mid",color=color,label=label,linewidth=1.8)
                axes[1][column].plot(t,[int(r["pendingLiveDataBytes"])/1048576 for r in selected],color=color,label=label,linewidth=1.5)
                views=[int(r["viewAdvance"]) for r in selected];max_view=max(max_view,max(views))
                axes[2][column].step(t,views,where="post",color=color,label=label,linewidth=1.5)
            axes[0][column].set_title("정상 Backup" if scenario=="CRASH" else "10~20초 노드 2 ACK +200ms")
            axes[0][column].legend(loc="upper right",fontsize=9)
            for row in range(3):
                axis=axes[row][column]
                if scenario=="SLOW_CRASH":axis.axvspan(10,20,color="#f5d9ab",alpha=0.35)
                axis.axvline(11,color="#b93838",linestyle="--",linewidth=1.2)
                axis.axvline(30,color="#666666",linestyle=":",linewidth=1)
                axis.grid(alpha=0.18);axis.set_ylim(bottom=0)
            axes[2][column].set_xlabel("가상 시간 (초)")
            axes[2][column].yaxis.set_major_locator(MaxNLocator(integer=True))
        axes[0][0].set_ylabel("완료 OK / 가상 초")
        axes[1][0].set_ylabel("생존 링크 미도착 data (MiB)")
        axes[2][0].set_ylabel("epoch / view 증가")
        for axis in axes.flat:axis.set_xlim(0,end_time+0.5)
        for axis in axes[2]:axis.set_ylim(0,max_view+1)
        label="64B" if size==64 else "8KiB"
        fig.suptitle(f"값 {label} · 입력 200회/s · 링크 1MiB/s · 빨간 선: Primary 종료",y=0.99)
        fig.text(0.5,0.012,"100ms 샘플. 선은 해당 실행의 관측 종료까지만 표시한다. 생존 data에서 죽은 노드 관련 frame은 제외한다.",ha="center",fontsize=9)
        fig.tight_layout(rect=(0,0.04,1,0.95));fig.savefig(OUTPUT/(name+".png"));plt.close(fig)
    files=[CSV,Path(__file__).resolve(),*sorted(OUTPUT.glob("*.png"))]
    (OUTPUT/"sources.json").write_text(json.dumps({"model":"sustained-primary-crash-1","sha256":{p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__":main()
