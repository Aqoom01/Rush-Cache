"""PB/VR/Raft 비교 CSV로 리더 crash 지연과 시나리오별 성공을 그린다."""
import csv
import hashlib
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager
from matplotlib.colors import ListedColormap

HERE=Path(__file__).resolve().parent
CSV=HERE/"runs/run-2026-10-29T00-25-33.998339700Z-530f1fcd/repeat-001/summary.csv"
OUT=HERE/"figures"


def main():
    with CSV.open(encoding="utf-8",newline="") as source:
        rows=list(csv.DictReader(source))
    assert len(rows)==216
    assert len({(r['variant'],r['scenario'],r['seed'],r['valueBytes'],r['mibps']) for r in rows})==216
    assert all(r['preOk']==r['retainedPre'] and r['postOk']==r['retainedPost'] for r in rows)
    font=Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font)); plt.rcParams['font.family']='Malgun Gothic'
    plt.rcParams.update({'font.size':10,'figure.dpi':160,'axes.unicode_minus':False})
    OUT.mkdir(exist_ok=True)
    colors={'PB':'#5374a6','VR':'#d58c37','RAFT':'#3a967b'}
    fig,axes=plt.subplots(2,2,figsize=(10.5,7),sharex=True)
    for y,size in enumerate((64,8192)):
        for x,rate in enumerate((1,10)):
            ax=axes[y][x]
            for variant in ('PB','VR','RAFT'):
                selected=sorted([r for r in rows if r['variant']==variant and r['scenario']=='CRASH'
                                 and int(r['valueBytes'])==size and int(r['mibps'])==rate],key=lambda r:int(r['seed']))
                assert len(selected)==3 and all(int(r['firstOkDelayNs'])>=0 for r in selected)
                ax.plot([int(r['seed']) for r in selected],[int(r['firstOkDelayNs'])/1e6 for r in selected],
                        marker='o',color=colors[variant],label=variant)
            ax.set_title(f"값 {size:,}B · 링크 {rate}MiB/s")
            ax.set_xticks((1,2,3)); ax.set_ylabel('첫 성공 응답까지 (가상 ms)'); ax.grid(alpha=.2); ax.set_ylim(bottom=0)
            if y==1: ax.set_xlabel('고정 seed')
    fig.suptitle('리더 crash 이후 새 쓰기의 첫 OK',fontsize=15)
    handles,labels=axes[0][0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,.04),ncol=3)
    fig.text(.5,.012,'PB/VR timeout 200ms · Raft [200,400)ms. 감지 분포가 달라 선출 자체의 속도 순위로 해석하지 않는다.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.1,1,.95));fig.savefig(OUT/'crash-delay.png');plt.close(fig)

    scenarios=('NORMAL','SLOW_BACKUP','CRASH','PARTITION','INFLIGHT_CRASH','FOLLOWER_REJOIN')
    variants=('PB','VR','RAFT')
    groups=[[ [r for r in rows if r['variant']==v and r['scenario']==s] for v in variants] for s in scenarios]
    cells=[[int(sum(int(r['postOk']) for r in g)==192 and sum(int(r['converged']) for r in g)==12) for g in line] for line in groups]
    fig,ax=plt.subplots(figsize=(10,6.5))
    ax.imshow(cells,cmap=ListedColormap(['#f4d3a7','#cce8df']),vmin=0,vmax=1,aspect='auto')
    for y,line in enumerate(groups):
        for x,g in enumerate(line):
            total=sum(int(r['postOk']) for r in g); same=sum(int(r['converged']) for r in g)
            ax.text(x,y,f'새 쓰기 {total}/192 성공\n최종 생존 KV 일치 {same}/12조건',ha='center',va='center',fontsize=10)
    ax.set_xticks(range(3),variants);ax.set_yticks(range(6),scenarios)
    ax.set_title('시나리오별 새 쓰기 완료와 최종 KV 일치',pad=16,fontsize=15)
    fig.text(.5,.045,'각 칸 12조건·192건. 녹색: 전부 성공 및 KV 일치 / 주황: 성공 또는 KV 일치 일부 미충족.',ha='center',fontsize=9)
    fig.text(.5,.015,'PB는 crash-only 기준형이다. 모든 방식의 성공 쓰기 보존 근거는 확인했으며 KV 일치와 구분한다.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.09,1,1));fig.savefig(OUT/'scenario-summary.png');plt.close(fig)
    (OUT/'sources.json').write_text(json.dumps({'csv':str(CSV.relative_to(HERE)),
        'sha256':hashlib.sha256(CSV.read_bytes()).hexdigest().upper(),'conditions':216,
        'timeDomain':'virtual','plots':['crash-delay.png','scenario-summary.png']},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
