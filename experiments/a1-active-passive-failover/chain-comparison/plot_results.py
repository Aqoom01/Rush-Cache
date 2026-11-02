"""네 방식 공통 비교와 Chain 위치별 crash CSV를 그림으로 재생성한다."""
import csv
import hashlib
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager
from matplotlib.colors import ListedColormap

HERE=Path(__file__).resolve().parent
RUN=HERE/'runs/run-2026-10-29T00-42-37.370281300Z-5b8d5890/repeat-001'
OUT=HERE/'figures'


def read(name):
    with (RUN/name).open(encoding='utf-8',newline='') as file:
        return list(csv.DictReader(file))


def main():
    rows=read('summary.csv'); positions=read('position-summary.csv')
    assert len(rows)==288 and len(positions)==24
    assert len({(r['variant'],r['scenario'],r['seed'],r['valueBytes'],r['mibps']) for r in rows})==288
    assert all(r['preOk']==r['retainedPre'] and r['postOk']==r['retainedPost'] for r in rows)
    assert all(int(r['preKvCopies'])==32 and int(r['sentAfter'])==0 for r in positions)
    font=Path('C:/Windows/Fonts/malgun.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font)); plt.rcParams['font.family']='Malgun Gothic'
    plt.rcParams.update({'font.size':10,'figure.dpi':160,'axes.unicode_minus':False})
    OUT.mkdir(exist_ok=True)
    variants=('PB','VR','RAFT','CHAIN')
    scenarios=('NORMAL','SLOW_BACKUP','CRASH','PARTITION','INFLIGHT_CRASH','FOLLOWER_REJOIN')
    groups=[[[r for r in rows if r['variant']==v and r['scenario']==s] for v in variants] for s in scenarios]
    cells=[[int(sum(int(r['postOk']) for r in g)==192 and sum(int(r['converged']) for r in g)==12) for g in line] for line in groups]
    fig,ax=plt.subplots(figsize=(12,7))
    ax.imshow(cells,cmap=ListedColormap(['#f4d3a7','#cce8df']),vmin=0,vmax=1,aspect='auto')
    for y,line in enumerate(groups):
        for x,g in enumerate(line):
            count=sum(int(r['postOk']) for r in g); same=sum(int(r['converged']) for r in g)
            ax.text(x,y,f'새 쓰기 {count}/192\n최종 KV 일치 {same}/12조건',ha='center',va='center',fontsize=9)
    ax.set_xticks(range(4),variants); ax.set_yticks(range(6),scenarios)
    ax.set_title('PB / VR / Raft / Chain: 새 쓰기 완료와 최종 상태',pad=16,fontsize=15)
    fig.text(.5,.055,'칸마다 12조건. 녹색은 새 쓰기 전부 성공 및 최종 KV 일치, 주황은 일부 미충족이다.',ha='center',fontsize=9)
    fig.text(.5,.025,'Chain은 정확한 crash 통지 후 재구성하며 살아 있는 분단 노드는 제거하지 않는다. master 비용 가정이 다르다.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.1,1,1)); fig.savefig(OUT/'four-way-summary.png'); plt.close(fig)

    fig,axes=plt.subplots(1,2,figsize=(11,5.5),sharey=True)
    for pending,ax in enumerate(axes):
        selected=sorted([r for r in positions if int(r['pending'])==pending and int(r['valueBytes'])==8192 and int(r['mibps'])==1],key=lambda r:int(r['failed']))
        assert len(selected)==3
        detect=[(int(r['detectedAtNs'])-int(r['faultAtNs']))/1e6 for r in selected]
        repair=[int(r['repairNs'])/1e6 for r in selected]
        total=[int(r['failoverNs'])/1e6 for r in selected]
        rest=[t-d-r for t,d,r in zip(total,detect,repair)]
        ax.bar(range(3),detect,color='#c1c9d4',label='감지 (외부 oracle 200ms)')
        ax.bar(range(3),repair,bottom=detect,color='#3a967b',label='master 조정 + suffix/Ack 복구')
        ax.bar(range(3),rest,bottom=[d+r for d,r in zip(detect,repair)],color='#d58c37',label='복구 후 재탐색 + 첫 쓰기 완료')
        for x,t in enumerate(total):
            label=f'{t:.2f}ms'
            if pending: label+='\n미완료 '+('보존' if int(selected[x]['pendingRetained']) else '미보존')
            ax.text(x,t+4,label,ha='center',va='bottom',fontsize=9)
        ax.set_xticks(range(3),('head 종료','middle 종료','tail 종료'))
        ax.set_title('미완료 쓰기 '+('있음 (head에만 적용)' if pending else '없음'))
        ax.set_ylim(0,295); ax.grid(axis='y',alpha=.2); ax.set_axisbelow(True)
    axes[0].set_ylabel('crash부터 새 쓰기 첫 OK (가상 ms)')
    handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,.04),ncol=3,fontsize=8)
    fig.suptitle('Chain 위치별 crash: 8KiB · 1MiB/s',fontsize=15)
    fig.text(.5,.01,'모든 조건에서 성공 응답한 기존 쓰기 16건 × 생존 노드 2대 = KV 복사본 32개 보존. 미완료 요청은 성공 보존과 별개다.',ha='center',fontsize=8)
    fig.tight_layout(rect=(0,.13,1,.93));fig.savefig(OUT/'position-failover.png');plt.close(fig)
    (OUT/'sources.json').write_text(json.dumps({'run':str(RUN.relative_to(HERE)),
        'files':{name:hashlib.sha256((RUN/name).read_bytes()).hexdigest().upper() for name in ('summary.csv','position-summary.csv')},
        'commonConditions':288,'positionConditions':24,'timeDomain':'virtual'},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
