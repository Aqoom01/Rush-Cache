"""Rosé 기반 단일 파티션 변형의 L별 trade-off와 lag 시계열을 그린다."""
import csv
import hashlib
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt, font_manager

HERE=Path(__file__).resolve().parent
RUN=HERE/'runs/run-2026-10-29T01-03-29.244815600Z-bee0648a/repeat-001'
OUT=HERE/'figures'


def read(name):
    with (RUN/name).open(encoding='utf-8',newline='') as file:
        return list(csv.DictReader(file))


def main():
    rows=read('summary.csv'); samples=read('samples.csv')
    assert len(rows)==240
    assert all(int(r['lostPreCrash'])<=int(r['limit']) for r in rows if int(r['limit'])>=0)
    font=Path('C:/Windows/Fonts/malgun.ttf')
    if font.exists():
        font_manager.fontManager.addfont(str(font)); plt.rcParams['font.family']='Malgun Gothic'
    plt.rcParams.update({'font.size':10,'figure.dpi':160,'axes.unicode_minus':False})
    OUT.mkdir(exist_ok=True)
    limits=(0,1,10,100,1000,-1); labels=('0\n동기','1','10','100','1000','∞\n비동기')
    def selected(scenario):
        return [next(r for r in rows if int(r['limit'])==limit and r['scenario']==scenario and r['intervalMs']=='1' and r['valueBytes']=='8192' and r['mibps']=='1') for limit in limits]
    normal=selected('NORMAL'); fault=selected('ISOLATED_CRASH')
    series=([int(r['ok'])/int(r['offered'])*100 for r in normal],[int(r['lostPreCrash']) for r in fault],[int(r['drainNs'])/1e9 for r in normal])
    titles=('정상 상태: 제출 2,000건 중 성공률','1초 격리 → 1.2초 crash: 성공 쓰기 유실','2초 입력 종료 뒤: 전체 수렴까지 추가 시간')
    units=('성공률 (%)','crash 전 OK 중 유실 (건)','추가 drain (가상 초)')
    fig,axes=plt.subplots(1,3,figsize=(13,5))
    for ax,data,title,unit in zip(axes,series,titles,units):
        ax.bar(range(6),data,color=['#597ba4']+['#43937f']*4+['#d78c3b'])
        for x,v in enumerate(data): ax.text(x,v+max(data)*.03,f'{v:.1f}' if unit!='crash 전 OK 중 유실 (건)' else f'{int(v):,}',ha='center',fontsize=9)
        ax.set_xticks(range(6),labels);ax.set_xlabel('L (미복제 op 상한)');ax.set_ylabel(unit);ax.set_title(title,fontsize=10)
        ax.set_ylim(0,max(data)*1.2 if max(data)>0 else 1);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle('Rosé 기반 변형: 성공 응답·유실·미처리 복제의 trade-off',fontsize=15)
    fig.text(.5,.035,'단일 파티션 · 값 8KiB · 1MiB/s · 1ms 간격. 서로 다른 시나리오를 목적별로 구분했으며 실제 서버 절대 성능이 아니다.',ha='center',fontsize=9)
    fig.text(.5,.005,'L=0은 비교용 동기 모드, ∞는 무제한 비동기다. 성공 지연만으로 BUSY와 미래 drain 비용을 숨기지 않는다.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.12,1,.91));fig.savefig(OUT/'loss-tradeoff.png');plt.close(fig)

    fig,axes=plt.subplots(1,2,figsize=(12,5.5),sharey=True)
    colors={0:'#597ba4',10:'#42937f',100:'#875bac',1000:'#bf6543',-1:'#d7a535'}
    for ax,scenario in zip(axes,('SLOW_BACKUP','ISOLATED_CRASH')):
        for limit in (0,10,100,1000,-1):
            chosen=[r for r in samples if int(r['limit'])==limit and r['scenario']==scenario and r['intervalMs']=='1' and r['valueBytes']=='8192' and r['mibps']=='1' and int(r['timeNs'])<=2_200_000_000]
            ax.plot([int(r['timeNs'])/1e9 for r in chosen],[float('nan') if int(r['lag'])<0 else int(r['lag']) for r in chosen],label='L='+('∞' if limit<0 else str(limit)),color=colors[limit])
        if scenario=='SLOW_BACKUP':
            ax.axvspan(.5,1.5,color='#ddd',alpha=.4);ax.set_title('0.5~1.5초: Backup 1 Append +200ms')
        else:
            ax.axvspan(1,1.2,color='#ddd',alpha=.4);ax.axvline(1.2,color='#b84e4e',linestyle='--');ax.set_title('1초 Primary 격리, 1.2초 영구 crash')
        ax.axvline(2,color='#999',linestyle=':');ax.set_xlabel('가상 시간 (초)');ax.grid(alpha=.2);ax.set_yscale('symlog',linthresh=1);ax.set_xlim(0,2.2)
    axes[0].set_ylabel('예약 로그 길이 - 최소 Backup 적용 ACK (op)')
    handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,.04),ncol=5)
    fig.suptitle('L별 lag 시계열: 상한을 지키는 대신 수락을 제한',fontsize=15)
    fig.text(.5,.01,'전환 중 lag 미정(-1)은 선을 끊었다. 2초 이후 새 입력은 없으며 로그를 줄여 버린 것이 아니라 복제를 drain한다.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.13,1,.92));fig.savefig(OUT/'lag-series.png');plt.close(fig)
    (OUT/'sources.json').write_text(json.dumps({'run':str(RUN.relative_to(HERE)),'files':{name:hashlib.sha256((RUN/name).read_bytes()).hexdigest().upper() for name in ('summary.csv','samples.csv')},
        'conditions':240,'timeDomain':'virtual','variant':'ROSE_ADAPTATION'},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
