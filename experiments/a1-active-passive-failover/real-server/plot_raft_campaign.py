"""Create standalone PNG/SVG figures from run-level CSV; no AWS access."""
import argparse
import csv
import json
import os
from pathlib import Path
import statistics
import hashlib
import time


def render(root):
    os.environ.setdefault('MPLCONFIGDIR',str(root/'.plot-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator
    from raft_campaign_plan import DETECTORS
    with (root/'runs.csv').open() as file: rows=list(csv.DictReader(file))
    manifest=json.loads((root/'manifest.json').read_text())
    figures=root/'figures';figures.mkdir(exist_ok=True)
    colors=('#2563eb','#d97706','#059669')
    names=('Raft','Raft + SWIM','Raft + SWIM + Lifeguard')
    title='LOCAL IMPLEMENTATION VALIDATION' if manifest['backend']=='local' else 'EC2 measured results'
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    def draw(ax,scenarios,metric,label):
        labels=[]
        for group,scenario in enumerate(scenarios):
            labels.append(scenario.replace('cpu-contention-observer','observer CPU contention').replace('cpu-observer','observer pause'))
            for i,detector in enumerate(DETECTORS):
                planned=[r for r in rows if r['scenario']==scenario and r['detector']==detector]
                passed=[r for r in planned if r['status']=='passed' and r.get(metric) not in ('',None)]
                values=[float(r[metric]) for r in passed];x=group+(i-1)*.24
                if metric=='falseLeaderDeadEvents' and detector=='FIXED_TIMEOUT':
                    ax.text(x,1.01,'no FD',transform=ax.get_xaxis_transform(),ha='center',fontsize=7,color=colors[i])
                    continue
                if values:
                    jitter=[((j%7)-3)*.012 for j in range(len(values))]
                    ax.scatter([x+v for v in jitter],values,color=colors[i],alpha=.6,s=22)
                    median=statistics.median(values)
                    ax.plot([x-.07,x+.07],[median,median],color=colors[i],lw=3)
                failed=sum(r['status']=='failed' for r in planned);pending=sum(r['status']=='pending' for r in planned)
                ax.text(x,1.01,f'{len(values)}/{len(planned)}'+(' !' if failed or pending else ''),
                        transform=ax.get_xaxis_transform(),ha='center',fontsize=7,color=colors[i])
        ax.set_xticks(range(len(scenarios)),labels,rotation=15 if len(scenarios)>1 else 0)
        ax.set_ylabel(label);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
        ax.set_ylim(bottom=0)
        if metric in ('falseLeaderDeadEvents','electionStarts','leaderIdentityChanges'):
            ax.set_ylim(0,max(1,ax.get_ylim()[1]))
            ax.yaxis.set_major_locator(MaxNLocator(integer=True,min_n_ticks=2))
    legend=[Line2D([0],[0],color=c,marker='o',lw=0,label=n) for c,n in zip(colors,names)]
    def save(fig,name,caption):
        fig.suptitle(title+' | fixed Raft timeout 500 ms',fontsize=13)
        fig.legend(handles=legend,loc='lower center',ncol=3,bbox_to_anchor=(.5,.025))
        fig.text(.5,.005,caption,ha='center',fontsize=8)
        fig.tight_layout(rect=(0,.12,1,.94))
        fig.savefig(figures/(name+'.png'),dpi=180);fig.savefig(figures/(name+'.svg'));plt.close(fig)
    fig,ax=plt.subplots(figsize=(8,5))
    draw(ax,['crash'],'recoveryMs','First successful post-fault SET (ms; upper estimate)')
    save(fig,'recovery','Dots: independent runs; line: median. Top: available measurements / planned runs; ! = failed/pending.')
    scenarios=[s for s in ('cpu-observer','cpu-contention-observer','delay-udp','delay-both') if any(r['scenario']==s for r in rows)]
    if scenarios:
        fig,axes=plt.subplots(3,1,figsize=(12,10))
        for ax,key,label in zip(axes,('falseLeaderDeadEvents','electionStarts','leaderIdentityChanges'),
                               ('False original-leader DEAD events','Candidate election starts','Actual leader identity changes')):
            draw(ax,scenarios,key,label)
        save(fig,'stability','Live-process elections under replication delay are not automatically unnecessary. Missing values are not zeros.')
    fig,axes=plt.subplots(2,2,figsize=(11,8))
    for ax,key,label in zip(axes.flat,('normalP50Ms','normalP99Ms','replicationBytesPerSecond','fdBytesPerSecond'),
                           ('Per-run request p50 (ms)','Per-run request p99 (ms)','Raft application bytes / second','FD application bytes / second')):
        draw(ax,['healthy'],key,label)
    short=sum(r['scenario']=='healthy' and r.get('normalMinimumSamplesMet')!='True' for r in rows)
    save(fig,'healthy-cost',f'One paced client, not saturation throughput. Healthy runs below sample requirement or missing: {short}.')
    (figures/'README.md').write_text('Dots are run-level measurements; horizontal marks are medians.\n'
        'Failed/pending and insufficient-sample runs must be reviewed in report.md and runs.csv.\n'
        'SVG is editable; PNG is intended for slides. Local charts are not EC2 performance evidence.\n')
    (figures/'provenance.json').write_text(json.dumps(dict(matplotlib=matplotlib.__version__,generatedAtEpochNs=time.time_ns(),
        plotterSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        runsCsvSha256=hashlib.sha256((root/'runs.csv').read_bytes()).hexdigest()),indent=2)+'\n')
    report=root/'report.md'
    with report.open('a') as out:
        out.write('\n## Figures\n\n')
        for file in sorted(figures.glob('*.png')): out.write(f'![{file.stem}](figures/{file.name})\n\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('out',type=Path)
    render(parser.parse_args().out.resolve())
