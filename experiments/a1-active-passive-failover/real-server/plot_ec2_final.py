"""Plot the validated EC2 cohort summaries with medians and observed ranges."""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
from matplotlib import pyplot as plt


WRITE_VARIANTS = (
    ('PB', 'INCREMENTAL', '', 'PB', '#26745d'),
    ('VR', 'INCREMENTAL', '', 'VR incremental', '#315f9a'),
    ('VR', 'INCREMENTAL_CATCHUP', '', 'VR catch-up', '#9a4b35'),
    ('RAFT', 'INCREMENTAL', '', 'Raft', '#80539b'),
)
READ_VARIANTS = (
    ('CHAIN', 'TAIL_ONLY', 'tail', 'Chain tail', '#315f9a'),
    ('CHAIN', 'TAIL_ONLY', 'spread', 'Chain redirect', '#9a4b35'),
    ('CHAIN', 'CRAQ', 'spread', 'CRAQ spread', '#26745d'),
)


def plot(source: Path, destination: Path, cohort: str) -> None:
    with source.open(encoding='utf-8', newline='') as stream:
        rows = list(csv.DictReader(stream))
    variants = READ_VARIANTS if cohort == 'read' else WRITE_VARIANTS
    clients = (1,) if cohort == 'crash' else (1, 4, 8)
    indexed = {(row['protocol'], row['mode'], row['target'], int(row['value']),
                int(row['clients'])): row for row in rows}
    assert len(indexed) == len(rows) == len(variants) * len(clients) * 2
    assert all(row['scenario'] == cohort and row['repeats'] == '5' for row in rows)

    metrics = (('failoverMs', 'First successful write after crash (ms)'),) if cohort == 'crash' else (
        ('opsPerSecond', 'Successful operations / s'), ('p99Ms', 'Per-run p99 latency (ms)'))
    titles = {'crash': 'Leader crash while a write is in flight',
              'write': 'Write throughput and latency in bounded windows',
              'read': 'Clean read throughput and latency'}
    fig, axes = plt.subplots(len(metrics), 2, figsize=(11, 3.5 * len(metrics) + 1.0),
                             squeeze=False, layout='constrained')
    for metric_index, (metric, ylabel) in enumerate(metrics):
        for value_index, value in enumerate((64, 8192)):
            ax = axes[metric_index][value_index]
            for index, (protocol, mode, target, label, color) in enumerate(variants):
                points = [indexed[(protocol, mode, target, value, count)] for count in clients]
                med = [float(row[f'{metric}_median']) for row in points]
                lo = [m - float(row[f'{metric}_min']) for m, row in zip(med, points)]
                hi = [float(row[f'{metric}_max']) - m for m, row in zip(med, points)]
                positions = [index] if cohort == 'crash' else [i + (index - (len(variants) - 1) / 2) * .045 for i in range(len(clients))]
                ax.errorbar(positions, med, yerr=(lo, hi), marker='o', markersize=5,
                            color=color, capsize=3, label=label)
            ax.set_title('64 B' if value == 64 else '8 KiB')
            ax.set_ylabel(ylabel)
            if cohort == 'crash':
                ax.set_xticks(range(len(variants)), [v[3] for v in variants], rotation=16)
            else:
                ax.set_xticks(range(len(clients)), clients)
                ax.set_xlabel('Concurrent clients')
            ax.set_ylim(bottom=0)
            ax.grid(axis='y', alpha=.24)
    if cohort != 'crash':
        handles, labels = axes[0][0].get_legend_handles_labels()
        fig.legend(handles, labels, loc='outside lower center', ncols=len(variants), frameon=False)
    fig.suptitle(titles[cohort] + '\nMedian and observed min–max across five EC2 runs per condition')
    fig.savefig(destination, dpi=170)
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cohort', choices=('crash', 'write', 'read'))
    parser.add_argument('summary', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    plot(args.summary, args.output, args.cohort)
