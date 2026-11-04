"""참조 run의 summary.csv에서 해석 문서용 정적 그래프를 재생성한다."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager, ticker
import numpy as np


ROOT = Path(__file__).resolve().parent
RUN = "run-2026-10-23T06-14-41.093912600Z-30c42899"
VARIANTS = ("stop-and-copy", "pre-copy", "post-copy")
COLORS = ("#4477AA", "#EE7733", "#228833")
MARKERS = ("o", "s", "^")
FIELDS = {
    "blocked": ("쓰기 차단 최소 tick", "쓰기 차단 최대 tick", "쓰기 차단 (tick)", 1),
    "total": ("총 시간 최소 tick", "총 시간 최대 tick", "전체 이동 시간 (tick)", 1),
    "bytes": ("전송량 비율 최소", "전송량 비율 최대", "전송량 / 초기 데이터 (배)", 1),
    "reads": ("조회 BUSY 비율 최소", "조회 BUSY 비율 최대", "조회 BUSY 응답 (%)", 100),
}


def load(path: Path) -> dict:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 48:
        raise ValueError(f"참조 full CSV는 48행이어야 합니다: {len(rows)}")
    index = {}
    for row in rows:
        identity = row["조건 ID"], row["방식"]
        if identity in index or row["방식"] not in VARIANTS:
            raise ValueError(f"중복/알 수 없는 방식: {identity}")
        if int(row["seed 수"]) != 5 or int(row["정합성 위반"]) != 0:
            raise ValueError(f"검증되지 않은 결과: {identity}")
        for low, high, _, _ in FIELDS.values():
            a, b = float(row[low]), float(row[high])
            if not np.isfinite([a, b]).all() or a > b or a < 0:
                raise ValueError(f"잘못된 범위: {identity}")
        index[identity] = row
    return index


def values(index, cases, variant, metric):
    low, high, _, multiplier = FIELDS[metric]
    minimum = np.array([float(index[case, variant][low]) for case in cases]) * multiplier
    maximum = np.array([float(index[case, variant][high]) for case in cases]) * multiplier
    return minimum, maximum


def lines(ax, index, cases, xs, metric, *, log_y=False, annotate_last=False):
    for variant, color, marker in zip(VARIANTS, COLORS, MARKERS):
        low, high = values(index, cases, variant, metric)
        center = (low + high) / 2
        ax.plot(xs, center, color=color, marker=marker, linewidth=2, markersize=6, label=variant)
        ax.fill_between(xs, low, high, color=color, alpha=.18)
        ax.errorbar(xs, center, yerr=[center - low, high - center], fmt="none", ecolor=color, capsize=4)
        if annotate_last:
            text = f"{low[-1]:.2f}" if low[-1] == high[-1] else f"{low[-1]:.2f}~{high[-1]:.2f}"
            ax.annotate(text, (xs[-1], center[-1]), xytext=(-8, 8), textcoords="offset points",
                        ha="right", color=color, fontsize=11)
    ax.set_ylabel(FIELDS[metric][2])
    if log_y:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda value, _: f"{value:g}"))
    else:
        ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=.24)
    ax.spines[["top", "right"]].set_visible(False)
    ax.margins(x=.09, y=.20)


def finish(fig, output: Path, title: str, note: str):
    handles, labels = fig.axes[0].get_legend_handles_labels()
    fig.suptitle(title, fontsize=16, fontweight="bold", y=.985)
    legend = fig.legend(handles, labels, ncol=3, loc="lower center", bbox_to_anchor=(.5, .055), frameon=False)
    legend.set_in_layout(False)
    fig.text(.5, .016, note, ha="center", fontsize=10)
    fig.tight_layout(rect=(0, .12, 1, .94), h_pad=2.4, w_pad=1.9)
    fig.savefig(output, dpi=180, facecolor="white")
    plt.close(fig)


def size_figure(index, output):
    sizes = [10_000, 100_000, 1_000_000]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8))
    for col, (scenario, rho) in enumerate((("S1", 0), ("S2", .1))):
        cases = [f"{scenario}-n{n}" for n in sizes]
        for row, metric in enumerate(("blocked", "total")):
            ax = axes[row, col]
            lines(ax, index, cases, sizes, metric, log_y=True)
            ax.set_xscale("log")
            ax.set_xticks(sizes, ["1만", "10만", "100만"])
            ax.set_xlabel("초기 key 수 (로그 축)")
            ax.set_title(f"{scenario} · 쓰기 부하 ρ={rho:g}")
    finish(fig, output, "데이터 크기에 따른 쓰기 차단과 전체 이동 시간",
           "가상 시간 · B=64 KiB/tick · 두 축 로그 눈금 · 선=seed 범위 중앙 · 음영/오차막대=seed 5개 최솟값~최댓값")


def load_figure(index, output):
    rhos = [.01, .1, .5, .9, 1.2]
    cases = [f"S3-rho{rho}" for rho in rhos]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8))
    for ax, metric in zip(axes.flat, FIELDS):
        lines(ax, index, cases, rhos, metric)
        ax.set_xticks([.01, .5, .9, 1.2])
        ax.set_xlabel("쓰기 부하 ρ")
        ax.set_title(FIELDS[metric][2])
    finish(fig, output, "S3 · 쓰기 부하가 커질 때 발생하는 비용",
           "10만 key · 균등 분포 · DELETE 30% · 선=seed 범위 중앙(평균 아님) · 음영/오차막대=최솟값~최댓값")


def post_figure(index, output):
    cases = ["S1-n100000", "S3-rho0.5"]
    x = np.arange(len(cases))
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.9))
    for ax, metric in zip(axes, ("blocked", "total", "reads")):
        for i, (variant, color) in enumerate(zip(VARIANTS, COLORS)):
            low, high = values(index, cases, variant, metric)
            center = (low + high) / 2
            positions = x + (i - 1) * .24
            ax.bar(positions, center, width=.21, color=color, label=variant)
            ax.errorbar(positions, center, yerr=[center-low, high-center], fmt="none",
                        ecolor="#333333", capsize=3)
            for pos, mid, lo, hi in zip(positions, center, low, high):
                text = f"{mid:.0f}" if metric != "reads" else f"{mid:.1f}"
                if metric == "blocked" and lo != hi:
                    text = f"{lo:.0f}~{hi:.0f}"
                ax.annotate(text, (pos, hi), xytext=(0, 5), textcoords="offset points",
                            ha="center", fontsize=10)
        if metric == "blocked":
            ax.set_yscale("log")
            ax.set_ylim(.7, 500)
        else:
            ax.set_ylim(bottom=0, top=ax.get_ylim()[1]*1.22)
        ax.set_xticks(x, ["쓰기 없음", "ρ=0.5"])
        ax.set_ylabel(FIELDS[metric][2])
        ax.set_title(("쓰기 차단 · 로그 축" if metric == "blocked" else FIELDS[metric][2]))
        ax.grid(axis="y", alpha=.24)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
    finish(fig, output, "post-copy · 쓰기 재개와 조회 가용성은 별도",
           "10만 key · 균등 분포 · 막대=seed 범위 중앙 · 오차막대=최솟값~최댓값 · BUSY는 실제 재시도 횟수가 아닌 응답 비율")


def skew_figure(index, output):
    theta = [0, .99, 1.2]
    cases = ["S4-theta0.0", "S4-theta0.99", "S4-theta1.2"]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.9))
    for ax, metric in zip(axes, ("bytes", "total", "reads")):
        lines(ax, index, cases, np.arange(3), metric)
        ax.set_xticks(range(3), ["균등\nθ=0", "Zipf\nθ=0.99", "Zipf\nθ=1.2"])
        ax.set_xlabel("요청 분포 (조건 간격 동일 표시)")
        ax.set_title(FIELDS[metric][2])
    finish(fig, output, "S4 · 같은 key에 요청이 몰릴 때 달라지는 비용",
           "10만 key · ρ=0.5 · DELETE 30% · 선=seed 범위 중앙 · 음영/오차막대=최솟값~최댓값")


def delete_figure(index, output):
    cases = ["S5-delete0", "S5-delete30"]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.9))
    for ax, metric in zip(axes, ("bytes", "total", "blocked")):
        lines(ax, index, cases, [0, 30], metric, log_y=metric == "blocked")
        ax.set_xticks([0, 30], ["0%", "30%"])
        ax.set_xlabel("쓰기 시도 중 DELETE 비율")
        ax.set_title("쓰기 차단 · 로그 축" if metric == "blocked" else FIELDS[metric][2])
    finish(fig, output, "S5 · DELETE 비율에 따른 복사 비용",
           "10만 key · 균등 분포 · ρ=0.5(시도당 152B 기준) · 선=seed 범위 중앙 · 음영/오차막대=최솟값~최댓값")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, default=ROOT / "runs" / RUN / "repeat-001" / "summary.csv")
    parser.add_argument("--out", type=Path, default=ROOT / "figures")
    args = parser.parse_args()
    available = {font.name for font in font_manager.fontManager.ttflist}
    font = next((name for name in ("Malgun Gothic", "Noto Sans CJK KR", "NanumGothic") if name in available), None)
    if font is None:
        raise RuntimeError("한글 폰트가 필요합니다: Malgun Gothic / Noto Sans CJK KR / NanumGothic")
    plt.rcParams.update({"font.family": font, "font.size": 12, "axes.titlesize": 12,
                         "axes.labelsize": 12, "xtick.labelsize": 11, "ytick.labelsize": 11,
                         "legend.fontsize": 11, "axes.unicode_minus": False})
    index = load(args.summary)
    args.out.mkdir(parents=True, exist_ok=True)
    charts = [
        ("s1-s2-size.png", size_figure),
        ("s3-write-load.png", load_figure),
        ("post-copy-tradeoff.png", post_figure),
        ("s4-request-skew.png", skew_figure),
        ("s5-delete-ratio.png", delete_figure),
    ]
    manifest = {"sourceRun": RUN, "sourceSummarySha256": hashlib.sha256(args.summary.read_bytes()).hexdigest(),
                "range": "seed 5개의 min/max; 중앙값은 (min+max)/2이며 seed 평균/중앙값 통계가 아님",
                "figures": []}
    for name, plot in charts:
        path = args.out / name
        plot(index, path)
        manifest["figures"].append({"file": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        print(path)
    (args.out / "sources.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
