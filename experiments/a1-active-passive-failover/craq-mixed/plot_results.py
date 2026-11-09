"""검증된 혼합 부하 CSV에서 멘토링용 그래프와 대표 조건 표를 생성한다."""
import csv
import hashlib
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt, font_manager

HERE = Path(__file__).resolve().parent
PCTS = (0, 1, 5, 10, 25, 50)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("plot_results.py <repeat-001 directory>")
    run = Path(sys.argv[1]).resolve()
    with (run / "summary.csv").open(encoding="utf-8", newline="") as stream:
        rows = [{k: v if k == "mode" else int(v) for k, v in r.items()} for r in csv.DictReader(stream)]
    assert len(rows) == 288
    identity = lambda r: tuple(r[k] for k in ("mode", "writePercent", "keys", "valueBytes", "ackDelayMs", "seed"))
    assert len({identity(r) for r in rows}) == len(rows)
    assert all(r["invalidReads"] == r["pending"] == 0 and r["correctReads"] == r["readOk"] for r in rows)
    font = Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams.update({"figure.dpi": 160, "font.size": 10, "axes.unicode_minus": False})
    out = HERE / "figures"
    out.mkdir(exist_ok=True)

    def select(**filters):
        result = [r for r in rows if all(r[k] == v for k, v in filters.items())]
        assert len(result) == 3 and {r["seed"] for r in result} == {1, 2, 3}
        return result

    def fraction(group, numerator, denominator):
        total = sum(r[denominator] for r in group)
        return 100 * sum(r[numerator] for r in group) / total if total else 0

    fig, axes = plt.subplots(2, 2, figsize=(11.5, 7), sharex=True, sharey=True)
    for y, ack in enumerate((0, 20)):
        for x, keys in enumerate((1, 64)):
            ax = axes[y, x]
            for size, color in ((64, "#4379b5"), (8192, "#d58639")):
                groups = [select(mode="CRAQ", keys=keys, valueBytes=size, ackDelayMs=ack, writePercent=p) for p in PCTS]
                avg = [fraction(g, "localReads", "readOk") for g in groups]
                lower = [min(100 * r["localReads"] / r["readOk"] for r in g) for g in groups]
                upper = [max(100 * r["localReads"] / r["readOk"] for r in g) for g in groups]
                ax.plot(range(6), avg, "o-", color=color, label="64 B" if size == 64 else "8 KiB")
                ax.fill_between(range(6), lower, upper, color=color, alpha=.13)
            ax.set_title(f'key {keys}개 · Tail 확인 추가 지연 {ack}ms')
            ax.set_xticks(range(6), PCTS)
            ax.set_ylim(0, 105)
            ax.grid(alpha=.2)
    axes[0, 0].set_ylabel("로컬 응답 / 성공 읽기 (%)")
    axes[1, 0].set_ylabel("로컬 응답 / 성공 읽기 (%)")
    for ax in axes[1]:
        ax.set_xlabel("제출 요청의 쓰기 비율 (%)")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(.5, .035), ncol=2)
    fig.suptitle("CRAQ: 쓰기 비율보다 읽기·쓰기의 key 충돌 범위도 중요", fontsize=15)
    fig.text(.5, .012, "3개 seed의 합산 비율, 음영은 seed별 범위. 1MiB/s · 1ms 전파 · 500요청/s · 단일 in-flight 쓰기.", ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .09, 1, .94))
    fig.savefig(out / "local-read-ratio.png")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11, 5), sharey=True)
    for ax, mode in zip(axes, ("TAIL_ONLY", "CRAQ")):
        groups = [select(mode=mode, keys=1, valueBytes=8192, ackDelayMs=20, writePercent=p) for p in PCTS[1:]]
        ok = [fraction(g, "writeOk", "writeOffered") for g in groups]
        busy = [fraction(g, "writeBusy", "writeOffered") for g in groups]
        timeout = [fraction(g, "writeTimeout", "writeOffered") for g in groups]
        ax.bar(range(5), ok, label="OK", color="#43937c")
        ax.bar(range(5), busy, bottom=ok, label="BUSY", color="#d2a04a")
        ax.bar(range(5), timeout, bottom=[a+b for a, b in zip(ok, busy)], label="TIMEOUT", color="#bd6262")
        for i, value in enumerate(ok):
            ax.text(i, value/2, f'{value:.1f}%', ha="center", va="center", fontsize=9)
        ax.set_xticks(range(5), PCTS[1:])
        ax.set_title("Chain" if mode == "TAIL_ONLY" else "CRAQ")
        ax.set_xlabel("제출 요청의 쓰기 비율 (%)")
        ax.grid(axis="y", alpha=.2)
        ax.set_axisbelow(True)
        ax.set_ylim(0, 102)
    axes[0].set_ylabel("제출 쓰기의 결과 비율 (%)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(.5, .055), ncol=3)
    fig.suptitle("쓰기 비율을 높여도 실제 성공 쓰기가 같은 비율로 늘지는 않음", fontsize=14)
    fig.text(.5, .012, "8KiB · key 1개 · Tail Ack/Completed +20ms · seed 3개 합산. 기존 단일 in-flight 입장 제한의 영향 포함.", ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .13, 1, .91))
    fig.savefig(out / "write-outcomes.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    groups = [select(mode="CRAQ", keys=1, valueBytes=8192, ackDelayMs=20, writePercent=p) for p in PCTS]
    clients = [fraction(g, "node2Reads", "readOffered") for g in groups]
    queries = [fraction(g, "queryAttempts", "readOffered") for g in groups]
    ax.bar(range(6), clients, label="CRAQ: Tail client 읽기", color="#5795bd")
    ax.bar(range(6), queries, bottom=clients, label="CRAQ: Tail 버전 조회", color="#d89b48")
    ax.axhline(100, linestyle="--", color="#555", label="Chain: Tail client 읽기 100건")
    ax.set_xticks(range(6), PCTS)
    ax.set_xlabel("제출 요청의 쓰기 비율 (%)")
    ax.set_ylabel("전체 읽기 100건당 Tail 요청 수")
    ax.set_ylim(0, 110)
    ax.set_title("CRAQ의 로컬 읽기가 줄면 Tail 조회 비용이 추가됨", fontsize=14, pad=15)
    ax.grid(axis="y", alpha=.2)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", bbox_to_anchor=(0, -.17), fontsize=9)
    fig.text(.5, .015, "8KiB · key 1개 · 확인 +20ms · seed 합산. 값 읽기와 버전 조회는 비용이 다르므로 요청 수를 CPU 비용으로 환산하지 않음.", ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .1, 1, 1))
    fig.savefig(out / "tail-requests.png")
    plt.close(fig)

    lines = ["# CRAQ 혼합 부하 대표 조건\n", "8KiB · 1MiB/s · 전파 1ms · Tail Ack/Completed +20ms · seed 1/2/3 합산.\n",
             "|제출 쓰기 %|key 수|로컬 읽기 %|dirty 읽기 %|쓰기 성공/제출|쓰기 BUSY|읽기 검증 위반|",
             "|---:|---:|---:|---:|---:|---:|---:|"]
    for p in PCTS:
        for keys in (1, 64):
            g = select(mode="CRAQ", keys=keys, valueBytes=8192, ackDelayMs=20, writePercent=p)
            lines.append(f'|{p}|{keys}|{fraction(g,"localReads","readOk"):.2f}|{fraction(g,"dirtyReads","readOk"):.2f}|'
                         f'{sum(r["writeOk"] for r in g)}/{sum(r["writeOffered"] for r in g)}|'
                         f'{sum(r["writeBusy"] for r in g)}|{sum(r["invalidReads"] for r in g)}|')
    (HERE / "representative-results.md").write_text("\n".join(lines)+"\n", encoding="utf-8")
    metadata = {"source": str((run / "summary.csv").relative_to(HERE)),
                "sha256": hashlib.sha256((run / "summary.csv").read_bytes()).hexdigest(), "conditions": len(rows),
                "scope": "virtual read routing and peer cost; no CPU/client value response model"}
    (out / "sources.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print("Wrote 3 figures, representative-results.md and sources.json")


if __name__ == "__main__":
    main()
