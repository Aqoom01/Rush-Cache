"""저장된 CSV에서 비용 비교 그림을 재생성한다. Java 재실행은 필요 없다."""

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

FOLDER = Path(__file__).resolve().parent
ROOT = FOLDER.parents[2]
CSV = FOLDER / "runs/run-2026-10-28T01-36-54.664991200Z-d71e8b8d/repeat-001/summary.csv"
OUTPUT = FOLDER / "figures"


def main() -> None:
    font = Path("C:/Windows/Fonts/malgun.ttf")
    if font.exists():
        font_manager.fontManager.addfont(str(font))
        plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams.update({"axes.unicode_minus": False, "font.size": 10, "figure.dpi": 160})
    with CSV.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))
    assert len(rows) == 216
    lookup = {
        (r["variant"], r["scenario"], int(r["history"]), int(r["valueBytes"]), int(r["mibps"]), int(r["timeoutMs"])): r
        for r in rows
    }
    assert len(lookup) == 216
    OUTPUT.mkdir(exist_ok=True)

    fig, axes = plt.subplots(1, 3, figsize=(11.5, 4.3), sharey=True)
    for axis, bandwidth in zip(axes, (1, 10, 100)):
        for variant, color, marker, label in (
            ("PB", "#2563a6", "o", "PB (전원 ACK)"),
            ("VR", "#d97706", "s", "VR (현재 전체 로그 구현)"),
        ):
            selected = [lookup[(variant, "NORMAL", history, 8192, bandwidth, 2000)] for history in (1, 16, 64)]
            assert all(r["outcome"] == "OK" for r in selected)
            values = [int(r["responseNs"]) / 1_000_000 for r in selected]
            axis.plot(range(3), values, color=color, marker=marker, label=label, linewidth=2)
            for x, value in enumerate(values):
                axis.annotate(f"{value:.2f}", (x, value), textcoords="offset points", xytext=(0, 7 if variant == "VR" else -15), ha="center", fontsize=8)
        axis.set_title(f"방향별 {bandwidth} MiB/s")
        axis.set_xticks(range(3), ("1", "16", "64"))
        axis.set_xlabel("준비된 성공 연산 수")
        axis.set_xlim(-0.2, 2.2)
        axis.set_yscale("log")
        axis.set_ylim(1.1, 1000)
        axis.grid(axis="y", alpha=0.2)
    axes[0].set_ylabel("쓰기 응답 (가상 ms, 로그 축)")
    axes[0].legend(loc="upper left", fontsize=8)
    fig.suptitle("값 8KiB · 감지 timeout 2,000ms: 전체 로그 전송 비용", y=0.98)
    fig.text(0.5, 0.025, "고정 모델의 단일 결과. 선은 측정점 연결이며 통계적 추정이나 실제 서버 성능이 아니다.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.075, 1, 0.94))
    fig.savefig(OUTPUT / "normal-write-cost.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 3, figsize=(11.5, 6.6))
    for row_index, timeout in enumerate((100, 2000)):
        for column_index, bandwidth in enumerate((1, 10, 100)):
            axis = axes[row_index][column_index]
            selected = [[lookup[("VR", "CRASH", history, size, bandwidth, timeout)] for size in (64, 1024, 8192)] for history in (1, 16, 64)]
            cells = [[0 if item["outcome"] == "OK" else 1 for item in row] for row in selected]
            assert all(item["outcome"] in ("OK", "CENSORED") for row in selected for item in row)
            axis.imshow(cells, cmap=ListedColormap(["#c9e8dc", "#f7c99b"]), vmin=0, vmax=1, aspect="auto")
            for y, row in enumerate(selected):
                for x, item in enumerate(row):
                    label = f"{int(item['recoveryNs']) / 1_000_000:.0f} ms" if item["outcome"] == "OK" else "5s 내 미완료"
                    axis.text(x, y, label, ha="center", va="center", fontsize=8)
            axis.set_xticks(range(3), ("64B", "1KiB", "8KiB"))
            axis.set_yticks(range(3), ("1", "16", "64"))
            axis.set_title(f"{bandwidth} MiB/s · timeout {timeout}ms", fontsize=10)
            axis.set_xlabel("값 크기")
            if column_index == 0:
                axis.set_ylabel("준비된 성공 연산 수")
    fig.suptitle("현재 VR 구현의 crash 후 첫 쓰기 성공: 54조건", y=0.99)
    fig.legend(handles=[Patch(color="#c9e8dc", label="첫 OK 관측"), Patch(color="#f7c99b", label="관측 상한까지 미완료")], loc="lower center", bbox_to_anchor=(0.5, 0.035), ncol=2, fontsize=9)
    fig.text(0.5, 0.012, "숫자는 가상 ms. 미완료 칸에 복구 시간 5,000ms를 대입하지 않는다.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.09, 1, 0.95))
    fig.savefig(OUTPUT / "vr-crash-outcomes.png")
    plt.close(fig)

    files = [CSV, Path(__file__).resolve(), *sorted(OUTPUT.glob("*.png"))]
    sources = {
        "model": "logical-link-fifo-1",
        "sha256": {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
    }
    (OUTPUT / "sources.json").write_text(json.dumps(sources, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
