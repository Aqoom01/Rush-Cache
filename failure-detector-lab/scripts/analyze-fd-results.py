#!/usr/bin/env python3
"""Generate dependency-free PNG charts and a report from benchmark CSV files."""

from __future__ import annotations

import argparse
import csv
import struct
import zlib
from pathlib import Path


ALGORITHMS = [
    "swim-lifeguard",
    "redis-cluster",
    "phi-gossip",
    "ignite-tcp-ring",
    "aerospike-mesh",
]
COLORS = [
    (52, 101, 164),
    (204, 0, 0),
    (117, 80, 123),
    (78, 154, 6),
    (245, 121, 0),
]


class Canvas:
    def __init__(self, width: int = 1000, height: int = 600) -> None:
        self.width = width
        self.height = height
        self.data = bytearray([255] * width * height * 3)

    def pixel(self, x: int, y: int, color: tuple[int, int, int]) -> None:
        if 0 <= x < self.width and 0 <= y < self.height:
            offset = (y * self.width + x) * 3
            self.data[offset:offset + 3] = bytes(color)

    def rect(self, x: int, y: int, width: int, height: int, color: tuple[int, int, int]) -> None:
        for row in range(max(0, y), min(self.height, y + max(0, height))):
            start = (row * self.width + max(0, x)) * 3
            end = (row * self.width + min(self.width, x + max(0, width))) * 3
            if end > start:
                self.data[start:end] = bytes(color) * ((end - start) // 3)

    def line(self, x1: int, y1: int, x2: int, y2: int, color: tuple[int, int, int]) -> None:
        dx, dy = abs(x2 - x1), -abs(y2 - y1)
        sx, sy = (1 if x1 < x2 else -1), (1 if y1 < y2 else -1)
        error = dx + dy
        while True:
            self.pixel(x1, y1, color)
            if x1 == x2 and y1 == y2:
                return
            twice = 2 * error
            if twice >= dy:
                error += dy
                x1 += sx
            if twice <= dx:
                error += dx
                y1 += sy

    def circle(self, cx: int, cy: int, radius: int, color: tuple[int, int, int]) -> None:
        for y in range(cy - radius, cy + radius + 1):
            for x in range(cx - radius, cx + radius + 1):
                if (x - cx) ** 2 + (y - cy) ** 2 <= radius ** 2:
                    self.pixel(x, y, color)

    def axes(self) -> None:
        self.line(80, 520, 950, 520, (40, 40, 40))
        self.line(80, 50, 80, 520, (40, 40, 40))
        for index in range(1, 5):
            y = 520 - index * 94
            self.line(80, y, 950, y, (225, 225, 225))

    def save(self, path: Path, title: str) -> None:
        raw = b"".join(
            b"\x00" + bytes(self.data[row * self.width * 3:(row + 1) * self.width * 3])
            for row in range(self.height)
        )

        def chunk(kind: bytes, value: bytes) -> bytes:
            return (struct.pack(">I", len(value)) + kind + value
                    + struct.pack(">I", zlib.crc32(kind + value) & 0xFFFFFFFF))

        png = b"\x89PNG\r\n\x1a\n"
        png += chunk(b"IHDR", struct.pack(">IIBBBBB", self.width, self.height, 8, 2, 0, 0, 0))
        png += chunk(b"tEXt", b"Title\x00" + title.encode("latin-1", "replace"))
        png += chunk(b"IDAT", zlib.compress(raw, 9))
        png += chunk(b"IEND", b"")
        path.write_bytes(png)


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def number(value: str | None) -> float | None:
    return None if value in (None, "") else float(value)


def lookup(data: list[dict[str, str]], algorithm: str, scenario: str) -> dict[str, str] | None:
    return next((row for row in data if row["algorithm"] == algorithm and row["scenario"] == scenario), None)


def scaled_height(value: float, maximum: float, height: int = 440) -> int:
    return 0 if maximum <= 0 else max(1, round(value / maximum * height))


def grouped_bars(series: list[list[float]], colors: list[tuple[int, int, int]], path: Path, title: str) -> None:
    canvas = Canvas()
    canvas.axes()
    maximum = max((value for group in series for value in group), default=1) or 1
    group_width = 150
    bar_width = max(12, 100 // max(1, len(series)))
    for algorithm_index in range(len(ALGORITHMS)):
        center = 150 + algorithm_index * group_width
        for series_index, values in enumerate(series):
            height = scaled_height(values[algorithm_index], maximum)
            x = center + (series_index - len(series) / 2) * bar_width
            canvas.rect(int(x), 520 - height, bar_width - 2, height, colors[series_index])
        canvas.rect(center - 10, 540, 20, 20, COLORS[algorithm_index])
    canvas.save(path, title)


def detection_chart(data: list[dict[str, str]], output: Path) -> None:
    p50 = [number((lookup(data, algorithm, "crash") or {}).get("p50_ms")) or 0 for algorithm in ALGORITHMS]
    p95 = [number((lookup(data, algorithm, "crash") or {}).get("p95_ms")) or 0 for algorithm in ALGORITHMS]
    grouped_bars([p50, p95], [(114, 159, 207), (52, 101, 164)],
                 output / "detection_latency.png", "Crash detection latency: p50 light, p95 dark")


def false_positive_chart(data: list[dict[str, str]], output: Path) -> None:
    scenarios = ["normal", "loss15", "loss45"]
    values = [[float((lookup(data, algorithm, scenario) or {}).get("false_unavailable_events", 0))
               for algorithm in ALGORITHMS] for scenario in scenarios]
    grouped_bars(values, [(138, 226, 52), (252, 175, 62), (204, 0, 0)],
                 output / "false_positive_by_loss.png", "False unavailable: normal green, loss15 amber, loss45 red")


def cost_latency_chart(detection: list[dict[str, str]], network: list[dict[str, str]], output: Path) -> None:
    canvas = Canvas()
    canvas.axes()
    points: list[tuple[float, float, int]] = []
    for index, algorithm in enumerate(ALGORITHMS):
        latency = number((lookup(detection, algorithm, "crash") or {}).get("p95_ms"))
        cost = number((lookup(network, algorithm, "crash") or {}).get("bytes_per_node_second_mean"))
        if latency is not None and cost is not None:
            points.append((cost, latency, index))
    max_x = max((point[0] for point in points), default=1) or 1
    max_y = max((point[1] for point in points), default=1) or 1
    for cost, latency, index in points:
        x = 80 + round(cost / max_x * 850)
        y = 520 - round(latency / max_y * 440)
        canvas.circle(x, y, 10, COLORS[index])
    canvas.save(output / "latency_vs_network_cost.png",
                "X modeled bytes/node/s; Y detection p95; colors follow algorithm order")


def coverage_chart(data: list[dict[str, str]], output: Path) -> None:
    canvas = Canvas(700, 600)
    scenarios = ["consecutive-3", "consecutive-4"]
    for row, algorithm in enumerate(ALGORITHMS):
        for column, scenario in enumerate(scenarios):
            value = float((lookup(data, algorithm, scenario) or {}).get("success_rate", 0))
            color = (round(204 * (1 - value)), round(190 * value), 40)
            canvas.rect(170 + column * 200, 50 + row * 100, 180, 80, color)
        canvas.rect(20, 80 + row * 100, 30, 30, COLORS[row])
    canvas.save(output / "consecutive_crash_coverage.png",
                "Rows algorithm order; columns consecutive-3 and consecutive-4; green is success")


def membership_chart(data: list[dict[str, str]], output: Path) -> None:
    p50 = [number((lookup(data, algorithm, "crash") or {}).get("p50_ms")) or 0 for algorithm in ALGORITHMS]
    p95 = [number((lookup(data, algorithm, "crash") or {}).get("p95_ms")) or 0 for algorithm in ALGORITHMS]
    grouped_bars([p50, p95], [(143, 240, 164), (78, 154, 6)],
                 output / "membership_convergence.png", "Membership convergence: p50 light, p95 dark; zero is N/A")


def write_report(comparison: list[dict[str, str]], output: Path) -> None:
    eligible = [row for row in comparison if number(row["crash_success_rate"]) == 1.0 and row["crash_p95_ms"]]
    provisional = min(eligible, key=lambda row: float(row["crash_p95_ms"]))["algorithm"] if eligible else "N/A"
    lines = [
        "# OpenKache Failure Detector 5종 벤치마크 보고서", "",
        "## 범위와 해석", "",
        "이 보고서는 동일한 deterministic fault schedule을 사용한 **Inspired Benchmark** 결과다. 실제 Redis, Cassandra, Ignite, Aerospike 제품 자체의 native benchmark가 아니며, 네트워크 바이트는 model-estimated다. 최종 채택 전 실제 UDP/TCP 및 OpenKache GET/SET workload 검증이 필요하다.", "",
        "## 요약", "",
        "| 알고리즘 | Crash 성공률 | Crash p95 (ms) | Loss 0/15/45 false unavailable | Bytes/node/s |", "|---|---:|---:|---:|---:|",
    ]
    for row in comparison:
        lines.append(f"| {row['algorithm']} | {row['crash_success_rate'] or 'N/A'} | {row['crash_p95_ms'] or 'N/A'} | {row['loss_false_unavailable_events']} | {row['bytes_per_node_second']} |")
    lines += [
        "", "## 시각화", "",
        "그래프의 알고리즘 순서/색상은 `swim-lifeguard`(파랑), `redis-cluster`(빨강), `phi-gossip`(보라), `ignite-tcp-ring`(초록), `aerospike-mesh`(주황)이다.", "",
        "![Crash detection latency](../charts/detection_latency.png)",
        "![False positives](../charts/false_positive_by_loss.png)",
        "![Latency vs cost](../charts/latency_vs_network_cost.png)",
        "![Consecutive crash coverage](../charts/consecutive_crash_coverage.png)",
        "![Membership convergence](../charts/membership_convergence.png)", "",
        "## 구현 범위와 원본 차이", "",
        "- SWIM + Lifeguard: 기존 direct/indirect probe, suspicion, incarnation, LHA 구현 재사용. memberlist wire compatibility는 아님.",
        "- Redis Cluster: PING/PONG, PFAIL, 유효기간이 있는 master failure reports, weak-majority FAIL을 구현. replica election/failover는 제외.",
        "- Phi + Gossip: heartbeat arrival history와 local UP/DOWN 판단을 재사용. DOWN을 gossip하거나 membership에서 자동 제거하지 않음.",
        "- Ignite TCP Ring: logical neighbor timeout, coordinator 검증, next-alive reconnection, versioned topology update를 구현. 실제 Ignite TCP Discovery wire protocol은 아님.",
        "- Aerospike Mesh: all-to-all heartbeat 탐지와 membership 변경 단계를 분리. agreement는 epoch+majority 기반 단순 모델이며 Paxos 재현이 아님.", "",
        "## OpenKache 적용 판단", "",
        f"현재 simulation 지표만으로 가장 빠른 100% crash-detection 후보는 **{provisional}**이다. 이는 잠정 후보이며 real-network 결과, 오탐 비용, 연속 장애 복구, 제품 workload 검증 전에는 최종 선정이 아니다.", "",
        "## 한계와 추가 검증", "",
        "- Native configuration 이름은 비교 모드를 뜻하며 상용 제품 기본값의 완전 복제는 아니다.",
        "- CPU/memory/GC는 아직 N/A다.",
        "- 실제 네트워크 비용은 기존 UDP harness와 신규 TCP harness에서 별도 계측해야 한다.",
        "- Calibrated mode용 독립 calibration dataset은 아직 수행하지 않았다.",
    ]
    (output / "benchmark_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    summary = args.input / "summary"
    args.output.mkdir(parents=True, exist_ok=True)
    charts = args.input / "charts"
    charts.mkdir(parents=True, exist_ok=True)

    detection = rows(summary / "detection_latency.csv")
    false_positive = rows(summary / "false_positive.csv")
    membership = rows(summary / "membership_convergence.csv")
    network = rows(summary / "network_overhead.csv")
    success = rows(summary / "detection_success.csv")
    comparison = rows(args.input / "report" / "algorithm_comparison.csv")

    detection_chart(detection, charts)
    false_positive_chart(false_positive, charts)
    cost_latency_chart(detection, network, charts)
    coverage_chart(success, charts)
    membership_chart(membership, charts)
    write_report(comparison, args.output)
    print(f"wrote charts to {charts}")
    print(f"wrote report to {args.output / 'benchmark_report.md'}")


if __name__ == "__main__":
    main()
