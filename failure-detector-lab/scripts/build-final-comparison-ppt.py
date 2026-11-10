#!/usr/bin/env python3
"""Build the final, five-method / eleven-scenario presentation from official S3 runs.

Requires python-pptx. No values are imputed; incomplete or unattributed data aborts.
Usage: python build-final-comparison-ppt.py --raw PATH --output PATH
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from pathlib import Path

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION, XL_LEGEND_POSITION
from pptx.enum.text import PP_ALIGN
from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls
from pptx.util import Inches, Pt

PROFILES = ["product-swim-lifeguard", "product-redis-cluster", "product-phi-gossip",
            "product-ignite-tcp-ring", "product-aerospike-mesh"]
NAMES = ["SWIM + Lifeguard", "Redis Cluster", "Phi Gossip", "Ignite TCP Ring", "Aerospike Mesh"]
SHORT = ["SWIM+LG", "Redis", "Phi", "Ignite", "Aerospike"]
COLORS = ["14B8A6", "3B82F6", "8B5CF6", "F59E0B", "F43F5E"]
FAULTS = ["normal", "crash", "loss15", "loss45", "delay", "cpu-pause-target",
          "cpu-pause-observer", "cpu-throttle-target", "cpu-throttle-observer",
          "cpu-contention-target", "io-contention-target"]
TITLES = ["정상 상태", "프로세스 강제 종료", "15% 패킷 손실", "45% 패킷 손실", "네트워크 지연",
          "대상 노드 일시 정지", "관측 노드 일시 정지", "대상 노드 CPU 제한", "관측 노드 CPU 제한",
          "대상 노드 CPU 경합", "대상 노드 I/O 경합"]
CONDITIONS = [
    "장애 주입 없이 실행 전체에서 불필요한 SUSPECT / DEAD 전이를 관측",
    "node1 JVM에 SIGKILL · 종료 후 자동 재시작 없음",
    "node1 egress의 실험 UDP/TCP 포트에 15% loss · 10초 · seed 적용",
    "node1 egress의 실험 UDP/TCP 포트에 45% loss · 10초 · seed 적용",
    "node1 egress에 netem delay 250ms ± 50ms · 10초",
    "node1 JVM SIGSTOP → 10초 후 SIGCONT",
    "node0 JVM SIGSTOP → 10초 후 SIGCONT",
    "node1 cgroup CPUQuota=10% · 10초 제한 후 CPUQuota=100% 복원",
    "node0 cgroup CPUQuota=10% · 10초 제한 후 CPUQuota=100% 복원",
    "node1에서 stress-ng matrixprod · CPU worker 1개 · 10초",
    "node1의 로컬 NVMe에서 fio libaio / direct / 4KiB / QD32 / randrw · 10초",
]
BG, PANEL, INK, MUTED = "0B1425", "142238", "F1F5F9", "A8B7CC"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def percentile(values, q=.95):
    if not values:
        return None
    values = sorted(values)
    at = (len(values) - 1) * q
    lo = int(at)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (at - lo)


def med(values):
    return statistics.median(values) if values else None


def fmt(value, digits=2):
    return "—" if value is None else f"{value:.{digits}f}"


def load_runs(raw):
    if not (raw / "COMPLETED").is_file():
        raise ValueError("COMPLETED marker missing; final report requires completed campaign")
    final = read_json(raw / "final-validation.json")
    if final.get("status") != "passed":
        raise ValueError("Final campaign validation did not pass")
    with (raw / "campaign" / "execution-status.csv").open(encoding="utf-8", newline="") as f:
        records = list(csv.DictReader(f))
    with (raw / "campaign" / "plan.tsv").open(encoding="utf-8", newline="") as f:
        plan = {r["sequence"]: r for r in csv.DictReader(f, delimiter="\t")}
    official = [r for r in records if r["status"] == "passed"]
    if len(official) != 550 or len({r["sequence"] for r in official}) != 550:
        raise ValueError("Expected exactly 550 unique official passed rows")
    runs, issues = [], []
    for row in official:
        p = plan[row["sequence"]]
        if any(row[k] != p[k] for k in ("profile", "fault", "seed")):
            raise ValueError(f"Plan mismatch: {row['run_id']}")
        base = raw / "runs" / row["run_id"] / "node-0"
        r = read_json(base / "result.json")
        properties = dict(line.split("=", 1) for line in
                          (base / "run.properties").read_text(encoding="utf-8").splitlines()
                          if "=" in line)
        ev = [json.loads(line[len("EVENT_JSON="):]) for line in
              (base / "events.log").read_text(encoding="utf-8").splitlines()
              if line.startswith("EVENT_JSON=")]
        if r.get("target") != "node-1" or r.get("nodeId") != 0:
            raise ValueError(f"Legacy or wrong target result: {row['run_id']}")
        det = [e for e in ev if e.get("kind") == "detector"]
        if any(not e.get("target") for e in det):
            raise ValueError(f"Unattributed event: {row['run_id']}")
        target_ev = [e for e in det if e["target"] == "node-1"]
        state, previous = "ALIVE", -1
        for e in target_ev:
            if e["oldStatus"] != state or e["newStatus"] == state or e["atNanos"] < previous:
                issues.append(f"Invalid state chain: {row['run_id']}")
            state, previous = e["newStatus"], e["atNanos"]
        if state != r.get("finalTargetStatus"):
            issues.append(f"Final state mismatch: {row['run_id']}")
        start, end = r.get("faultAtNanos"), r.get("faultEndAtNanos")
        for status, field in (("SUSPECT", "suspectLatencyNanos"), ("DEAD", "deadLatencyNanos")):
            hits = [e["atNanos"] for e in target_ev if e["newStatus"] == status and
                    start is not None and e["atNanos"] >= start]
            expected = min(hits) - start if hits else None
            if r.get(field) != expected:
                issues.append(f"Target latency mismatch: {row['run_id']} {field}")
        duration = r["runDurationNanos"] / 1e9
        dead = any(e["newStatus"] == "DEAD" for e in target_ev)
        suspect = any(e["newStatus"] == "SUSPECT" for e in target_ev)
        end_state = "ALIVE"
        if end is not None:
            for e in target_ev:
                if e["atNanos"] <= end:
                    end_state = e["newStatus"]
        recovery = [e["atNanos"] for e in target_ev if e["newStatus"] == "ALIVE" and
                    end is not None and e["atNanos"] > end]
        recovery_eligible = end is not None and end_state != "ALIVE"
        for status, field in (("SUSPECT", "preFaultSuspectEvents"), ("DEAD", "preFaultDeadEvents")):
            expected = sum(e["newStatus"] == status and (start is None or e["atNanos"] < start)
                           for e in target_ev)
            if r.get(field) != expected:
                issues.append(f"Pre-fault metric mismatch: {row['run_id']} {field}")
        recorded_recovery = [e["atNanos"] for e in target_ev if e["newStatus"] == "ALIVE" and
                             end is not None and e["atNanos"] >= end]
        recorded_recovery_at = min(recorded_recovery) if recorded_recovery else None
        if r.get("recoveredAtNanos") != recorded_recovery_at:
            issues.append(f"Recovery event mismatch: {row['run_id']}")
        expected_latency = recorded_recovery_at - end if recorded_recovery_at is not None else None
        if r.get("recoveryLatencyNanos") != expected_latency:
            issues.append(f"Recovery latency mismatch: {row['run_id']}")
        if row["fault"] == "normal" and (start is not None or end is not None):
            issues.append(f"Normal has fault marker: {row['run_id']}")
        if row["fault"] != "normal" and start is None:
            issues.append(f"Missing fault start: {row['run_id']}")
        if row["fault"] not in ("normal", "crash") and end is None:
            issues.append(f"Missing fault end: {row['run_id']}")
        ready = [e for e in ev if e.get("kind") == "transport-ready"]
        if row["profile"] in PROFILES[3:]:
            peers = 1 if row["profile"] == PROFILES[3] else 4
            if len(ready) != 1 or ready[0]["peerCount"] != peers or (start and ready[0]["atNanos"] >= start):
                issues.append(f"TCP readiness mismatch: {row['run_id']}")
        if row["fault"] == "io-contention-target":
            fio = read_json(raw / "runs" / row["run_id"] / "node-1" / "fio-validation.json")
            if fio.get("status") != "passed":
                issues.append(f"fio invalid: {row['run_id']}")
        runs.append(dict(row, result=r, jar_sha256=properties.get("jar_sha256"),
                         dead=dead, suspect=suspect, final_alive=state == "ALIVE",
                         dead_s=None if r.get("deadLatencyNanos") is None else r["deadLatencyNanos"] / 1e9,
                         suspect_s=None if r.get("suspectLatencyNanos") is None else r["suspectLatencyNanos"] / 1e9,
                         recovery_eligible=recovery_eligible,
                         recovery_s=(min(recovery) - end) / 1e9 if recovery_eligible and recovery else None,
                         tx_fps=r["sentFrames"] / duration,
                         rx_fps=r["receivedFrames"] / duration,
                         app_kbps=(r["sentBytes"] + r["receivedBytes"]) * 8 / duration / 1000))
    counts = Counter((r["profile"], r["fault"]) for r in runs)
    if len(counts) != 55 or any(n != 10 for n in counts.values()):
        issues.append("55 cells × 10 repetitions contract failed")
    if issues:
        raise ValueError("\n".join(issues))
    return runs, records, final


def summarize(runs):
    summary = []
    for fault in FAULTS:
        for profile in PROFILES:
            cell = [r for r in runs if r["fault"] == fault and r["profile"] == profile]
            dead_times = [r["dead_s"] for r in cell if r["dead_s"] is not None]
            recovery_times = [r["recovery_s"] for r in cell if r["recovery_s"] is not None]
            summary.append(dict(fault=fault, profile=profile, n=len(cell),
                suspect_runs=sum(r["suspect"] for r in cell), dead_runs=sum(r["dead"] for r in cell),
                final_alive_runs=sum(r["final_alive"] for r in cell),
                dead_p50_s=med(dead_times), dead_p95_s=percentile(dead_times),
                recovery_eligible=sum(r["recovery_eligible"] for r in cell),
                recovered_runs=len(recovery_times), recovery_p50_s=med(recovery_times),
                tx_fps_p50=med([r["tx_fps"] for r in cell]),
                app_kbps_p50=med([r["app_kbps"] for r in cell])))
    return summary


def color(value):
    return RGBColor.from_string(value)


class Deck:
    def __init__(self):
        self.prs = Presentation()
        self.prs.slide_width, self.prs.slide_height = Inches(13.333), Inches(7.5)

    def text(self, slide, text, x, y, w, h, size=18, ink=INK, bold=False):
        box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
        tf = box.text_frame
        tf.word_wrap = True
        tf.margin_left = tf.margin_right = Inches(.025)
        for i, line in enumerate(str(text).split("\n")):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.text = line
            p.font.name, p.font.size, p.font.bold = "Malgun Gothic", Pt(size), bold
            p.font.color.rgb = color(ink)
            p.space_after = Pt(8)
        return box

    def slide(self, title, subtitle="", section="RESULTS"):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = color(BG)
        self.text(s, f"OPENKACHE   /   {section}", .55, .25, 11, .3, 10, "64D9CD", True)
        self.text(s, title, .55, .83, 12.2, .7, 28, bold=True)
        self.text(s, subtitle, .57, 1.56, 12.1, .6, 13, MUTED)
        self.text(s, "EC2 m6gd.medium × 5  ·  11 scenarios × 5 methods × 10 runs  ·  2026.11", .57, 7.1, 11.3, .22, 9, MUTED)
        self.text(s, f"{len(self.prs.slides):02d}", 12.15, 7.04, .6, .3, 11, MUTED)
        return s

    def table(self, s, headers, rows, y=2.4, widths=None, height=3.25, size=15):
        shape = s.shapes.add_table(len(rows) + 1, len(headers), Inches(.6), Inches(y), Inches(12.1), Inches(height))
        table = shape.table
        if widths:
            for col, width in zip(table.columns, widths):
                col.width = Inches(width)
        for ri, row in enumerate([headers] + rows):
            for ci, value in enumerate(row):
                c = table.cell(ri, ci)
                c.text = str(value)
                c.fill.solid()
                c.fill.fore_color.rgb = color("20344D" if ri == 0 else PANEL if ri % 2 else "182A43")
                c.margin_left = Inches(.11)
                c.margin_top = Inches(.12)
                for p in c.text_frame.paragraphs:
                    p.font.name, p.font.size = "Malgun Gothic", Pt(size)
                    p.font.bold = ri == 0
                    p.font.color.rgb = color("67E8F9" if ri == 0 else INK)
        return table

    def notes(self, s, text):
        s.notes_slide.notes_text_frame.text = text

    def chart(self, s, categories, series, y=2.3, h=3.7):
        data = CategoryChartData()
        data.categories = categories
        for name, values in series:
            data.add_series(name, values)
        chart = s.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(.65), Inches(y), Inches(12), Inches(h), data).chart
        chart.chart_style = 10
        chart.has_title = False
        chart.has_legend = False
        if len(series) > 1:
            for i, (name, _) in enumerate(series):
                self.text(s, "■  " + name, .85 + i * 2.5, y + h + .07,
                          2.4, .25, 11, COLORS[i % 5])
        chart._chartSpace.append(parse_xml(
            f'<c:spPr {nsdecls("c", "a")}><a:solidFill><a:srgbClr val="{BG}"/></a:solidFill>'
            '<a:ln><a:noFill/></a:ln></c:spPr>'))
        for axis in (chart.category_axis, chart.value_axis):
            axis.tick_labels.font.name = "Malgun Gothic"
            axis.tick_labels.font.size = Pt(12)
            axis.tick_labels.font.color.rgb = color(INK)
        for i, ser in enumerate(chart.series):
            if len(series) == 1:
                for j, point in enumerate(ser.points):
                    point.format.fill.solid()
                    point.format.fill.fore_color.rgb = color(COLORS[j % 5])
            else:
                ser.format.fill.solid()
                ser.format.fill.fore_color.rgb = color(COLORS[i % 5])
        plot = chart.plots[0]
        plot.has_data_labels = True
        plot.data_labels.position = XL_LABEL_POSITION.OUTSIDE_END
        plot.data_labels.number_format = "0.00"
        plot.data_labels.font.size = Pt(10)
        plot.data_labels.font.color.rgb = color(INK)
        return chart


def build(runs, records, final, output):
    summary = summarize(runs)
    lookup = {(r["fault"], r["profile"]): r for r in summary}
    d = Deck()
    s = d.slide("장애 감지 방법론 5종 비교", "11개 시나리오 · 550회 공식 실행 · 실제 EC2 네트워크 / 로컬 NVMe", "FINAL REPORT")
    d.text(s, "빠른 탐지와 안정적인 판정 사이의\n트레이드오프를 실제 장애로 검증", .7, 2.6, 11.8, 1.6, 31, bold=True)
    d.text(s, "SWIM + Lifeguard  /  Redis Cluster  /  Phi Gossip\nIgnite TCP Ring  /  Aerospike Mesh", .73, 5.0, 12, .9, 21, MUTED)
    d.notes(s, "제품 본체 벤치마크가 아닌 product-inspired 실험 어댑터 비교다. 공식 실행은 execution-status.csv의 passed 550개만 사용한다.")

    crash = [lookup["crash", p] for p in PROFILES]
    fastest = min((i for i, r in enumerate(crash) if r["dead_p50_s"] is not None), key=lambda i: crash[i]["dead_p50_s"])
    network_faults = {"loss15", "loss45", "delay"}
    net_counts = [sum(r["dead"] for r in runs if r["profile"] == p and r["fault"] in network_faults) for p in PROFILES]
    s = d.slide("핵심 결과", "탐지 속도와 네트워크 장애 중의 DEAD 판정을 함께 평가")
    d.table(s, ["관점", "실측 요약"], [
        ["Crash 탐지", " / ".join(f"{SHORT[i]} {r['dead_runs']}/10" for i, r in enumerate(crash))],
        ["가장 짧은 Crash 중앙값", f"{NAMES[fastest]} · {fmt(crash[fastest]['dead_p50_s'])}초 (탐지된 실행 조건부)"],
        ["네트워크 장애 중 DEAD", " / ".join(f"{SHORT[i]} {n}/30" for i, n in enumerate(net_counts))],
        ["실험 증거", "550개 공식 실행 · 각 조합 10회 · 대상별 이벤트/최종 상태/지연 교차 검증"],
    ], widths=[2.5, 9.6], size=16)
    d.text(s, "단일 종합 순위를 만들지 않고, 탐지율·판정 안정성·복구·비용을 함께 비교합니다.", .7, 6.22, 12, .45, 17, "64D9CD")

    s = d.slide("실험 설계와 재현 조건", "서울 리전 · m6gd 기반 · 독립 프로세스와 실제 소켓", "DESIGN")
    d.table(s, ["항목", "설정 / 해석"], [
        ["배치", "m6gd.medium 5대 · observer=node0 / 장애 대상=node1"],
        ["반복", "5방법 × 11시나리오 × seed 1–10 = 550회 · 고정 seed 순서 셔플"],
        ["시간", "준비 완료 후 warm-up 15초 · 가역 장애 10초 · 복구 관측 최소 15초"],
        ["통신", "SWIM/Redis/Phi: UDP · Ignite/Aerospike: TCP heartbeat + UDP control"],
        ["스토리지", "instance-store 로컬 NVMe · fio 파일 준비는 detector 시작 전 완료"],
    ], widths=[2, 10.1], height=3.7, size=16)

    s = d.slide("비교하는 다섯 가지 구현", "알고리즘 원리를 재현한 실험용 어댑터이며, 상용 제품 전체 구현의 성능을 뜻하지 않음", "DESIGN")
    d.table(s, ["방법론", "판정 원리", "해석 범위"], [
        [NAMES[0], "direct/indirect probe + suspicion + local health", "전용 UDP SWIM 실험 경로"],
        [NAMES[1], "ping/pong timeout + 독립 실패 보고", "Redis Cluster-inspired membership"],
        [NAMES[2], "heartbeat 분포 기반 Phi + gossip 전달", "Phi accrual 실험 구현"],
        [NAMES[3], "논리 ring TCP heartbeat + coordinator 검증", "Ignite TCP Discovery-inspired"],
        [NAMES[4], "TCP mesh heartbeat + epoch/majority removal", "간소화된 합의 · 실제 Paxos 구현 아님"],
    ], widths=[2.8, 5.4, 3.9], height=3.6, size=14)

    s = d.slide("지표 정의: 결과 유효성과 성능을 구분", "passed는 실험 증거가 유효하다는 뜻이며, 오탐 또는 미탐이 0이라는 뜻이 아님", "METRICS")
    d.table(s, ["지표", "정의"], [
        ["DEAD 발생 실행", "node1이 한 번 이상 DEAD로 전이한 실행 수 / 10"],
        ["Crash 탐지 지연", "fault-start → node1 최초 DEAD · 탐지된 실행에 한해 p50 / p95"],
        ["최종 ALIVE", "관측 종료 시 node1 detector 상태가 ALIVE인 실행 수 / 10"],
        ["복구 지연", "fault-end 시 비-ALIVE인 실행의 이후 최초 ALIVE · 조건부 중앙값"],
        ["통신 비용", "node0 송수신 application bytes 합 / 실제 관측 시간 · TCP/IP 헤더 제외"],
    ], widths=[2.7, 9.4], height=3.7, size=15)
    d.notes(s, "Normal/네트워크 손실·지연에서 DEAD는 false conviction으로 해석한다. SIGSTOP과 자원 장애는 판정 정책 차이가 있어 모두 같은 의미의 오탐으로 합치지 않는다. 결측 지연은 0초가 아니다.")

    s = d.slide("전체 11개 시나리오: DEAD 발생 분포", "각 셀은 DEAD 발생 실행 수 / 10 · Crash 행은 탐지, 나머지 행은 생존 프로세스에 대한 판정", "OVERVIEW")
    rows = [[TITLES[i]] + [f"{lookup[f, p]['dead_runs']}/10" for p in PROFILES] for i, f in enumerate(FAULTS)]
    t = d.table(s, ["시나리오"] + SHORT, rows, y=2.2, height=4.55, widths=[3.1, 1.8, 1.8, 1.8, 1.8, 1.8], size=12)
    for ri, fault in enumerate(FAULTS, 1):
        for ci, p in enumerate(PROFILES, 1):
            n = lookup[fault, p]["dead_runs"]
            t.cell(ri, ci).fill.fore_color.rgb = color("164E45" if (fault == "crash" and n == 10) else "672B3C" if (fault != "crash" and n) else PANEL)

    for fi, fault in enumerate(FAULTS):
        cells = [lookup[fault, p] for p in PROFILES]
        s = d.slide(f"{fi+1:02d}  {TITLES[fi]}", CONDITIONS[fi], "SCENARIO")
        headers = ["방법론", "SUSPECT", "DEAD", "최종 ALIVE", "DEAD p50 / p95(s)", "통신(kbit/s)"]
        rows = [[NAMES[i], f"{r['suspect_runs']}/10", f"{r['dead_runs']}/10", f"{r['final_alive_runs']}/10",
                 f"{fmt(r['dead_p50_s'])} / {fmt(r['dead_p95_s'])}", fmt(r['app_kbps_p50'])] for i, r in enumerate(cells)]
        d.table(s, headers, rows, widths=[2.7, 1.4, 1.2, 1.6, 3.2, 2.0], height=3.2, size=13)
        if fault == "crash":
            takeaway = "전 방법 10/10 탐지 · 중앙값: Aerospike 1.54초 < SWIM 3.20초 < Ignite 5.34초 < Redis 15.42초 < Phi 18.62초."
        elif fault == "normal":
            takeaway = "5개 방법 모두 SUSPECT 0/10 · DEAD 0/10 · 최종 ALIVE 10/10."
        elif fault in network_faults:
            takeaway = f"생존 노드의 DEAD 판정: " + " · ".join(f"{SHORT[i]} {r['dead_runs']}/10" for i, r in enumerate(cells))
            if fault == "delay":
                ratio = lookup["delay", PROFILES[0]]["app_kbps_p50"] / lookup["normal", PROFILES[0]]["app_kbps_p50"]
                takeaway = f"전 방법 DEAD 0/10 · SWIM SUSPECT 10/10, 통신량은 정상 대비 {ratio:.1f}배로 증가."
            elif fault == "loss15":
                takeaway = "전 방법 DEAD 0/10 · SWIM만 SUSPECT 8/10 · 최종 상태는 모두 ALIVE."
        elif fault == "cpu-pause-target":
            takeaway = "SWIM: DEAD 후 10/10 ALIVE 복귀 · Ignite/Aerospike: 재개 후에도 10/10 DEAD 유지."
        elif fault == "cpu-pause-observer":
            takeaway = "전 방법 DEAD 0/10 · Ignite SUSPECT 10/10, Aerospike 9/10 · 종료 시 모두 ALIVE."
        else:
            takeaway = "전 방법 SUSPECT/DEAD 0/10, 최종 ALIVE 10/10 · 이 부하 조건을 넘어선 안정성은 추가 검증 필요."
        d.text(s, takeaway, .7, 5.95, 12, .55, 16, "64D9CD")
        d.text(s, "각 방법 n=10 · 지연/통신은 중앙값 · p95는 탐지된 실행의 경험 분위수 · —는 해당 전이 없음", .7, 6.54, 12, .3, 11, MUTED)
        d.notes(s, f"fault={fault}. 공식 실행 50개. SUSPECT는 한 번 이상 SUSPECT 전이가 있었던 실행 수다. DEAD p50/p95는 장애 이후 대상 node1만 계산한다. 통신량은 observer만의 application bytes이며 전체 클러스터 트래픽이나 실제 NIC 대역폭이 아니다.")

    s = d.slide("Crash: 탐지율을 확인한 뒤 지연을 비교", "탐지된 실행의 최초 node1 DEAD 지연 · 단위 초", "SYNTHESIS")
    d.chart(s, SHORT, [("p50", [r["dead_p50_s"] for r in crash]), ("p95", [r["dead_p95_s"] for r in crash])])
    d.text(s, "탐지 횟수: " + "  |  ".join(f"{SHORT[i]} {r['dead_runs']}/10" for i, r in enumerate(crash)), .7, 6.35, 12, .4, 17, "64D9CD")

    s = d.slide("네트워크 손실·지연에서의 판정 안정성", "동일한 실행 중 DEAD 이벤트의 발생 횟수를 세는 대신, DEAD가 있었던 실행 비율을 비교", "SYNTHESIS")
    d.chart(s, SHORT, [(TITLES[FAULTS.index(f)], [lookup[f, p]["dead_runs"] * 10 for p in PROFILES]) for f in ("loss15", "loss45", "delay")])
    d.text(s, "세로축: DEAD 발생 실행 비율(%) · 각 조건 n=10 · 1회 차이가 10%p이며 작은 표본의 불확실성 존재", .7, 6.35, 12, .5, 14, MUTED)

    s = d.slide("가역 장애 이후 상태 복원", "fault-end 시 비-ALIVE인 경우만 복구 지연의 분모로 사용 · 계속 ALIVE인 실행을 0초 복구로 처리하지 않음", "SYNTHESIS")
    rows = []
    for i, p in enumerate(PROFILES):
        eligible = [r for r in runs if r["profile"] == p and r["fault"] not in ("normal", "crash") and r["recovery_eligible"]]
        times = [r["recovery_s"] for r in eligible if r["recovery_s"] is not None]
        rows.append([NAMES[i], str(len(eligible)), str(len(times)),
                     fmt(med(times) * 1000 if times else None), str(len(eligible) - len(times))])
    d.table(s, ["방법론", "복구 대상 실행", "ALIVE 복귀", "조건부 p50(ms)", "관측 내 미복귀"], rows, widths=[3, 2.2, 2, 2.5, 2.4], size=14)
    d.text(s, "membership removal 이후 재가입이 구현되지 않은 경로에서는 DEAD가 유지될 수 있습니다. 이를 상태 기록 오류와 구분합니다.", .7, 6.18, 12, .55, 15, "F9C66C")
    d.notes(s, "복구 지연은 서비스 재시작이나 요청 성공 시간이 아닌 detector ALIVE 전이 시간이다. observer pause에서는 재개 직후 큐 처리 때문에 sub-ms 전이가 생길 수 있다. 미복귀는 해당 실행의 관측 구간 내 미복귀를 뜻한다.")

    s = d.slide("정상 상태의 통신 비용", "observer node0 · 송수신 application bytes 합의 초당 비트 수 · 각 방법 n=10", "SYNTHESIS")
    d.chart(s, SHORT, [("kbit/s", [lookup["normal", p]["app_kbps_p50"] for p in PROFILES])])
    d.text(s, "TCP/IP 헤더·재전송·다른 노드 트래픽은 포함하지 않습니다. 전체 클러스터 비용으로 일반화하지 않습니다.", .7, 6.38, 12, .5, 14, MUTED)

    s = d.slide("선택 기준: 속도·안정성·복원 정책", "이번 550회에서 관측한 차이 · 제품 전체 성능으로 일반화하지 않음", "CONCLUSION")
    d.table(s, ["운영 관심사", "이 실험에서 확인할 근거"], [
        ["빠른 종료 감지", f"Aerospike {fmt(crash[4]['dead_p50_s'])}초 · SWIM {fmt(crash[0]['dead_p50_s'])}초 · 모두 10/10 탐지"],
        ["고손실 판정 안정성", "45% loss: Redis/Phi/Ignite DEAD 0/10 · SWIM/Aerospike 5/10"],
        ["보수적 판정의 대가", f"Redis {fmt(crash[1]['dead_p50_s'])}초 · Phi {fmt(crash[2]['dead_p50_s'])}초의 Crash 중앙값"],
        ["일시 정지 후 복원", "SWIM ALIVE 복귀 10/10 · Ignite/Aerospike는 재가입 경로 검증 필요"],
        ["통신 비용", f"delay에서 SWIM {lookup['delay',PROFILES[0]]['app_kbps_p50']:.2f} kbit/s · 정상 대비 증가 원인 후속 분석"],
    ], widths=[3, 9.1], size=15, height=3.7)

    s = d.slide("결과의 신뢰 범위와 한계", "완료된 실험의 근거와 적용 범위를 함께 공개", "LIMITATIONS")
    d.text(s, "01  다섯 제품의 본체가 아닌 product-inspired 구현 비교\n02  조합당 10회: 0/10도 실제 오류 확률 0%를 증명하지 않음\n03  node0 관점의 node1 판정: 클러스터 전체 일관성/서비스 성공률은 별도 검증\n04  자원 경합 주입은 detector CPU/디스크 의존성과 분리해서 해석\n05  최종 ALIVE는 관측 종료 상태이며, 관측 기간 이후의 복구는 알 수 없음\n06  대상 attribution 수정 전 결과와 중복 재시도는 공식 표본에서 제외", .75, 2.35, 11.85, 3.9, 21)

    s = d.slide("감사 가능한 최종 데이터", "슬라이드의 모든 수치는 공식 run_id와 원시 이벤트로 추적 가능", "APPENDIX")
    d.table(s, ["아티팩트", "용도"], [
        ["COMPLETED / final-validation.json", "캠페인 완료·550개 checkpoint 검증"],
        ["campaign/plan.tsv / execution-status.csv", "공식 실행 선택과 조합당 10회 검증"],
        ["runs/<run_id>/node-0/result.json / events.log", "대상별 전이·지연·최종 상태·실제 프레임 증거"],
        ["node-1/fio-validation.json", "NVMe 장치·QD·IOPS·실제 장애 시간 검증"],
        ["summary.csv / official-runs.csv / report-audit.json", "55개 셀 요약·550개 표본·최종 보고서 교차 검증"],
    ], widths=[6.1, 6.0], height=3.6, size=13)
    hashes = Counter(r.get("jar_sha256") for r in runs)
    d.text(s, "공식 실행 JAR 2개: 대상별 계측 수정 18회 / TCP 종료 race 보완 532회 · SHA-256은 official-runs.csv에 보존",
           .7, 6.3, 12, .45, 12, MUTED)
    d.notes(s, "처음 18회와 이후 532회는 JAR SHA-256이 다르다. 추가 변경은 종료 직후 TCP reader의 executor 예외 처리이며 detector 판정 규칙 변경은 없다. 알고리즘 outcome으로 통과할 때까지 재시도한 추가 2회는 duplicate-after-valid로 제외하고 첫 증거 유효 실행을 채택했다.")
    # Organize the full report into the four chapters requested for presentation.
    original = list(d.prs.slides)
    agenda = d.slide("발표 구성", "방법론과 실험 조건을 먼저 정의하고, 결과를 근거로 결론을 도출", "CONTENTS")
    d.table(agenda, ["구분", "발표에서 답하는 질문"], [
        ["1. 방법론", "어떤 원리로 감지하며, 주기와 판정 기준은 어떻게 다른가?"],
        ["2. 시나리오", "어디에 어떤 장애를 얼마나 주입했고, 무엇을 측정했는가?"],
        ["3. 각 시나리오별 결과", "11개 조건 각각에서 5개 방법은 어떤 상태와 지연을 보였는가?"],
        ["4. 결론", "속도·판정 안정성·복원·통신량을 종합하면 어떤 장단점이 있는가?"],
    ], widths=[3.2, 8.9], size=18, height=3.6)

    settings = d.slide("판정 설정과 비교 기준", "설정값을 고정한 동작 비교 · 동일한 탐지 시간/오탐 목표로 정규화한 비교는 아님")
    d.table(settings, ["방법론", "주기", "판정 설정", "복원 관련 구현"], [
        [NAMES[0], "probe 200ms", "probe timeout 100ms + 적응형 suspicion", "probe 성공/반박에 따른 ALIVE 전이"],
        [NAMES[1], "ping 1초", "node timeout 15초 + 실패 보고", "ping/pong과 membership 상태에 의존"],
        [NAMES[2], "heartbeat 1초", "Phi threshold 8 · local pause guard 5초", "새 heartbeat 수신 시 ALIVE 전이"],
        [NAMES[3], "heartbeat 1초", "neighbor timeout 5초 + 검증 대기 1초", "멤버 제거 후 자동 재가입 경로 없음"],
        [NAMES[4], "heartbeat 150ms", "heartbeat timeout 1.5초 + 간소화 합의", "멤버 제거 후 자동 재가입 경로 없음"],
    ], widths=[2.65, 1.65, 4.0, 3.8], height=3.65, size=13)
    d.text(settings, "10초 일시 정지는 Redis의 15초 timeout보다 짧습니다. 무판정을 곧바로 알고리즘 우수성으로 해석하지 않습니다.", .7, 6.38, 12, .45, 14, "F9C66C")

    scenarios = d.slide("11개 시나리오와 장애 주입", "각 시나리오에서 5방법 × 10회 = 50회 · 가역 장애는 10초 적용")
    conditions_short = [
        "주입 없음", "node1 JVM SIGKILL", "node1 egress UDP/TCP 15% 손실",
        "node1 egress UDP/TCP 45% 손실", "node1 egress 250ms ± 50ms 지연",
        "node1 SIGSTOP 후 SIGCONT", "node0 SIGSTOP 후 SIGCONT", "node1 CPUQuota 10%",
        "node0 CPUQuota 10%", "node1 stress-ng matrixprod / worker 1개",
        "node1 로컬 NVMe / fio libaio, direct, 4KiB, QD32",
    ]
    d.table(scenarios, ["번호", "시나리오", "조건", "판정 시 확인할 점"], [
        [f"{i+1:02d}", TITLES[i], conditions_short[i],
         "실제 종료 탐지율/지연" if f == "crash" else
         "불필요한 SUSPECT/DEAD" if f == "normal" else
         "생존 노드 DEAD / 최종 ALIVE" if f in network_faults else
         "재개 후 복원 / 관측 노드 영향" if "pause" in f else "부하 중 상태 변화"]
        for i, f in enumerate(FAULTS)
    ], widths=[.65, 2.7, 5.15, 3.6], y=2.2, height=4.55, size=12)

    verdict = d.slide("최종 결론: 속도·안정성·복원 정책의 차이", "이 구현과 설정에서 관측한 장단점 · 우선순위에 따라 선택 기준이 달라짐")
    d.table(verdict, ["방법론", "Crash p50", "loss45 DEAD", "대상 정지 후", "이번 실험의 해석"], [
        [NAMES[0], fmt(crash[0]["dead_p50_s"])+"초", "5/10", "ALIVE 10/10", "빠른 감지·복원 / 고손실 판정과 지연 시 통신 증가 주의"],
        [NAMES[1], fmt(crash[1]["dead_p50_s"])+"초", "0/10", "ALIVE 10/10", "보수적 판정 / 15초 timeout에 따른 긴 종료 탐지"],
        [NAMES[2], fmt(crash[2]["dead_p50_s"])+"초", "0/10", "ALIVE 10/10", "관측된 변동에 안정적 / 이번 설정에서 가장 긴 종료 탐지"],
        [NAMES[3], fmt(crash[3]["dead_p50_s"])+"초", "0/10", "DEAD 10/10", "이번 조건에서 속도·고손실 판정 절충 / 재가입 구현 필요"],
        [NAMES[4], fmt(crash[4]["dead_p50_s"])+"초", "5/10", "DEAD 10/10", "가장 짧은 종료 탐지 / 고손실 DEAD·재가입 범위 주의"],
    ], widths=[2.4, 1.3, 1.5, 1.7, 5.2], height=3.65, size=13)
    d.text(verdict, "핵심 결론: 속도·안정성·복원에 장단점이 있습니다. 알고리즘 우열은 동일 목표로 설정을 맞춘 뒤 검증해야 합니다.", .7, 6.35, 12, .55, 15, "64D9CD")

    def replace_text(shape, value):
        paragraph = shape.text_frame.paragraphs[0]
        paragraph.runs[0].text = value

    # Retain the evidence, with the newly discovered shutdown exceptions disclosed.
    old_body = original[22].shapes[5]._element
    old_body.getparent().remove(old_body)
    d.text(original[22],
        "01  제품 본체가 아닌 실험용 구현 비교 · timeout/주기 차이를 포함한 결과\n"
        "02  조합당 10회 · 0/10은 오류 확률 0%를 증명하지 않음 · p95는 탐색적\n"
        "03  node0의 node1 판정 · 서비스 성공률/전체 클러스터 비용은 별도 계측 필요\n"
        "04  CPU·I/O 주입 성공과 detector의 실제 자원 대기 발생은 별개\n"
        "05  제거된 멤버의 자동 재가입 미구현이 DEAD 유지에 영향\n"
        "06  SWIM 173·277번: 결과 출력 후 UDP 종료 예외 2건 · 측정 영향 징후 없음",
        .75, 2.35, 11.85, 3.9, 19)
    d.notes(original[22], "observer 원시 로그 전수 검색에서 확인한 2건 모두 RESULT_JSON 이후 executor shutdown 시점 RejectedExecutionException. 예외 0건이라는 기존 진행 보고를 정정한다. 해당 실행의 측정값은 정상 종료 시점까지 기록돼 있으며, 향후 UDP 종료 race 수정이 필요하다.")

    method_slides = [original[3], settings]
    scenario_slides = [original[2], scenarios, original[4]]
    result_slides = [original[5]] + original[6:17]
    conclusion_slides = [original[1]] + original[17:22] + [verdict, original[22], original[23]]
    for chapter, slides in (("1. 방법론", method_slides), ("2. 시나리오", scenario_slides),
                            ("3. 각 시나리오별 결과", result_slides), ("4. 결론", conclusion_slides)):
        for slide in slides:
            replace_text(slide.shapes[0], "OPENKACHE   /   " + chapter)
    replace_text(method_slides[0].shapes[1], "1. 방법론 | 다섯 가지 장애 감지 방식")
    replace_text(scenario_slides[0].shapes[1], "2. 시나리오 | 실험 환경과 공통 조건")
    replace_text(result_slides[0].shapes[1], "3. 각 시나리오별 결과 | 전체 비교")
    replace_text(conclusion_slides[0].shapes[1], "4. 결론 | 실험으로 확인한 핵심 결과")
    final_order = [original[0], agenda] + method_slides + scenario_slides + result_slides + conclusion_slides
    ids = {slide.slide_id: item for slide, item in zip(d.prs.slides, list(d.prs.slides._sldIdLst))}
    ordered_ids = [ids[slide.slide_id] for slide in final_order]
    for item in list(d.prs.slides._sldIdLst):
        d.prs.slides._sldIdLst.remove(item)
    for page, (slide, item) in enumerate(zip(final_order, ordered_ids), 1):
        d.prs.slides._sldIdLst.append(item)
        replace_text(slide.shapes[4], f"{page:02d}")

    output.mkdir(parents=True, exist_ok=True)
    ppt = output / "OpenKache-Final-11Scenarios-5Methods-4Sections.pptx"
    d.prs.save(ppt)
    for name, rows in (("summary.csv", summary), ("official-runs.csv", [{k:v for k,v in r.items() if k != "result"} for r in runs])):
        with (output / name).open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    audit = dict(status="passed", official_runs=len(runs), cells=len(summary), runs_per_cell=10,
                 slides=len(d.prs.slides), excluded_statuses=dict(Counter(r["status"] for r in records if r["status"] != "passed")),
                 jar_versions=dict(hashes),
                 chapters=["1. 방법론", "2. 시나리오", "3. 각 시나리오별 결과", "4. 결론"],
                 observer_shutdown_exceptions_after_result=["sequence-0173-attempt-1", "sequence-0277-attempt-1"],
                 source_final_validation=final, metrics="Target node1 only; null preserved; observer application traffic only")
    (output / "report-audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(dict(pptx=str(ppt), **audit), ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs, records, final = load_runs(args.raw)
    build(runs, records, final, args.output)


if __name__ == "__main__":
    main()
