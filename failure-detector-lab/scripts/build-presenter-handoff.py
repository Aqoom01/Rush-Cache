#!/usr/bin/env python3
"""Generate the evidence-backed OpenStd presenter deck and handoff package."""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION
from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls
from pptx.util import Inches, Pt

spec = importlib.util.spec_from_file_location("evidence_base", Path(__file__).with_name("build-final-comparison-ppt.py"))
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)
P, N, SHORT, F = B.PROFILES, B.NAMES, B.SHORT, B.FAULTS
BG, INK, MUTED = B.BG, B.INK, B.MUTED
TEAL, AMBER, RED = "14B8A6", "F59E0B", "F43F5E"

DECISION = "OpenStd는 SWIM + Lifeguard를 장애 감지의 기반 알고리즘으로 선정한다."
RELEASE = "고손실 오탐 완화와 복제 계층의 안전한 승격 검증을 적용 완료 조건으로 둔다."


class PresenterDeck(B.Deck):
    def __init__(self):
        super().__init__()
        self.script = []

    def add(self, title, subtitle, chapter, speech):
        slide = self.slide(title, subtitle, chapter)
        slide.shapes[0].text_frame.paragraphs[0].runs[0].text = "OPENSTD   /   " + chapter
        self.notes(slide, speech)
        self.script.append((len(self.prs.slides), chapter, title, speech))
        return slide

    def panel_chart(self, s, title, series, x, y=2.65, w=5.8, h=2.9,
                    maximum=None, digits=0, method_colors=False):
        self.text(s, title, x, y-.42, w, .36, 15, bold=True)
        data = CategoryChartData()
        data.categories = SHORT
        for name, values, _ in series:
            data.add_series(name, values)
        chart = s.shapes.add_chart(XL_CHART_TYPE.BAR_CLUSTERED, Inches(x), Inches(y),
                                   Inches(w), Inches(h), data).chart
        chart.has_title = chart.has_legend = False
        chart._chartSpace.append(parse_xml(
            f'<c:spPr {nsdecls("c", "a")}><a:solidFill><a:srgbClr val="{BG}"/></a:solidFill>'
            '<a:ln><a:noFill/></a:ln></c:spPr>'))
        chart.category_axis.reverse_order = True
        chart.value_axis.minimum_scale = 0
        if maximum is not None:
            chart.value_axis.maximum_scale = maximum
            chart.value_axis.major_unit = 5 if maximum == 12 else maximum/2
        for axis in (chart.category_axis, chart.value_axis):
            axis.tick_labels.font.name = "Malgun Gothic"
            axis.tick_labels.font.size = Pt(11)
            axis.tick_labels.font.color.rgb = B.color(INK)
        plot = chart.plots[0]
        plot.gap_width = 60
        plot.has_data_labels = True
        labels = plot.data_labels
        labels.number_format = "0" if not digits else "0."+"0"*digits
        labels.position = XL_LABEL_POSITION.OUTSIDE_END
        labels.font.size = Pt(10)
        labels.font.color.rgb = B.color(INK)
        for i, (_, _, color) in enumerate(series):
            chart.series[i].format.fill.solid()
            chart.series[i].format.fill.fore_color.rgb = B.color(color)
            if method_colors:
                for j, point in enumerate(chart.series[i].points):
                    point.format.fill.solid()
                    point.format.fill.fore_color.rgb = B.color(B.COLORS[j])
        for i, (name, _, color) in enumerate(series):
            self.text(s, "■ "+name, x+i*(w/len(series)), y+h+.03,
                      w/len(series), .3, 10, color)
        return chart

    def takeaway(self, s, winner, meaning):
        self.text(s, winner, .72, 6.0, 11.9, .42, 17, "67E8F9", True)
        self.text(s, meaning, .72, 6.47, 11.9, .40, 12, MUTED)


def write_csv(path, records):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def finalize(out):
    """Package only this task's generated materials after real PowerPoint export."""
    assert (out/"OpenStd-FailureDetector-Decision-Briefing.pdf").is_file(), "Render the final PDF before packaging"
    manifest = {}
    for path in sorted(out.rglob("*")):
        if path.is_file() and path.name != "해시목록.json" and not path.name.startswith("~$"):
            manifest[path.relative_to(out).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    (out/"해시목록.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    archive = out.parent/"OpenStd-FailureDetector-Presenter-Handoff-20261108.zip"
    with ZipFile(archive,"w",compression=ZIP_DEFLATED,compresslevel=6) as z:
        for path in sorted(out.rglob("*")):
            if path.is_file() and not path.name.startswith("~$"):
                z.write(path, (Path(out.name)/path.relative_to(out)).as_posix())
    print(json.dumps(dict(archive=str(archive),files=len(manifest)+1,bytes=archive.stat().st_size),ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    if args.finalize_only:
        finalize(args.output)
        return
    if args.raw is None:
        parser.error("--raw is required when generating a presentation")
    raw, out = args.raw, args.output
    out.mkdir(parents=True, exist_ok=True)
    runs, records, validation = B.load_runs(raw)
    summary = B.summarize(runs)
    cells = {(r["fault"], r["profile"]): r for r in summary}
    get = lambda f, field: [cells[f, p][field] for p in P]
    d = PresenterDeck()
    add = d.add
    ch1, ch2, ch3, ch4 = "1. 방법론", "2. 실험 시나리오", "3. 시나리오별 결과 그래프", "4. 결론"

    s = add("OpenStd Failure Detector 선정 보고", "11개 시나리오 × 5개 방법론 × 10회 = 공식 550회", "FINAL REPORT",
        "오늘 결론은 OpenStd의 장애 감지 기반으로 SWIM+Lifeguard를 선정한다는 것입니다. 이 선택은 모든 조건에서 1등이라는 뜻이 아닙니다. 실제 측정값과 프로젝트가 우선하는 감지·복원 특성을 연결해서 설명하겠습니다. 운영 투입에는 고손실 오탐 개선과 승격 안전성 검증이 남아 있습니다.")
    d.text(s, "선정: SWIM + Lifeguard", .7, 2.65, 12, .9, 36, "67E8F9", True)
    d.text(s, "빠른 장애 감지와 일시 정지 후 복원을 함께 고려한 설계 선택", .73, 3.9, 11.8, .8, 24)
    d.text(s, "판단 근거: 실측 550회 + OpenStd의 모듈 경계\n적용 조건: 고손실 오탐 완화 · 안전한 승격 · 서버 통합 검증", .73, 5.25, 12, 1, 19, MUTED)

    s = add("발표 흐름: 측정에서 선택까지", "각 장은 마지막 선정 판단에 필요한 질문에 답합니다.", "CONTENTS",
        "먼저 다섯 방식과 설정을 소개하고, 동일 EC2 환경에서 어떤 장애를 넣었는지 설명합니다. 이후 모든 시나리오를 그래프로 비교합니다. 마지막에 어떤 지표에서 누가 나았는지와 왜 OpenStd는 SWIM 계열을 선택했는지 연결합니다.")
    d.table(s, ["구분", "핵심 질문"], [
        [ch1, "무엇을 비교하며, OpenStd는 어떤 특성을 우선하는가?"],
        [ch2, "장애를 어디에 어떻게 넣고, 무엇을 측정했는가?"],
        [ch3, "각 조건에서 누가 나았고, 차이가 없던 조건은 무엇인가?"],
        [ch4, "한 가지를 선택한다면 무엇이며, 근거와 남은 조건은 무엇인가?"],
    ], widths=[4.0, 8.1], height=3.65, size=17)

    s = add("OpenStd의 선정 기준", "프로젝트 관점의 설계 판단 · 사전 등록된 SLA 또는 통계적 종합 점수가 아님", ch1,
        "이번 선정에서는 빠른 실제 종료 감지와 일시적인 멈춤 이후 상태 복원을 함께 우선합니다. 이 우선순위는 프로젝트 관점의 설계 판단이며 사전 등록된 SLA나 실험으로 측정된 점수가 아닙니다. FD는 관측과 의심을 보고하고, 승격은 복제 계층이 결정한다는 프로젝트 경계도 고려합니다. 고손실에서 DEAD가 절대 없어야 한다는 조건이라면 현재 SWIM 설정은 탈락입니다.")
    d.table(s, ["선정 기준", "근거 / 판단 범위"], [
        ["빠른 장애 인지", "Crash 탐지율과 지연을 함께 비교"],
        ["일시 정지 후 복원", "정지 구간에서의 판정 + 재개 후 node1 최종 상태"],
        ["프로젝트 통합", "기존 SWIM baseline 활용 가능 · Lifeguard/UDP 통합은 추가 구현 필요"],
        ["승격 책임 분리", "FD는 신호 제공 · A1/A2가 승격/재구성의 안전성을 판단"],
        ["배포 완료 조건", "고손실 오탐 개선 · 실제 서버/승격 통합 테스트 통과"],
    ], widths=[3, 9.1], height=3.7, size=16)

    s = add("비교 방법론 5종", "실제 제품 binary가 아닌 product-inspired 구현 비교", ch1,
        "SWIM은 direct와 indirect probe 및 suspicion을, Redis는 ping/pong과 실패 보고를 사용합니다. Phi는 heartbeat 간격을 점수로 바꾸고, Ignite는 TCP ring과 검증 경로를, Aerospike는 TCP mesh와 간소화된 합의를 사용합니다. Aerospike의 실제 Paxos나 각 제품의 전체 동작을 재현한 벤치마크는 아닙니다.")
    d.table(s, ["방법론", "감지와 판정 원리", "구현 범위"], [
        [N[0], "direct/indirect probe · suspicion · local health", "전용 UDP 실험 경로"],
        [N[1], "PING/PONG · timeout · 독립 실패 보고", "실험용 membership 판단"],
        [N[2], "heartbeat 도착 이력 · Phi 점수 · gossip relay", "observer-local 상태"],
        [N[3], "TCP ring heartbeat · helper 검증", "제품 native discovery와는 다름"],
        [N[4], "TCP mesh heartbeat · epoch/majority removal", "간소화 합의 · 실제 Paxos 아님"],
    ], widths=[2.8, 5.8, 3.5], height=3.6, size=14)

    s = add("설정 차이까지 공개하고 비교", "탐지 속도에는 알고리즘뿐 아니라 timeout/주기 선택도 영향을 줍니다.", ch1,
        "SWIM은 probe 200ms와 timeout 100ms에 suspicion을 더합니다. Redis timeout은 15초, Phi threshold는 8, Ignite neighbor timeout은 5초, Aerospike timeout은 1.5초입니다. 따라서 결과는 이 설정들의 동작 비교입니다. 동일 목표로 조정한 알고리즘 우열 비교로 해석하지 않습니다. 특히 10초 정지는 Redis timeout보다 짧습니다.")
    d.table(s, ["방법론", "주기", "주요 판정 설정"], [
        [N[0], "probe 200ms", "probe timeout 100ms + 적응형 suspicion"],
        [N[1], "PING 1초", "node timeout 15초"],
        [N[2], "heartbeat 1초", "Phi threshold 8 · local pause guard 5초"],
        [N[3], "heartbeat 1초", "neighbor timeout 5초 + 검증 대기 1초"],
        [N[4], "heartbeat 150ms", "heartbeat timeout 1.5초 + 간소화 합의"],
    ], widths=[3, 2.2, 6.9], height=3.5, size=16)
    d.text(s, "10초 정지에서 DEAD가 없었다는 사실은, 정지를 감지하고 복원했다는 의미와 다릅니다.", .7, 6.3, 12, .5, 16, "F9C66C")

    s = add("실험 환경과 표본", "EC2 m6gd.medium 5대 · observer=node0 / 판정 대상=node1", ch2,
        "서울 리전의 서로 다른 EC2 5대에서 실제 소켓으로 실행했습니다. 각 방법과 조건마다 seed 1부터10까지 실행해 55조합 550회를 확보했습니다. 가역 장애는 10초이며, fio 파일 준비는 detector 시작 전에 끝냈습니다. 대상별 이벤트와 최종 상태를 교차 검증했습니다. 안전한 서비스 failover 자체를 실험한 것은 아닙니다.")
    d.table(s, ["항목", "조건"], [
        ["표본", "11 시나리오 × 5 방법 × 10회 = 550회 · 조합당 n=10"],
        ["시간", "준비 후 warm-up 15초 · 가역 장애 10초 · 복구 관측 최소 15초"],
        ["통신", "SWIM/Redis/Phi UDP · Ignite/Aerospike TCP heartbeat + UDP control"],
        ["저장장치", "instance-store 로컬 NVMe · /dev/nvme1n1"],
        ["유효성", "node1 상태 전이/최종 상태/지연 대조 · 실제 프레임 · TCP readiness"],
    ], widths=[2.4, 9.7], height=3.65, size=16)

    s = add("11개 실험 시나리오", "정상 · 영구 종료 · 네트워크 변동 · 프로세스 정지 · 자원 부하", ch2,
        "조건은 정상, 실제 종료, 두 손실률, 지연, 대상과 관측자의 일시 정지, 대상과 관측자의 CPU 제한, CPU 경합, I/O 경합입니다. 네트워크 조건은 node1의 실험 포트 egress에 적용합니다. CPU 제한은 10%, CPU 경합은 worker 한 개, I/O는 direct libaio QD32입니다.")
    brief = ["주입 없음", "node1 SIGKILL", "node1 egress UDP/TCP 15% loss", "node1 egress UDP/TCP 45% loss",
             "node1 egress 250ms ± 50ms", "node1 SIGSTOP / SIGCONT", "node0 SIGSTOP / SIGCONT",
             "node1 CPUQuota=10%", "node0 CPUQuota=10%", "node1 matrixprod / worker 1", "node1 NVMe fio / direct / QD32"]
    d.table(s, ["번호", "시나리오", "주입 조건", "비교 관점"], [
        [str(i+1), B.TITLES[i], brief[i], "종료 감지 속도" if f=="crash" else
         "무장애 오판" if f=="normal" else "생존 노드 판정/복원" if f in ("loss15","loss45","delay") else
         "정지/재개 시 반응" if "pause" in f else "부하 시 상태 변화"] for i,f in enumerate(F)
    ], y=2.2, height=4.55, widths=[.6, 2.8, 5.1, 3.6], size=12)

    s = add("그래프를 읽는 기준", "횟수는 실행 수 / 10 · 한 실행에 이벤트가 여러 번 있어도 한 번으로 집계", ch2,
        "Crash에서는 DEAD 탐지율을 먼저 확인하고 탐지된 실행의 지연을 비교합니다. 정상이나 네트워크 변동에서는 생존 노드의 DEAD 판정과 SUSPECT를 봅니다. 일시 정지는 정지 중 판정과 최종 ALIVE를 함께 봅니다. 통신량은 observer application bytes만 포함합니다. 차이가 없는 조건은 공동 결과로 남기며 임의의 1등을 정하지 않습니다.")
    d.table(s, ["조건", "좋은 결과를 판단하는 기준"], [
        ["Crash", "탐지율을 유지하면서 최초 node1 DEAD 지연이 짧음"],
        ["정상 / 네트워크 변동", "생존 노드 DEAD가 적음 · SUSPECT와 통신량을 보조 비교"],
        ["10초 일시 정지", "정지 중 반응 + 재개 후 ALIVE 복귀를 각각 확인"],
        ["CPU / I/O 부하", "상태 변화 관측 · 차이가 없으면 우열 없음"],
        ["주의", "p95는 n=10의 경험 분위수 · 최종 ALIVE는 서비스 요청 성공과 다름"],
    ], widths=[3.1, 9], height=3.6, size=16)

    graph_rows = []
    scenario_notes = []
    for i, fault in enumerate(F):
        cell = [cells[fault,p] for p in P]
        if fault=="normal":
            verdict = "판정 안정성 공동 결과: 전 방법 SUSPECT/DEAD 0/10"
            nuance = "observer 통신량은 Phi가 가장 낮음. 정상 판정만으로 한 방법을 우승자로 정하지 않음."
            speech = "정상에서는 다섯 방식 모두 SUSPECT와 DEAD가 없었습니다. 판정 안정성은 공동 결과입니다. 통신량만 보면 Phi가 가장 낮고 Aerospike가 가장 높습니다. 이는 observer 한 노드의 application traffic 비교입니다."
        elif fault=="crash":
            verdict = "종료 탐지 속도 1위: Aerospike 1.54초 · 전 방법 10/10 탐지"
            nuance = "SWIM 3.20초 · Ignite 5.34초 · Redis 15.42초 · Phi 18.62초. 설정 차이를 포함한 결과."
            speech = "실제 종료는 모든 방법이 10회 전부 탐지했습니다. 중앙값 순서는 Aerospike 1.54, SWIM 3.20, Ignite 5.34, Redis 15.42, Phi 18.62초입니다. 이 지표의 1등은 Aerospike입니다. SWIM은 가장 빠르지 않지만 Redis와 Phi보다 수초 대에서 감지합니다. timeout 차이도 함께 읽어야 합니다."
        elif fault=="loss15":
            verdict = "DEAD 억제는 공동 결과 · SWIM만 SUSPECT 8/10"
            nuance = "최종 상태는 모두 ALIVE. Redis/Phi/Ignite/Aerospike는 SUSPECT도 0/10."
            speech = "15% 손실에서는 전 방법의 DEAD가 0입니다. 그러나 SWIM에서만 SUSPECT가 8회 발생해 의심 안정성은 다른 네 방식이 낫습니다. SWIM에 유리한 결과로 해석하지 않습니다. 모든 실행의 최종 상태는 ALIVE입니다."
        elif fault=="loss45":
            verdict = "DEAD 억제 우세: Redis / Phi / Ignite 0/10"
            nuance = "SWIM과 Aerospike는 각각 DEAD 5/10. SWIM 고손실 오탐 개선은 필수 적용 조건."
            speech = "45% 손실은 SWIM 선택의 가장 큰 반대 근거입니다. Redis, Phi, Ignite는 DEAD가 0회인 반면 SWIM과 Aerospike는 각각 5회였습니다. 이 조건에서 SWIM이 우수하다고 말할 수 없습니다. 따라서 현재 설정을 곧바로 운영 승인하지 않고, 고손실 오탐 완화를 채택 완료 조건으로 남깁니다."
        elif fault=="delay":
            ratio=cells[fault,P[0]]["app_kbps_p50"]/cells["normal",P[0]]["app_kbps_p50"]
            verdict = f"DEAD는 모두 0/10 · SWIM 통신량은 정상 대비 {ratio:.1f}배"
            nuance = "SWIM SUSPECT 10/10, 다른 네 방식 0/10. 이 지연 조건에서는 SWIM의 의심·통신 비용이 불리."
            speech = f"지연 조건에서 DEAD는 전부 0회입니다. SWIM은 모든 실행에서 SUSPECT가 있었고 통신량은 정상 대비 {ratio:.1f}배로 증가했습니다. 판정의 최종 결과는 같아도 과정과 비용은 다릅니다. SWIM의 통신 증가 원인은 후속 점검 대상입니다."
        elif fault=="cpu-pause-target":
            verdict = "정지 중 DEAD 탐지 + 재개 후 ALIVE 복귀: SWIM만 10/10"
            nuance = "Redis/Phi는 DEAD 미발생 · Ignite/Aerospike는 재개 후 DEAD 유지. 자동 재가입 구현 차이 포함."
            speech = "SWIM은 10초 정지 중 DEAD를 선언하고 재개 후 10회 모두 ALIVE로 복귀했습니다. Redis와 Phi는 DEAD를 선언하지 않았습니다. 따라서 이 둘을 복구 성공으로 세면 안 됩니다. Ignite와 Aerospike는 DEAD 후 복귀하지 않았는데, 실험 구현에 자동 재가입이 없는 영향도 포함됩니다. 빠른 반응과 복원을 함께 원하는 설계에서 SWIM을 선택한 핵심 근거입니다."
        elif fault=="cpu-pause-observer":
            verdict = "SUSPECT 억제 우세: SWIM / Redis / Phi 0/10"
            nuance = "Ignite SUSPECT 10/10 · Aerospike 9/10. DEAD는 전 방법 0/10, 최종 상태도 모두 ALIVE."
            speech = "관측자가 멈췄다가 재개됐을 때 SWIM, Redis, Phi는 SUSPECT가 없었습니다. Ignite는 10회, Aerospike는 9회 있었습니다. 모두 DEAD 없이 최종 ALIVE라 치명적 판정 차이는 없었지만 관측자 지연에 대한 의심 억제 측면은 앞의 세 방식이 나았습니다."
        else:
            verdict = "관측된 상태 변화 차이 없음: 전 방법 SUSPECT/DEAD 0/10"
            nuance = "이 부하에서 최종 ALIVE 10/10. 부하 강도·서비스 지연 한계까지 견딘다는 증거는 아님."
            speech = "이 조건에서는 모든 방법에서 SUSPECT와 DEAD가 없고 최종 ALIVE가 10회입니다. 따라서 이 시나리오의 우승자는 없습니다. 주입이 실행됐다는 사실과 detector나 실제 서비스가 충분히 지연됐다는 것은 다르므로 자원 장애 전반에 강하다고 일반화하지 않습니다."
        s=add(f"{i+1:02d}  {B.TITLES[i]}", B.CONDITIONS[i], ch3, speech)
        if fault=="crash":
            d.panel_chart(s, "최초 DEAD 지연 (초, 낮을수록 짧음)", [
                ("p50", get(fault,"dead_p50_s"), TEAL), ("p95",get(fault,"dead_p95_s"),"3B82F6")], .65, maximum=22,digits=2)
            d.panel_chart(s,"실제 종료 탐지 실행 수 / 10",[("DEAD 탐지",get(fault,"dead_runs"),TEAL)],6.85,maximum=12)
        else:
            d.panel_chart(s, "SUSPECT / DEAD 발생 실행 수 (각 n=10)", [
                ("SUSPECT",get(fault,"suspect_runs"),AMBER), ("DEAD",get(fault,"dead_runs"),RED)],.65,maximum=12)
            if fault in ("normal","loss15","delay"):
                d.panel_chart(s,"observer 통신량 중앙값 (kbit/s)",[("송수신 application bytes",get(fault,"app_kbps_p50"),TEAL)],6.85,digits=2,method_colors=True)
            else:
                d.panel_chart(s,"관측 종료 시 ALIVE 실행 수 / 10",[("최종 ALIVE",get(fault,"final_alive_runs"),TEAL)],6.85,maximum=12)
        d.takeaway(s,verdict,nuance)
        scenario_notes.append(dict(fault=fault,title=B.TITLES[i],criterion_verdict=verdict,qualification=nuance,speech=speech))
        for j,r in enumerate(cell):
            graph_rows.append(dict(scenario=fault,method=P[j],n=10,suspect_runs=r["suspect_runs"],dead_runs=r["dead_runs"],
                final_alive_runs=r["final_alive_runs"],dead_p50_s=r["dead_p50_s"],dead_p95_s=r["dead_p95_s"],observer_app_kbps_p50=r["app_kbps_p50"],
                official_run_ids=";".join(run["run_id"] for run in runs if run["fault"]==fault and run["profile"]==P[j])))

    s=add("전체 시나리오를 종합하면", "한 지표의 1등이 모든 운영 요구에서 1등인 것은 아닙니다.",ch4,
        "종합표는 실측만 모았습니다. 가장 빠른 Aerospike는 고손실 DEAD가 있고, 보수적인 Redis와 Phi는 긴 탐지 시간이 있습니다. Ignite는 현재 조건에서 속도와 고손실 판정의 절충이 좋아 유력한 대안입니다. SWIM은 빠른 감지와 일시 정지 후 복귀를 함께 보여줬지만 고손실 오탐과 통신 비용을 개선해야 합니다.")
    d.table(s,["방법론","Crash p50","loss45 DEAD","정지 중 DEAD","재개 후 ALIVE"],[
        [N[j],B.fmt(cells["crash",p]["dead_p50_s"])+"초",f"{cells['loss45',p]['dead_runs']}/10",
         f"{cells['cpu-pause-target',p]['dead_runs']}/10",f"{cells['cpu-pause-target',p]['final_alive_runs']}/10"] for j,p in enumerate(P)
    ],widths=[3.0,2.0,2.0,2.5,2.6],height=3.65,size=15)
    d.text(s,"Redis/Phi의 최종 ALIVE는 DEAD 후 복귀가 아니라 DEAD 미발생입니다.",.7,6.35,12,.4,15,"F9C66C")

    s=add("왜 OpenStd는 SWIM + Lifeguard를 선택했나", "실측 근거와 프로젝트의 설계 우선순위를 연결한 판단",ch4,
        "선정 이유는 세 가지입니다. 첫째 Crash 중앙값 3.20초로 빠른 인지가 가능했습니다. 둘째 10초 정지에서 반응한 뒤 10회 모두 ALIVE로 돌아왔습니다. 셋째 프로젝트에 SWIM baseline이 있고 FD와 승격을 분리하는 구조와 연계할 수 있습니다. 셋째는 실험 결과가 아닌 코드와 아키텍처에 근거한 통합 판단입니다. Lifeguard와 실제 UDP 통합이 이미 완성됐다는 의미는 아닙니다.")
    d.table(s,["선정 이유","근거","구분"],[
        ["빠른 실제 종료 감지","Crash 10/10 탐지 · p50 3.20초","실측"],
        ["멈춤과 복귀를 모두 처리","대상 정지: DEAD 10/10 → 최종 ALIVE 10/10","실측"],
        ["관측자 정지 영향 억제","observer pause: SUSPECT/DEAD 0/10","실측 · Redis/Phi도 동일"],
        ["기존 구조와 통합","SWIM baseline 존재 · FD 신호와 승격 책임 분리","프로젝트 설계 판단"],
        ["숨기지 않는 약점","loss45 DEAD 5/10 · delay 통신량 11.5배","미해결 적용 조건"],
    ],widths=[2.9,6.0,3.2],height=3.65,size=14)

    s=add("다른 방식을 선택하지 않은 이유", "상대 방식의 강점을 인정한 상태에서 OpenStd의 우선순위로 결정",ch4,
        "Aerospike는 속도 1등이지만 고손실 판정과 재가입 범위를 보완해야 합니다. Redis와 Phi는 고손실에서 더 안정적이지만 현재 설정의 실제 종료 인지가 15초 이상입니다. Ignite는 가장 강한 대안입니다. 이번에는 빠른 멈춤 감지와 복원을 우선해 SWIM을 택했습니다. 고손실에서 DEAD 0회를 필수로 두면 이번 선정은 달라져야 하며 Ignite를 우선 재검토합니다. 재가입 부재는 제품의 한계가 아닌 실험 구현의 범위입니다.")
    d.table(s,["방법론","인정하는 강점","이번 선택에서의 판단"],[
        ["Aerospike","가장 빠른 Crash 1.54초","loss45 DEAD 5/10 · 이번 구현의 재가입 미지원"],
        ["Redis","loss45 DEAD 0/10","Crash 15.42초 · 빠른 인지 우선순위와 거리"],
        ["Phi","loss45 DEAD 0/10 · 낮은 observer 통신량","Crash 18.62초 · 이번 설정에서 인지가 가장 늦음"],
        ["Ignite","Crash 5.34초 + loss45 DEAD 0/10","유력 대안 · 재가입 범위와 통합 비용을 함께 검토"],
    ],widths=[2,4.5,5.6],height=3.4,size=15)
    d.text(s,"고손실에서 DEAD 0회를 필수 조건으로 삼으면 현재 SWIM 설정은 부적합하며, Ignite 등을 재검토해야 합니다.",.7,6.25,12,.65,15,"F9C66C")

    s=add("선정 후 적용 구조와 완료 조건", "다음 단계의 설계안 · 이번 실험에서 서비스 failover 안전성까지 검증한 것은 아님",ch4,
        "적용은 SWIM 신호를 바로 승격 명령으로 쓰지 않는 구조입니다. FD는 상태와 근거를 보고하고 복제 계층은 별도 안전 조건을 확인해 승격합니다. epoch 증가만으로 fencing이 완료되는 것도 아닙니다. 고손실 오탐 개선, 실제 서버 통합, 승격과 기존 owner 차단을 포함한 테스트가 필요합니다. 구체적인 허용 오탐률과 RTO는 팀이 합의해야 합니다.")
    for x,title,body in [( .75,"SWIM + Lifeguard","상태·의심·근거 보고"),(4.95,"A1 / A2 복제 계층","안전 조건·기존 owner 차단"),(9.05,"승격 / 재구성","검증된 권한으로 수행")]:
        shape=s.shapes.add_shape(1,Inches(x),Inches(2.7),Inches(3.4),Inches(1.35))
        shape.fill.solid(); shape.fill.fore_color.rgb=B.color("173449")
        d.text(s,title,x+.1,2.88,3.2,.4,18,"67E8F9",True)
        d.text(s,body,x+.1,3.45,3.2,.4,13)
    d.text(s,"→",4.25,3.12,.55,.5,25); d.text(s,"→",8.45,3.12,.55,.5,25)
    d.text(s,"완료 조건  ① 고손실 오탐 완화  ② 실제 서버 통합  ③ 승격/fencing 안전성 검증\n허용 오탐률·복구 시간 목표는 합의 후 검증하며, 이번 측정으로 충족됐다고 주장하지 않습니다.",.8,4.9,11.8,1.2,19)

    s=add("최종 결론", "선정한 한 가지와 선택 이유, 남은 적용 조건을 함께 명시",ch4,
        DECISION+" 실제 종료를 중앙값 3.20초에 인지하고 일시 정지 후 10회 모두 ALIVE로 돌아온 관측을 근거로 빠른 감지와 복원을 우선했습니다. "+RELEASE+" 가장 우수한 알고리즘을 증명한 것이 아니라 프로젝트 우선순위에 따른 기반 설계 선정입니다.")
    d.text(s,"OpenStd의 기반 Failure Detector\nSWIM + Lifeguard 선정",.75,2.45,11.8,1.4,33,"67E8F9",True)
    d.text(s,"근거: Crash 3.20초 · 대상 일시 정지 후 ALIVE 복귀 10/10\n판단: 빠른 감지와 일시 장애 후 복원을 함께 우선",.8,4.25,11.6,1.0,21)
    d.text(s,"적용 조건: 고손실 오탐 완화 + 안전한 승격 + 실제 서버 통합 검증",.8,5.8,11.8,.7,20,"F9C66C",True)

    s=add("발표에서 지켜야 할 해석 범위", "반대 근거와 미검증 범위를 공개해야 선정 결론을 방어할 수 있습니다.",ch4,
        "한계를 정확히 설명합니다. 실험은 product-inspired이며 조합당 10회입니다. 실제 서비스 요청의 성공률과 전체 클러스터 비용은 측정하지 않았습니다. 자원 부하가 발생했다는 사실로 서비스 내성을 증명하지 않습니다. JAR은 종료 race 수정 전후 2개이며 SWIM 종료 예외 2건은 결과 출력 후에 발생했습니다. 이 사항들을 자료에 보존했습니다.")
    d.text(s,"01  제품 본체 벤치마크 아님 · 설정/주기 차이를 포함한 비교\n02  각 조합 n=10 · 0/10도 오류 확률 0%를 증명하지 않음\n03  멤버 자동 재가입 미구현이 Ignite/Aerospike의 DEAD 유지에 영향\n04  자원 경합 주입 ≠ 서비스 지연 영향 검증 · 관측은 node0의 node1 판정\n05  공식 JAR 2종: 18회 / 532회 · 추가 변경은 TCP 종료 race 처리\n06  SWIM 173·277번: 결과 출력 뒤 UDP 종료 예외 · 측정 영향 징후 없음",.75,2.35,11.9,4,19)

    s=add("발표자에게 넘기는 근거 자료", "슬라이드 → 집계값 → 공식 run_id → 원시 이벤트로 추적",ch4,
        "발표자는 PPT와 PDF 외에 슬라이드별 대본, 예상 질문 답변, 55개 조합의 지표표와 550개 공식 실행표를 받습니다. evidence에는 공식 실행의 원시 결과와 이벤트를 담았습니다. 과거 수정 전 표본 40개와 중복 재시도 2개는 공식 집계에서 빠졌고 체크포인트에 이력이 남습니다. 모든 수치의 근거를 다시 확인할 수 있습니다.")
    d.table(s,["자료","발표자 용도"],[
        ["발표대본.md / 예상질문답변.md","슬라이드별 설명과 반론 대응"],
        ["시나리오별판단.csv / 그래프근거.csv","조건별 비교 기준과 그래프 55개 셀 수치"],
        ["official-runs.csv / summary.csv","공식 550개 표본과 조합별 집계"],
        ["evidence/","완료 마커 · 검증 · plan/status · 공식 원시 이벤트"],
        ["재현 소스 / 해시목록.json","집계/생성 코드와 패키지 파일 무결성 확인"],
    ],widths=[5.1,7],height=3.55,size=16)

    filename="OpenStd-FailureDetector-Decision-Briefing"
    ppt=out/(filename+".pptx")
    d.prs.save(ppt)
    write_csv(out/"summary.csv",summary)
    write_csv(out/"official-runs.csv",[{k:v for k,v in r.items() if k!="result"} for r in runs])
    write_csv(out/"그래프근거.csv",graph_rows)
    write_csv(out/"시나리오별판단.csv",[{k:v for k,v in r.items() if k!="speech"} for r in scenario_notes])
    script=["# 발표 대본 — OpenStd Failure Detector 선정", "", "> 권장 발표 12–15분. 상황에 따라 자원 부하 4장은 묶어서 설명합니다.", "", DECISION, RELEASE, ""]
    for page,chapter,title,speech in d.script:
        script.extend([f"## {page:02d}. {title}","",f"분류: {chapter}","",speech,"",f"근거: {('그래프근거.csv 및 evidence/runs/의 해당 조건 50개' if chapter==ch3 else 'summary.csv, 시나리오별판단.csv 및 프로젝트 구조 문서')}",""])
    (out/"발표대본.md").write_text("\n".join(script),encoding="utf-8")
    faq=[
        ("45% 손실에서 5/10 오판인데 왜 SWIM을 고르나요?", "이번 선정은 빠른 감지와 일시 정지 후 복원을 우선한 기반 설계 선택입니다. 고손실 결과는 SWIM의 명백한 약점이며 현재 설정의 운영 승인을 뜻하지 않습니다. 손실 환경에서 DEAD 0회를 필수로 두면 이번 선택은 달라져야 하고 Ignite 등을 재검토해야 합니다. 개선 완료·안전 승격 검증이 적용 조건입니다."),
        ("그럼 실험이 SWIM이 제일 좋다는 증거인가요?", "아닙니다. Crash 속도는 Aerospike가 가장 짧고, loss45의 DEAD 억제는 Redis/Phi/Ignite가 우세했습니다. SWIM 선정은 이 실측과 기존 SWIM baseline, 감지-승격 책임 경계를 바탕으로 빠른 감지와 복원을 우선한 프로젝트 판단입니다. 종합 점수나 사전 등록된 SLA를 실험이 산출한 것은 아닙니다."),
        ("Ignite가 5.34초와 고손실 DEAD 0/10이면 더 좋은 선택 아닌가요?", "충분히 합리적인 대안입니다. 이번에는 정지 중 빠른 반응과 이후 ALIVE 복원을 함께 우선했고 기존 SWIM 코드의 통합 경로도 고려했습니다. Ignite 실험 구현의 재가입 부재는 실제 제품 한계로 일반화하면 안 됩니다. 고손실 판정 억제가 최우선이면 Ignite를 우선 재검토해야 합니다."),
        ("Aerospike가 가장 빠른데 왜 선택하지 않았나요?", "Crash p50 1.54초로 속도는 우세했습니다. 그러나 loss45에서 DEAD 5/10이고 이번 구현에는 자동 재가입이 없습니다. 단일 지표 최단 시간보다 빠른 감지와 복원까지 함께 보는 기준으로 선택했습니다."),
        ("Redis/Phi의 일시 정지 후 ALIVE 10/10도 복원 성공 아닌가요?", "해당 10초 정지에서는 DEAD를 선언하지 않았습니다. DEAD 후 복귀와 상태가 계속 ALIVE였던 것을 구분합니다. Redis node timeout은 15초로 주입된 정지보다 길었습니다. 이를 모두 같은 복구 성능으로 집계하지 않습니다."),
        ("timeout이 다르면 비교가 불공정한 것 아닌가요?", "이번 결과는 지정된 독립 설정들의 동작 비교로 유효합니다. 알고리즘 자체의 우열은 이 자료로 확정하지 않습니다. 같은 탐지 지연 또는 허용 오탐 목표로 설정을 조정한 추가 비교가 필요하며, 발표에 설정표를 공개합니다."),
        ("10회로 0% 오탐을 말할 수 있나요?", "말할 수 없습니다. 0/10은 관측된 실행에서 0건이라는 뜻입니다. 독립 Bernoulli 가정 아래 Wilson 95% 구간 상한은 약 27.8%이고, 실제 반복의 독립성도 따로 고려해야 합니다. p95 역시 작은 표본의 경험 분위수로만 제시합니다."),
        ("일시 정지 후 DEAD 유지가 실험 버그인가요?", "대상별 이벤트와 최종 상태는 일치합니다. Ignite/Aerospike 구현은 제거된 멤버를 단순 heartbeat로 재가입시키지 않아 DEAD가 유지됩니다. 그래프는 이번 구현의 복원 범위를 보여주며 상용 제품의 복구 불능을 뜻하지 않습니다."),
        ("I/O/CPU에서 모두 정상인데 어떤 방식이 더 강한가요?", "그 조건에서는 우열이 관측되지 않았습니다. fio 장치/QD/IOPS/시간은 검증됐지만 detector가 실제 디스크 대기에 놓였다는 증거나 서비스 GET/SET 지연은 측정하지 않았습니다. 자원 장애 전반의 내성을 증명한 것으로 말하지 않습니다."),
        ("통신량은 클러스터 전체 네트워크 비용인가요?", "node0의 application 송수신 bytes를 실제 실행 시간으로 나눈 값입니다. TCP/IP 헤더, 커널 재전송, 다른 노드 트래픽은 포함하지 않습니다. 전체 클러스터 비용 또는 확장성 실측으로 일반화하지 않습니다."),
        ("이번 실험으로 안전한 failover도 검증됐나요?", "아닙니다. FD 상태는 승격 안전성 증명이 아닙니다. A1/A2의 권한 결정, 기존 owner의 요청 차단/fencing, 실제 클라이언트 요청 성공과 데이터 유실을 별도로 검증해야 합니다. epoch만 올리는 것으로 안전성을 주장하지 않습니다."),
        ("이미 OpenStd 서버에 Lifeguard가 들어가 있나요?", "현재 저장소의 cache-core/failure는 SWIM baseline이며 실제 UDP adapter와 Lifeguard local-health 확장은 범위 밖이라고 명시돼 있습니다. 실험은 failure-detector-lab 코드이므로 이식과 서버 통합을 완료해야 합니다."),
        ("실패한 시도를 버리고 좋은 것만 고른 것 아닌가요?", "대상 계측 수정 전 40개 행을 pre-target-fix로 제외하고 수정 후 최초 증거 유효 실행을 사용했습니다. loss45 Aerospike sequence18 attempt3의 DEAD 오판도 포함했습니다. 잘못된 outcome gate가 만든 attempt4/5는 중복 2개로 제외했고 이력을 보존했습니다. passed는 알고리즘 성공이 아니라 증거 유효성을 뜻합니다."),
        ("종료 예외와 JAR 두 버전은 결과를 무효로 만드나요?", "공식 18회/532회의 JAR SHA-256을 표에 공개했습니다. 추가 변경은 TCP 종료 race 처리였고, SWIM 173/277의 UDP 종료 예외 2건은 RESULT_JSON 뒤 executor 종료 중 발생했습니다. 핵심 측정에 영향 징후는 없지만 예외 0건이라고 발표하면 안 됩니다. 전체 원시 로그를 보존했습니다."),
    ]
    (out/"예상질문답변.md").write_text("# 예상 질문과 답변\n\n"+"\n\n".join(f"## {i+1}. {q}\n\n{a}" for i,(q,a) in enumerate(faq)),encoding="utf-8")
    brief = f"""# 발표자용 핵심 요약

## 30초 결론

{DECISION} 실제 종료를 10/10 탐지했고 중앙값은 3.20초였으며, 대상의 일시 정지 뒤 10회 모두 ALIVE로 복귀했다.
빠른 감지와 복원을 함께 우선한 프로젝트의 기반 설계 선택이다. {RELEASE}

## 반드시 설명할 세 가지

1. **조건별 1등은 다르다.** Crash 속도는 Aerospike 1.54초. loss45 DEAD 억제는 Redis/Phi/Ignite 0/10.
2. **SWIM 선택 이유는 감지와 복원을 함께 우선했기 때문이다.** 대상 정지에서 DEAD 후 ALIVE 복귀 10/10을 보였고, 기존 SWIM baseline과 FD/승격 책임 경계를 활용할 수 있다.
3. **현재 설정 그대로의 운영 승인이 아니다.** loss45 DEAD 5/10, delay 통신량 약 11.5배라는 반대 근거를 공개하고 개선과 통합 검증을 요구한다.

## 발표자가 하지 말아야 할 주장

- “모든 시나리오에서 SWIM이 가장 좋았다.” → 결과와 다름.
- “0/10이므로 오탐 확률 0%다.” → 작은 표본의 관측 결과일 뿐.
- “Ignite/Aerospike 제품은 일시 정지 후 복구하지 못한다.” → 이번 실험 구현의 자동 재가입 범위 차이.
- “CPU/I/O 장애에 모두 강하다.” → 실제 detector/서비스 자원 대기 영향은 별도 계측 필요.
- “안전한 자동 failover가 검증됐다.” → 감지와 승격 안전성은 별개.
- “OpenStd 서버의 Lifeguard 통합이 완료됐다.” → 현재 baseline과 실험 코드의 통합 작업이 남음.

## 선택이 바뀌는 조건

고손실에서도 DEAD 0회가 필수 요구라면 현재 SWIM 설정은 부적합하다. 이 경우 Ignite 등을 우선 재검토한다.
선정 우선순위는 실험 이후 프로젝트 관점에서 정리했으며 사전 등록 SLA 또는 정규화된 종합 순위로 제시하지 않는다.
"""
    (out/"발표자용-핵심요약.md").write_text(brief,encoding="utf-8")
    readme=f"""# OpenStd Failure Detector — 발표자 인계 자료

## 최종 전달 문장

> {DECISION} 실제 종료 감지 중앙값 3.20초와 대상 일시 정지 후 ALIVE 복귀 10/10을 근거로 빠른 감지와 복원을 우선했다. {RELEASE}

이는 프로젝트의 기반 설계 선정이며 운영 투입 승인 또는 알고리즘 전체 우열의 증명은 아닙니다.

## 발표 순서

- 1–2쪽: 결론 예고와 목차
- 3–5쪽: 방법론 — 선정 기준, 감지 원리, 설정값
- 6–8쪽: 실험 시나리오 — 환경, 11개 장애, 지표 정의
- 9–19쪽: 각 시나리오별 결과 그래프 — 조건별 우세/동률과 의미
- 20–24쪽: 결론 — 종합 결과, SWIM 선정 근거, 대안, 적용 조건, 최종 문장
- 25–26쪽: 해석 범위와 근거 자료

## 먼저 읽을 파일

1. `{filename}.pptx`: 편집 가능한 실제 그래프 22개와 발표자 노트 포함
2. `{filename}.pdf`: 최종 PowerPoint 렌더링본
3. `발표대본.md`: 슬라이드별 대본 (권장 12–15분)
4. `예상질문답변.md`: 고손실 오판·Ignite 대안·설정 차이·통계·승격 안전성 등 14문항
5. `시나리오별판단.csv`: 조건별 비교 기준, 우세 결과와 해석 한계
6. `그래프근거.csv`, `summary.csv`, `official-runs.csv`: 55개 셀과 공식 550회

## 선정의 논리

- 실측: Aerospike는 Crash 속도 1위. Redis/Phi/Ignite는 loss45 DEAD 0/10으로 SWIM보다 우세.
- 실측: SWIM은 Crash 3.20초, 대상 10초 정지 중 DEAD 판정 후 10/10 ALIVE 복귀.
- 프로젝트 판단: 빠른 감지와 일시 정지 후 복원을 함께 우선하며 기존 SWIM baseline 통합 경로를 고려.
- 반대 근거: SWIM loss45 DEAD 5/10, delay 통신량 정상 대비 약 11.5배. 반드시 공개.
- 필수 조건: 고손실 오탐 개선, 실제 서버 통합, A1/A2 승격/fencing 안전성 검증.
- 선택이 바뀌는 조건: 고손실 DEAD 0회가 필수라면 현재 SWIM 설정은 부적합. Ignite 등을 재검토.
- 실험 이후 정리한 설계 우선순위이며 사전 등록된 SLA나 검증된 통합 SLO라고 주장하지 않습니다.

## 근거와 재현

`evidence/`에는 공식 run_id만의 node0 원시 결과/이벤트/manifest, 관련 node1 장애/fio 증거,
완료·최종 검증·plan/status를 담았습니다. 제외된 실행의 원시 파일은 이 묶음에서 제외하며 status 이력은 보존합니다.
`source_reference/`는 현재 작업 트리의 집계/생성/검증 코드 및 관련 구현·설정입니다.
공식 실행 당시 고정된 소스 스냅샷/커밋 증명이나 두 JAR binary를 제공하는 것은 아닙니다.
JAR SHA-256은 `official-runs.csv`와 `보고서검증.json`에 공개합니다.
`해시목록.json`으로 PDF/그림을 포함한 파일 무결성을 확인할 수 있습니다.

Python 3.10+와 python-pptx가 있는 환경에서 압축을 푼 디렉터리 기준으로 재집계/재생성:

```bash
python source_reference/scripts/build-presenter-handoff.py --raw evidence --output rebuilt
```

PDF/PNG는 Windows PowerPoint가 있는 환경에서 `source_reference/scripts/render-final-report.ps1`로 생성합니다.
원시 실험의 재실행은 별도 EC2 인프라와 전체 lab 빌드가 필요하며 이 패키지는 추가 실험을 수행하지 않습니다.

실험 종료: 2026-11-07 01:53 KST. EC2 종료 후 S3 및 로컬 원본을 보존했습니다.
패키지는 발표 자료이며 안전한 운영 배포를 증명하는 인증 자료가 아닙니다.
"""
    (out/"먼저읽기.md").write_text(readme,encoding="utf-8")
    audit=dict(status="passed",official_runs=len(runs),cells=len(summary),runs_per_cell=10,
        slides=len(d.prs.slides),scenario_charts=22,selected_method=N[0],decision_scope="project design selection; release conditional",
        excluded_statuses=dict(Counter(r["status"] for r in records if r["status"]!="passed")),
        jar_versions=dict(Counter(r["jar_sha256"] for r in runs)),source_final_validation=validation,
        known_post_result_udp_exceptions=["sequence-0173-attempt-1","sequence-0277-attempt-1"])
    (out/"보고서검증.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding="utf-8")

    # Copy only report evidence and explicitly chosen reference source files.
    import shutil
    evidence=out/"evidence"
    def copy(source,destination):
        destination.parent.mkdir(parents=True,exist_ok=True)
        if destination.exists() and source.stat().st_size == destination.stat().st_size and source.stat().st_mtime_ns == destination.stat().st_mtime_ns:
            return
        shutil.copy2(source,destination)
    for rel in ["COMPLETED","final-validation.json","campaign/plan.tsv","campaign/execution-status.csv"]:
        copy(raw/rel,evidence/rel)
    for r in runs:
        for node,files in [("node-0",["result.json","events.log","run.env","run.properties"]),
                           ("node-1",["fault-events.jsonl","fio-validation.json","fio-prepare.json"])]:
            for name in files:
                rel=Path("runs")/r["run_id"]/node/name
                if (raw/rel).is_file(): copy(raw/rel,evidence/rel)
    lab=Path(__file__).resolve().parent.parent
    sources=["scripts/build-presenter-handoff.py","scripts/build-final-comparison-ppt.py","scripts/render-final-report.ps1",
             "scripts/experiments/ec2/validate-run-evidence.py","scripts/experiments/ec2/run-product-ec2-campaign.sh",
             "scripts/experiments/ec2/apply-fault.sh","scripts/experiments/ec2/validate-fio-evidence.py",
             "src/main/java/com/openkache/failuredetector/transport/ProductUdpExperimentNodeMain.java",
             "src/main/java/com/openkache/failuredetector/transport/UdpExperimentNodeMain.java"]
    for family,config in [("swim","SwimLifeguardConfig"),("redis","RedisClusterConfig"),("phi","PhiAccrualConfig"),
                          ("ignite","IgniteTcpRingConfig"),("aerospike","AerospikeMeshConfig")]:
        sources.append(f"src/main/java/com/openkache/failuredetector/{family}/{config}.java")
    for rel in sources: copy(lab/rel,out/"source_reference"/rel)
    for rel in ["docs/architecture.md","cache-core/src/main/java/io/openkache/core/failure/SwimFailureDetector.java"]:
        source = lab.parent/rel
        if not source.is_file():
            source = lab/"project"/rel
        copy(source,out/"source_reference/project"/rel)
    print(json.dumps(audit,ensure_ascii=False,indent=2))
    print(str(ppt))


if __name__=="__main__":
    main()
