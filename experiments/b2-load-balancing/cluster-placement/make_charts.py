"""cluster-placement EC2 결과 그래프(SVG, 표준 라이브러리만). 사용법: python3 -I make_charts.py <S3에서 받은 결과 루트> <출력 폴더>

집계 정의는 ClusterPlacementReport와 같다: 처리량·불균형·miss·CPU는 window 10-19, 이동·재적재는 20 window 합.
"""
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict
from html import escape

root, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
STEADY, SCALE_EVENT = 10, 10
SCENARIOS = ["HOT_RANGE", "ZIPF", "MOVING_HOTSPOT", "BURSTY", "SCALE_OUT"]
POLICIES = [  # (variant, 표시 이름, 색) — 색은 정책에 고정, 순위로 바꾸지 않는다
    ("greedy-load-placement", "greedy", "#2a78d6"),
    ("slicer-inspired", "slicer", "#eb6834"),
    ("estore-inspired", "estore", "#1baf7a"),
    ("cost-aware-rebalancing", "cost-aware", "#eda100"),
    ("hysteresis-rebalancing", "hysteresis", "#e87ba4"),
    ("weighted-rendezvous", "rendezvous", "#6250d6"),
    ("no-rebalance", "no-rebalance", "#8a8984"),
]
NAME = {v: n for v, n, _ in POLICIES}
COLOR = {v: c for v, _, c in POLICIES}
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0", "#fcfcfb"
FONT = "Pretendard, 'Noto Sans KR', 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif"

# ---------- 데이터 ----------
runs = defaultdict(list)       # (scenario, policy) -> [per-run dict]
series = defaultdict(lambda: defaultdict(list))  # (scenario, policy) -> window -> [per-window dict]
for cfg in sorted(glob.glob(os.path.join(root, "*", "config.properties"))):
    props = dict(l.rstrip("\n").split("=", 1) for l in open(cfg, encoding="utf-8") if "=" in l and not l.startswith("#"))
    scenario, _ = props["scenario"].rsplit("-s", 1)
    policy = props["variant"]
    path = glob.glob(os.path.join(os.path.dirname(cfg), "b2-cluster-placement", "windows-*.csv"))[0]
    by_w = defaultdict(list)
    for r in csv.DictReader(open(path, encoding="utf-8")):
        by_w[int(r["window"])].append(r)
    acc = defaultdict(float)
    imb = []
    for w, rows in sorted(by_w.items()):
        reqs = [int(r["requests"]) for r in rows]
        total = sum(reqs)
        members = (3 if w < SCALE_EVENT else 4) if scenario == "SCALE_OUT" else len(rows)
        el = int(rows[0]["elapsedNanos"]) / 1e9
        hit = sum(int(r["hit"]) for r in rows)
        miss = sum(int(r["miss"]) for r in rows)
        ref = sum(int(r["refills"]) for r in rows)
        acc["moved"] += int(rows[0]["movedTablets"])
        acc["refills"] += ref
        series[(scenario, policy)][w].append({
            "throughput": total / el, "imbalance": max(reqs) / (total / members),
            "miss": miss / (hit + miss) if hit + miss else 0, "node4": reqs[3] / total if total else 0,
            "burst": rows[0]["burst"] == "true",
        })
        if w >= STEADY:
            acc["req"] += total
            acc["ref_s"] += ref
            acc["el"] += el
            acc["hit"] += hit
            acc["miss"] += miss
            imb.append(max(reqs) / (total / members))
    runs[(scenario, policy)].append({
        "throughput": acc["req"] / acc["el"], "ops": (acc["req"] + acc["ref_s"]) / acc["el"],
        "refill_rate": acc["ref_s"] / acc["el"], "imbalance": statistics.mean(imb),
        "miss": acc["miss"] / (acc["hit"] + acc["miss"]), "moved": acc["moved"], "refills": acc["refills"],
    })


def policies_of(scenario):
    return [v for v, _, _ in POLICIES if (scenario, v) in runs]


def mean(scenario, policy, key):
    return statistics.mean(r[key] for r in runs[(scenario, policy)])


def seed_values(scenario, policy, key):
    return [r[key] for r in runs[(scenario, policy)]]


# ---------- SVG 도구 ----------
class Svg:
    def __init__(self, w, h):
        self.w, self.h, self.parts = w, h, []

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=13, fill=INK2, anchor="start", weight=400):
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" text-anchor="{anchor}" '
                 f'font-weight="{weight}">{escape(str(s))}</text>')

    def line(self, x1, y1, x2, y2, stroke=GRID, width=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke}" stroke-width="{width}"{d}/>')

    def hbar(self, x, y, w, h, fill):
        # 값 쪽 끝만 둥글게(4px), 기준선 쪽은 각지게
        r = min(4, h / 2, max(w, 0))
        if w <= 0:
            return
        self.add(f'<path d="M{x:.1f},{y:.1f} H{x + w - r:.1f} Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f} '
                 f'V{y + h - r:.1f} Q{x + w:.1f},{y + h:.1f} {x + w - r:.1f},{y + h:.1f} H{x:.1f} Z" fill="{fill}"/>')

    def rect(self, x, y, w, h, fill):
        if w > 0:
            self.add(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" fill="{fill}"/>')

    def circle(self, x, y, r, fill):
        self.add(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{fill}" stroke="{SURFACE}" stroke-width="2"/>')

    def polyline(self, pts, stroke, width=2):
        d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        self.add(f'<polyline points="{d}" fill="none" stroke="{stroke}" stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round"/>')

    def save(self, name, title, subtitle):
        head = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" viewBox="0 0 {self.w} {self.h}" '
                f'font-family="{FONT}"><rect width="100%" height="100%" fill="{SURFACE}"/>'
                f'<text x="24" y="34" font-size="20" font-weight="700" fill="{INK}">{escape(title)}</text>'
                f'<text x="24" y="58" font-size="13" fill="{INK2}">{escape(subtitle)}</text>')
        open(os.path.join(out, name), "w", encoding="utf-8").write(head + "".join(self.parts) + "</svg>")


def nice_max(v):
    """눈금 5칸으로 v를 덮는 가장 작은 깔끔한 최댓값."""
    cands = sorted((step * mag * 5, step * mag) for step in [0.1, 0.15, 0.2, 0.25, 0.4, 0.5, 0.8]
                   for mag in [1, 10, 100, 1000, 10000, 100000])
    return next(c for c in cands if c[0] >= v)


def fmt(v, kind):
    if kind == "k":
        return f"{v / 1000:.1f}k"
    if kind == "pct":
        return f"{v * 100:.0f}%"
    return f"{v:.2f}"


# ---------- 1. 시나리오별 정책 막대(small multiples) ----------
def bar_figure(name, key, title, subtitle, kind, lower_better, ref=None):
    panel_w, label_w, gap = 250, 92, 28
    bar_h, row_h = 16, 24
    rows = max(len(policies_of(s)) for s in SCENARIOS)
    svg = Svg(24 + len(SCENARIOS) * (panel_w + label_w + gap), 110 + rows * row_h + 50)
    vmax, step = nice_max(max(max(seed_values(s, p, key)) for s in SCENARIOS for p in policies_of(s)))
    for i, s in enumerate(SCENARIOS):
        x0 = 24 + i * (panel_w + label_w + gap) + label_w
        y0 = 100
        svg.text(x0 - label_w, 88, s, size=14, fill=INK, weight=700)
        for t in range(0, 6):
            gx = x0 + panel_w * t / 5
            svg.line(gx, y0 - 4, gx, y0 + rows * row_h, GRID)
            svg.text(gx, y0 + rows * row_h + 16, fmt(vmax * t / 5, kind), size=11, fill=MUTED, anchor="middle")
        if ref is not None:
            rx = x0 + panel_w * ref / vmax
            svg.line(rx, y0 - 4, rx, y0 + rows * row_h, INK2, 1, "4 3")
        for j, p in enumerate(policies_of(s)):
            y = y0 + j * row_h
            m = mean(s, p, key)
            svg.text(x0 - 8, y + bar_h - 3, NAME[p], size=12, fill=INK2, anchor="end")
            svg.hbar(x0, y, panel_w * m / vmax, bar_h, COLOR[p])
            vals = seed_values(s, p, key)
            if len(vals) > 1:  # seed 범위(최소-최대)
                svg.line(x0 + panel_w * min(vals) / vmax, y + bar_h / 2, x0 + panel_w * max(vals) / vmax, y + bar_h / 2, INK, 1.5)
            svg.text(x0 + panel_w * max(vals) / vmax + 5, y + bar_h - 3, fmt(m, kind), size=11, fill=INK)
        svg.line(x0, y0 - 4, x0, y0 + rows * row_h, INK2, 1)
    note = "낮을수록 좋음" if lower_better else "높을수록 좋음"
    svg.save(name, title, f"{subtitle} · {note} · 막대=seed 2개 평균, 검은 선=seed 범위")


bar_figure("fig1-throughput.svg", "throughput", "처리량 (측정 요청/초)",
           "EC2 cache-server 4대, window 10-19 평균, 재적재 SET 제외", "k", False)
bar_figure("fig2-miss-rate.svg", "miss", "GET miss 비율 (이동 비용)",
           "Tablet을 옮기면 새 노드가 비어 있어 miss가 난다 · window 10-19", "pct", True)
bar_figure("fig3-imbalance.svg", "imbalance", "부하 불균형 (가장 바쁜 노드 요청 ÷ 노드 평균)",
           "1.0이 완전 균등, 점선=1.0 · window 10-19", "x", True, ref=1.0)

# ---------- 2. 실제 연산량 상한: 측정 요청 + 재적재 SET ----------
label_w, plot_w, bar_h, row_h = 210, 560, 14, 20
items = [(s, p) for s in SCENARIOS for p in policies_of(s)]
svg = Svg(24 + label_w + plot_w + 140, 120 + len(items) * row_h + len(SCENARIOS) * 14 + 40)
vmax, _ = nice_max(max(mean(s, p, "ops") for s, p in items))
x0, y = 24 + label_w, 110
for t in range(0, 6):
    gx = x0 + plot_w * t / 5
    svg.line(gx, y - 6, gx, svg.h - 46, GRID)
    svg.text(gx, svg.h - 30, fmt(vmax * t / 5, "k"), size=11, fill=MUTED, anchor="middle")
# 범례(두 계열이라 식별용 범례 필수)
svg.rect(x0, 76, 12, 12, "#2a78d6")
svg.text(x0 + 18, 87, "측정 요청", size=12)
svg.rect(x0 + 100, 76, 12, 12, "#eb6834")
svg.text(x0 + 118, 87, "miss 후 재적재 SET", size=12)
prev = None
for s, p in items:
    if s != prev:
        svg.text(24, y + 11, s, size=12, fill=INK, weight=700)
        prev = s
    svg.text(x0 - 8, y + bar_h - 2, NAME[p], size=11, fill=INK2, anchor="end")
    meas = mean(s, p, "throughput")
    ref = mean(s, p, "refill_rate")
    w1, w2 = plot_w * meas / vmax, plot_w * ref / vmax
    svg.rect(x0, y, w1, bar_h, "#2a78d6")
    svg.hbar(x0 + w1 + 2, y, max(w2 - 2, 0), bar_h, "#eb6834")
    svg.text(x0 + w1 + w2 + 6, y + bar_h - 2, fmt(meas + ref, "k"), size=11, fill=INK)
    y += row_h
    if (s, p) == [i for i in items if i[0] == s][-1]:
        y += 14
svg.line(x0, 104, x0, svg.h - 46, INK2, 1)
svg.save("fig4-ops-ceiling.svg", "서버가 처리한 전체 연산 = 측정 요청 + 재적재 SET",
         "대부분 초당 약 2.3-2.4만 건에서 멈춤 → 상한은 부하 생성 노드(1 vCPU, 동시 32). 예외: HOT_RANGE 무이동(hot 노드 포화), SCALE_OUT 4대")

# ---------- 3. Trade-off: 이동 비용(miss) vs 불균형 ----------
pw, ph, gap = 230, 210, 40
svg = Svg(24 + len(SCENARIOS) * (pw + gap) + 30, 100 + ph + 120)
for i, s in enumerate(SCENARIOS):
    x0, y0 = 60 + i * (pw + gap), 100
    ymax = max(1.8, max(mean(s, p, "imbalance") for p in policies_of(s)) * 1.05)
    ymax = 3.4 if ymax > 2 else ymax
    svg.text(x0, 88, s, size=14, fill=INK, weight=700)
    for t in range(0, 5):
        gy = y0 + ph - ph * t / 4
        v = 1 + (ymax - 1) * t / 4
        svg.line(x0, gy, x0 + pw, gy, GRID)
        svg.text(x0 - 6, gy + 4, f"{v:.1f}", size=10, fill=MUTED, anchor="end")
    for t in range(0, 5):
        gx = x0 + pw * t / 4
        svg.text(gx, y0 + ph + 16, f"{0.7 * t / 4 * 100:.0f}%", size=10, fill=MUTED, anchor="middle")
    svg.line(x0, y0 + ph, x0 + pw, y0 + ph, INK2)
    pts = []
    for p in policies_of(s):
        px = x0 + pw * mean(s, p, "miss") / 0.7
        py = y0 + ph - ph * (mean(s, p, "imbalance") - 1) / (ymax - 1)
        pts.append((px, py, p))
    pts.sort(key=lambda t: t[1])
    last_y = -100
    for px, py, p in pts:
        svg.circle(px, py, 5, COLOR[p])
        ly = max(py + 4, last_y + 13)  # 라벨 겹침 방지
        svg.text(px + 9, ly, NAME[p], size=11, fill=INK2)
        last_y = ly
    svg.text(x0 + pw / 2, y0 + ph + 34, "GET miss 비율 →", size=11, fill=INK2, anchor="middle")
svg.text(24, 100 + ph / 2, "불균형", size=11, fill=INK2)
svg.save("fig5-tradeoff.svg", "균형과 이동 비용의 Trade-off",
         "왼쪽 아래일수록 좋음(적게 옮기고 균등) · x=GET miss 비율, y=최대/평균 요청 · window 10-19")


# ---------- 4. window 시계열 ----------
def line_panel(svg, x0, y0, pw, ph, scenario, key, ymax, kind, title, marks=None, ref=None):
    svg.text(x0, y0 - 12, title, size=13, fill=INK, weight=700)
    for t in range(0, 5):
        gy = y0 + ph - ph * t / 4
        svg.line(x0, gy, x0 + pw, gy, GRID)
        svg.text(x0 - 6, gy + 4, fmt(ymax * t / 4, kind), size=10, fill=MUTED, anchor="end")
    for w in range(0, 20, 5):
        svg.text(x0 + pw * w / 19, y0 + ph + 16, str(w), size=10, fill=MUTED, anchor="middle")
    for w in marks or []:
        svg.line(x0 + pw * w / 19, y0, x0 + pw * w / 19, y0 + ph, MUTED, 1, "3 3")
    if ref is not None:
        svg.line(x0, y0 + ph - ph * ref / ymax, x0 + pw, y0 + ph - ph * ref / ymax, INK2, 1, "4 3")
    svg.line(x0, y0 + ph, x0 + pw, y0 + ph, INK2)
    ends = []
    for p in policies_of(scenario):
        pts = []
        for w in range(20):
            v = statistics.mean(x[key] for x in series[(scenario, p)][w])
            pts.append((x0 + pw * w / 19, y0 + ph - ph * min(v, ymax) / ymax))
        svg.polyline(pts, COLOR[p])
        ends.append((pts[-1][1], p))
    ends.sort()
    last = -100
    for ey, p in ends:  # 끝점 직접 라벨
        ly = max(ey + 4, last + 13)
        svg.text(x0 + pw + 8, ly, NAME[p], size=11, fill=INK2)
        last = ly
    svg.text(x0 + pw / 2, y0 + ph + 32, "window", size=11, fill=INK2, anchor="middle")


pw, ph = 380, 190
svg = Svg(2 * (pw + 170) + 40, 100 + ph + 80)
line_panel(svg, 70, 110, pw, ph, "SCALE_OUT", "node4", 0.4, "pct", "새 노드(4번째)가 받는 요청 비율 · 점선=목표 25%",
           marks=[SCALE_EVENT], ref=0.25)
line_panel(svg, 70 + pw + 190, 110, pw, ph, "SCALE_OUT", "throughput", 30000, "k", "처리량(측정 요청/초)",
           marks=[SCALE_EVENT])
svg.save("fig6-scale-out.svg", "SCALE_OUT: window 10에 3대 → 4대",
         "seed 2개 평균 · cost-aware는 비용 단가 때문에 한 번도 옮기지 않아 새 노드를 쓰지 않음")

svg = Svg(2 * (pw + 170) + 40, 100 + ph + 80)
line_panel(svg, 70, 110, pw, ph, "MOVING_HOTSPOT", "miss", 0.8, "pct", "MOVING_HOTSPOT: GET miss 비율")
burst_ws = [w for w in range(20) if series[("BURSTY", "greedy-load-placement")][w][0]["burst"]]
line_panel(svg, 70 + pw + 190, 110, pw, ph, "BURSTY", "miss", 0.8, "pct", "BURSTY: GET miss 비율 · 점선=burst window",
           marks=burst_ws)
svg.save("fig7-dynamic-miss.svg", "동적 부하에서 window별 이동 비용",
         "seed 2개 평균 · greedy는 매 window 배치를 다시 정해 miss가 계속 높게 유지됨")

# 그래프에 쓴 값(PPT 표·검증용)
with open(os.path.join(out, "chart-data.csv"), "w", newline="", encoding="utf-8") as f:
    wtr = csv.writer(f)
    wtr.writerow(["scenario", "policy", "seeds", "throughput", "server_ops", "imbalance", "miss_rate", "moved_tablets", "refills"])
    for s in SCENARIOS:
        for p in policies_of(s):
            wtr.writerow([s, NAME[p], len(runs[(s, p)])] + [round(mean(s, p, k), 4) for k in
                          ["throughput", "ops", "imbalance", "miss", "moved", "refills"]])
print("wrote", sorted(os.listdir(out)))
