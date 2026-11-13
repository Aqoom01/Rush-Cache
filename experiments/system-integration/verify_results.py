#!/usr/bin/env python3
"""원본 JSON과 파생 CSV·source manifest를 독립적으로 대조한다(표준 라이브러리만 사용)."""
import csv
import hashlib
import json
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def rows(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def close(actual, expected, message):
    require(abs(float(actual) - expected) <= 0.000002, message)


def audit(directory):
    require((directory / "COMPLETE").is_file(), "실험 미완료")
    manifest = json.loads((directory / "manifest.json").read_text())
    digest = hashlib.sha256()
    for path in sorted(Path("cache-core/src").rglob("*.java")):
        digest.update(str(path).encode())
        digest.update(path.read_bytes())
    require(digest.hexdigest() == manifest["coreSourceSha256"], "현재 source와 실행 source 불일치")
    require(hashlib.sha256((directory / "config.properties").read_bytes()).hexdigest()
            == manifest["configSha256"], "config hash 불일치")
    require(not set(manifest["screeningSeeds"]) & set(manifest["confirmationSeeds"]), "독립 seed 위반")
    counts = {}
    for phase, csv_name, json_name, aggregate_name, effects_name in [
        ("screening", "summary.csv", "screening.json", "aggregate.csv", "conditional-effects.csv"),
        ("confirmation", "confirmation.csv", "confirmation.json", "confirmation-aggregate.csv", "confirmation-effects.csv"),
    ]:
        data = rows(directory / csv_name)
        raw = json.loads((directory / json_name).read_text())
        require(len(data) == len(raw), "JSON/CSV 행 수 불일치")
        index = {}
        tapes = defaultdict(set)
        for row, original in zip(data, raw):
            key = row["variant"], row["scenario"], row["seed"]
            require(key not in index, "중복 실행")
            index[key] = row
            tapes[key[1:]].add(row["tape_hash"])
            for field, value in row.items():
                expected = original[field]
                if expected is None:
                    require(value == "", "미관측 값이 0으로 바뀜")
                elif isinstance(expected, bool):
                    require(value == str(expected).lower(), "bool 불일치")
                elif isinstance(expected, (float, int)):
                    if field in {"goodput_virtual_rps", "p99_virtual_ms"}:
                        close(value, float(expected), "숫자 불일치")
                    else:
                        require(int(value) == expected, "정수 불일치")
                else:
                    require(value == expected, "문자열 불일치")
            require(int(row["offered"]) == sum(int(row[f]) for f in ["ok", "rejected", "pending"]), "요청 집계 누락")
            safe = all(int(row[f]) == 0 for f in ["lost", "stale_reads", "false_dead"])
            safe = safe and row["planned_moves"] == row["completed_moves"]
            require(safe == (row["safe"] == "true"), "safe 파생 판정 불일치")
            require(bool(original["events"]), "event 원본 없음")
        require(all(len(tape) == 1 for tape in tapes.values()), "paired 요청열 불일치")
        for group in rows(directory / aggregate_name):
            selected = [r for r in data if r["variant"] == group["variant"] and r["scenario"] == group["scenario"]]
            require(len(selected) == int(group["n"]), "aggregate n 불일치")
            require(sum(r["safe"] == "true" for r in selected) == int(group["safe_n"]), "aggregate safe_n 불일치")
            close(group["mean_goodput_virtual_rps"], statistics.mean(float(r["goodput_virtual_rps"]) for r in selected), "aggregate mean 불일치")
        for pair in rows(directory / effects_name):
            baseline = pair["baseline"].split("-")
            candidate = pair["candidate"].split("-")
            require(sum(a != b for a, b in zip(baseline, candidate)) == 1, "짝비교에 여러 축이 변경됨")
            deltas = []
            for key, row in index.items():
                if key[0] != pair["candidate"] or key[1] != pair["scenario"]:
                    continue
                reference = index.get((pair["baseline"], key[1], key[2]))
                if reference is not None:
                    deltas.append(float(row["goodput_virtual_rps"]) - float(reference["goodput_virtual_rps"]))
            require(len(deltas) == int(pair["n"]), "paired n 불일치")
            close(pair["mean_delta_goodput"], statistics.mean(deltas), "paired delta 불일치")
        counts[phase] = len(data)
    return counts


if __name__ == "__main__":
    repo = Path(__file__).resolve().parents[2]
    os.chdir(repo)
    for item in sys.argv[1:]:
        directory = Path(item)
        counts = audit(directory)
        print(f"PASS {directory}: {counts}")
