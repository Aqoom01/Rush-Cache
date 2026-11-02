"""Validate stored correctness evidence and replay its positive witness (Python 3, stdlib only).

This validates an artifact, not the service. VIOLATION/INCONCLUSIVE remain valid stored
evidence; exit 0 does not mean correctness PASS. No search for a missing witness is made.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def payload(value):
    if value is None:
        return None
    raw = base64.b64decode(value["base64"], validate=True)
    require(len(raw) == value["length"] and digest(raw) == value["sha256"], "payload length/hash")
    return raw


def scan_payloads(node):
    if isinstance(node, dict):
        if set(node) == {"base64", "length", "sha256"}:
            payload(node)
        else:
            for v in node.values():
                scan_payloads(v)
    elif isinstance(node, list):
        for v in node:
            scan_payloads(v)


def identity(o):
    return o["runId"], o["loadSlot"], o["clientId"], o["sequence"]


def outcome(row):
    return next((c["outcome"] for c in row["calls"] if c["kind"] == "OUTCOME"), None)


def invoke(row):
    o = outcome(row)
    return o["invoked_ns"] if o else row["calls"][0]["invoked_ns"]


def replay(rows, report):
    result = report["history_result"]
    witness = result["witness"]
    if not witness:
        require(result["verdict"] != "PASS", "PASS without witness")
        return
    require(report["initial_state_attested"], "witness without known initial state")
    state = {payload(e["key"]): payload(e["value"]) for e in report["initial_state"]}
    required = {i for i, row in enumerate(rows) if outcome(row) and outcome(row)["category"] == "SUCCESS"}
    require(len(witness) == len(set(witness)) and all(type(i) is int and 0 <= i < len(rows) for i in witness), "invalid witness indexes")
    require(required <= set(witness), "witness omits successful operation")
    positions = {i: p for p, i in enumerate(witness)}
    for i in witness:
        row = rows[i]
        for j in required:
            if i != j and outcome(rows[j])["terminal_ns"] < invoke(row):
                require(positions[j] < positions[i], "witness violates real time")
        request = row["request"]
        key = payload(request["key"])
        old = state.get(key)
        op = request["operation"]
        if i in required:
            response = outcome(row)["result"]
            require(response is not None, "successful response missing")
            value = payload(response["value"])
            if op == "GET":
                require((response["status"], value) == (("MISS", None) if old is None else ("OK", old)), "witness GET mismatch")
            elif op == "SET":
                require((response["status"], value) == ("OK", None), "witness SET result")
            else:
                require((response["status"], value) == (("MISS" if old is None else "OK"), None), "witness DELETE result")
        else:
            require(op != "GET" and not all(c["kind"] == "NOT_ADMITTED" for c in row["calls"]), "invalid optional effect")
        if op == "SET":
            state[key] = payload(request["input_value"])
        elif op == "DELETE":
            state.pop(key, None)


def validate(root):
    root = Path(root)
    require((root / "STARTED").is_file() and (root / "COMPLETE").is_file(), "completion marker missing")
    integrity_bytes = (root / "integrity.json").read_bytes()
    require((root / "COMPLETE").read_text().strip() == "integrity_sha256=" + digest(integrity_bytes), "integrity marker hash")
    integrity = json.loads(integrity_bytes)
    require(integrity["schema_version"] == "benchmark-correctness-integrity-v1" and integrity["complete"] is True, "integrity format")
    names = {"history.jsonl", "ack-ledger.json", "ack-audit.json", "correctness.json"}
    require(len(integrity["files"]) == 4 and {e["path"] for e in integrity["files"]} == names, "integrity file population")
    for e in integrity["files"]:
        require(digest((root / e["path"]).read_bytes()) == e["sha256"], "file hash: " + e["path"])
    report = json.loads((root / "correctness.json").read_bytes())
    ledger = json.loads((root / "ack-ledger.json").read_bytes())
    audit = json.loads((root / "ack-audit.json").read_bytes())
    rows = [json.loads(line) for line in (root / "history.jsonl").read_text().splitlines()]
    for value in (report, ledger, audit, rows):
        scan_payloads(value)
    require(report["schema_version"] == "benchmark-correctness-v1", "correctness format")
    token = report["history_token"]
    result = report["history_result"]
    require(token == integrity["history_token"] == result["historyToken"], "history token")
    require(result["verdict"] in {"PASS", "VIOLATION", "INCONCLUSIVE"} and result["verdict"] == integrity["history_verdict"], "history verdict")
    ids, submissions, pending = set(), set(), 0
    uncertain, rejected, successful = 0, 0, 0
    for row in rows:
        require((row["run_id"], row["load_slot"], row["clock_id"]) == (report["run_id"], report["load_slot"], report["clock_id"]), "history identity")
        r = row["request"]
        rid = (row["run_id"], row["load_slot"], r["client_id"], r["sequence"])
        require(rid not in ids and row["calls"], "duplicate/empty operation")
        ids.add(rid)
        require(r["operation"] in {"GET", "SET", "DELETE"}, "operation")
        require((r["input_value"] is not None) == (r["operation"] == "SET"), "request value shape")
        first = None
        for c in row["calls"]:
            require(c["submission"] not in submissions and c["submission"] >= 1, "duplicate submission")
            submissions.add(c["submission"])
            if c["kind"] == "PENDING":
                pending += 1
                require(c["observed_ns"] is None and c["outcome"] is None, "pending terminal")
            else:
                require(c["observed_ns"] >= c["invoked_ns"], "call clock")
            require(report["sealed_ns"] >= (c["observed_ns"] if c["observed_ns"] is not None else c["invoked_ns"]), "seal clock")
            o = c["outcome"]
            if c["kind"] == "OUTCOME":
                require(o is not None and identity(o["id"]) == rid and o["operation"] == r["operation"], "outcome identity")
                require(row["calls"][0]["invoked_ns"] <= o["invoked_ns"] <= o["decided_ns"] <= o["terminal_ns"] <= c["observed_ns"], "outcome clock")
                require(first is None or first == o, "conflicting outcomes")
                first = o
            else:
                require(o is None, "non-outcome payload")
        if all(c["kind"] == "NOT_ADMITTED" for c in row["calls"]):
            rejected += 1
        elif first and first["category"] == "SUCCESS":
            successful += 1
        else:
            uncertain += 1
    require((len(rows), len(submissions), pending) == (report["operations"], report["submissions"], report["pending_calls"]), "collection counts")
    complete = not report["collection_failures"] and pending == 0
    require(complete == report["collection_complete"] == integrity["collection_complete"], "collection completeness")
    if result["verdict"] == "PASS":
        require(complete and uncertain == 0 and successful > 0, "uncertain/empty PASS")
        require((result["requiredOperations"], result["uncertainOperations"], result["omittedAdmissions"]) == (successful, uncertain, rejected), "checker counts")
    replay(rows, report)
    expected_ack = "not_run"
    if audit.get("status") != "not_run":
        require(audit["history_token"] == ledger["history_token"] == token, "ACK token")
        require([i["entry"] for i in audit["items"]] == ledger["entries"], "ACK entries")
        require(audit["excluded_keys"] == len(ledger["exclusions"]), "ACK exclusions")
        bad, unknown = False, not ledger["entries"] or bool(ledger["exclusions"])
        entry_keys = [payload(e["key"]) for e in ledger["entries"]]
        require(len(entry_keys) == len(set(entry_keys)), "duplicate ACK key")
        for item in audit["items"]:
            e, read = item["entry"], item["read"]
            writes = [row for row in rows if row["request"]["operation"] != "GET" and payload(row["request"]["key"]) == payload(e["key"]) and not all(c["kind"] == "NOT_ADMITTED" for c in row["calls"])]
            require(len(writes) == 1, "ACK key was overwritten or not written")
            mutation = writes[0]
            ack = outcome(mutation)
            require(ack and ack["category"] == "SUCCESS" and identity(ack["id"]) == identity(e["id"]) and ack["terminal_ns"] == e["acknowledged_ns"], "ACK entry not acknowledged in history")
            require(e["deletion"] == (mutation["request"]["operation"] == "DELETE") and payload(e["expected_value"]) == payload(mutation["request"]["input_value"]), "ACK expectation differs from mutation")
            if item["status"] == "INCONCLUSIVE":
                unknown = True
                continue
            require(ledger["exclusive_namespace"] and audit["readiness"]["status"] == "READY" and audit["barrier_ns"] >= e["acknowledged_ns"], "ACK barrier")
            require(read and read["clock_id"] == report["clock_id"] and read["request"]["operation"] == "GET", "audit read")
            require(payload(read["request"]["key"]) == payload(e["key"]), "audit key differs from ACK key")
            o = read["outcome"]
            require(o["category"] == "SUCCESS" and o["operation"] == "GET" and o["invoked_ns"] >= audit["barrier_ns"], "audit outcome")
            require((o["id"]["clientId"], o["id"]["sequence"]) == (read["request"]["client_id"], read["request"]["sequence"]), "audit ID")
            response = o["result"]
            expected = ("MISS", None) if e["deletion"] else ("OK", payload(e["expected_value"]))
            matches = (response["status"], payload(response["value"])) == expected
            require(item["status"] == ("MATCH" if matches else "MISMATCH"), "audit mismatch hidden")
            bad |= not matches
        expected_ack = "VIOLATION" if bad else "INCONCLUSIVE" if unknown else "PASS"
        require(audit["verdict"] == expected_ack, "ACK verdict")
    require(report["ack_audit_verdict"] == integrity["ack_verdict"] == expected_ack, "ACK report verdict")
    return {"directory": str(root.resolve()), "artifact_valid": True, "history_verdict": result["verdict"], "ack_verdict": expected_ack, "operations": len(rows)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+")
    args = parser.parse_args()
    for directory in args.directories:
        print(json.dumps(validate(directory), ensure_ascii=False))
