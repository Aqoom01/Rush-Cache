import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import operating_network
import operating_ci
import operating_suite as suite


STRATEGY = dict(a1="RAFT", a2="V06", fd="F3", distribution="RANGE", split="HEAT", placement="ESTORE",
                scaler="pax", scheduler="slamig", copy="PRE", drain="incremental-drain")
VALID_RESULT = dict(correctnessValid=True, migrationValid=True, coverageValid=True,
                    jointComponentsValid=True, fdRuntimeValid=True, offered=100, ok=98)


class OperatingSuiteTest(unittest.TestCase):
    def test_gitlab_builds_exact_cases_with_individual_workload_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "plan"
            matrix = suite.TOOL.parent / "matrices/operating-top30.csv"
            self.assertEqual(210, operating_ci.prepare(matrix, output, 30))
            with (output / "cases.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            spec = json.loads((output / "cases.json").read_text())
            self.assertEqual(210, spec["expectedCases"])
            self.assertEqual(list(suite.SCENARIOS), [row["operating_case"] for row in rows[:7]])
            down = dict(zip(spec["perRowArguments"][4][::2], spec["perRowArguments"][4][1::2]))
            self.assertEqual(("2", "400", "20"), (down["--initial-groups"], down["--rate"], down["--rate-after"]))
            self.assertEqual(("30", "leader-crash"), (rows[-1]["strategy_number"], rows[-1]["operating_case"]))

    def test_gitlab_report_requires_verified_cleanup_and_all_cases(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "plan"
            operating_ci.prepare(suite.TOOL.parent / "matrices/operating-top30.csv", output, 3)
            with (output / "cases.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            records = [
                dict(selectors=row, phase="operating", status="FAILED_VALIDATION" if index == 7 else "PASS",
                     cleanupConfirmed=index != 14, backupManifest="verified")
                for index, row in enumerate(rows[:-1])
            ]
            with patch.object(operating_ci, "read_receipts", return_value=records):
                report = operating_ci.report(output / "cases.csv", root, root / "report.json")
            self.assertEqual(["PASS", "REJECTED", "INCOMPLETE"], [row["status"] for row in report["outcomes"]])

    def test_strategy_input_keeps_scenarios_and_seeds_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            matrix = Path(temporary) / "strategies.csv"
            with matrix.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=[*STRATEGY, "scenario", "workload", "seed"])
                writer.writeheader()
                writer.writerow({**STRATEGY, "scenario": "AUTO", "workload": "UNIFORM", "seed": 1})
                writer.writerow({**STRATEGY, "scenario": "AUTO", "workload": "HOT_RANGE", "seed": 2})
            self.assertEqual([STRATEGY], suite.select_strategies(matrix, 0, 1))
            with self.assertRaisesRegex(ValueError, "strategies only"):
                suite.select_strategies(matrix, 1, 1)

    def test_scale_must_finish_after_rate_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            load_dir = Path(temporary)
            before = dict(activeGroups=[0], scalingDecisions=10, pendingJobs=0, requestedCount=-1)
            (load_dir / "before-rate-change.json").write_text(json.dumps(before))
            after = dict(activeGroups=[0, 1], scalingDecisions=11, pendingJobs=0, requestedCount=-1,
                         owners=[{"owner": 0}, {"owner": 1}])
            self.assertTrue(all(suite.assess_case("scale-out", VALID_RESULT, after, None, load_dir, .95).values()))
            after["requestedCount"] = 2
            self.assertFalse(suite.assess_case("scale-out", VALID_RESULT, after, None, load_dir, .95)["automatic_scale_completed"])
            after["requestedCount"] = -1
            after["scalingDecisions"] = 10
            self.assertFalse(suite.assess_case("scale-out", VALID_RESULT, after, None, load_dir, .95)["automatic_scale_completed"])

    def test_crash_requires_successful_write_after_load_observes_confirmation(self):
        gateway = {"repairs": [{"id": 3, "state": "DONE"}]}
        fault = {"evidence": {"repair_admission": {"repair": 3}}}
        with tempfile.TemporaryDirectory() as temporary:
            load_dir = Path(temporary)
            (load_dir / "first-post-crash-index").write_text("16")
            requests = load_dir / "requests.csv"
            requests.write_text("phase,index,command,status\nmeasure,15,Set,0\nmeasure,16,Get,0\n")
            self.assertFalse(suite.assess_case("leader-crash", VALID_RESULT, gateway, fault, load_dir, .95)["post_crash_write_succeeded"])
            with requests.open("a") as stream:
                stream.write("measure,17,Set,0\n")
            self.assertTrue(all(suite.assess_case("leader-crash", VALID_RESULT, gateway, fault, load_dir, .95).values()))

    def test_one_failed_case_rejects_an_otherwise_passing_strategy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            topology = root / "topology.json"
            topology.write_text(json.dumps({"replica_hosts": [f"replica-{i}" for i in range(6)],
                                            "gateway_host": "gateway", "load_host": "load"}))
            args = suite.argparse.Namespace(topology=topology, identity=topology, known_hosts=topology,
                                            bundle=root, output=root / "output", start=0, seeds=[1, 2],
                                            stop_on_failed_case=False)

            def run_case(args, strategy, name, seed, directory):
                return {"scenario": name, "seed": seed, "wall_seconds": 1,
                        "status": "FAILED_VALIDATION" if (name, seed) == ("scale-out", 2) else "PASS"}

            with patch.object(suite, "verify_bundle"), patch.object(suite, "run_case", side_effect=run_case):
                self.assertEqual(0, suite.execute(args, [STRATEGY]))
            record = json.loads((args.output / "suite-results.json").read_text())["outcomes"][0]
            self.assertEqual(("REJECTED", 14), (record["status"], record["collected"]))

    def test_network_fault_removes_its_qdisc_if_filter_installation_fails(self):
        installed = False
        commands = []

        def command(*args):
            nonlocal installed
            commands.append(args)
            if args[0] == "ip":
                return '[{"dev":"eth0"}]'
            if "-j" in args:
                return '[{"root":true,"handle":"71:","kind":"prio"}]' if installed else '[]'
            if "replace" in args:
                installed = True
            if "filter" in args:
                raise RuntimeError("filter failed")
            if "del" in args:
                installed = False
            return ""

        with patch.object(operating_network, "command", side_effect=command), patch.object(operating_network.os, "geteuid", return_value=1000, create=True):
            with self.assertRaisesRegex(RuntimeError, "filter failed"):
                operating_network.impair("slow-replica", [], "192.0.2.4", 27101, 1)
        self.assertFalse(installed)
        self.assertIn(("sudo", "-n", "tc", "qdisc", "del", "dev", "eth0", "root", "handle", "71:"), commands)


if __name__ == "__main__":
    unittest.main()
