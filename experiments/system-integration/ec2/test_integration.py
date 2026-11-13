"""배포 도구의 입력·불완전 결과·fault 확인·소유 프로세스 보호 검증. 서버 기동은 별도 실제 리허설에서 한다."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("integration", Path(__file__).with_name("integration.py"))
integration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(integration)


class ControllerContractTest(unittest.TestCase):
    def test_resource_samples_preserve_completed_records_incrementally(self):
        with tempfile.TemporaryDirectory() as temp:
            c=object.__new__(integration.Controller)
            c.output=Path(temp);c.metrics=[];c.group_count=1;c.run_id='sample-test';c.plan={};c.remote=False;c.started=[]
            c.node_status=lambda group,index: {'runId':c.run_id,'group':group,'index':index}
            c.sample();first=(c.output/'resource-samples.jsonl').read_bytes()
            c.sample();data=(c.output/'resource-samples.jsonl').read_bytes()
            self.assertTrue(data.startswith(first))
            samples=[json.loads(line) for line in data.splitlines()]
            self.assertEqual(2,len(samples));self.assertEqual(3,len(samples[1]['nodes']))
            self.assertIn('collected_monotonic_ns',samples[1])
            self.assertGreaterEqual(samples[1]['completed_monotonic_ns'],samples[1]['collected_monotonic_ns'])

    def test_bundle_rejects_extra_jar(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d); (p / "lib").mkdir()
            jar = p / "lib/experiment-runner-test.jar"; jar.write_bytes(b"fixture")
            (p / "BUNDLE_SHA256SUMS").write_text(integration.sha(jar) + "  lib/experiment-runner-test.jar\n")
            integration.verify_bundle(p)
            (p / "lib/extra.jar").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "extra/missing"):
                integration.verify_bundle(p)

    def test_unknown_role_fails_before_pid_lookup_or_signal(self):
        from argparse import Namespace
        with tempfile.TemporaryDirectory() as d:
            with patch.object(integration.os, "kill") as kill:
                with self.assertRaisesRegex(ValueError, "unknown role"):
                    integration.agent(Namespace(bundle=d, root=d, role="../../other", action="kill", java="java"))
                kill.assert_not_called()

    def test_fault_is_not_fabricated_after_migration_publication(self):
        controller = integration.Controller.__new__(integration.Controller)
        controller.fault = None; controller.plan = {"scenario": "CRASH_MOVE"}
        controller.gateway_host = "127.0.0.1"; controller.gateway_port = 1
        with patch.object(integration, "endpoint", return_value={"migrations": [{"state": "DONE", "published": True}]}):
            controller.fault_if_due()
        self.assertIsNone(controller.fault)

    def test_control_decoder_rejects_truncated_or_wrong_header(self):
        with patch.object(integration.socket, "create_connection") as connect:
            s = connect.return_value.__enter__.return_value
            s.recv.return_value = b""
            with self.assertRaises(EOFError):
                integration.endpoint("127.0.0.1", 1, 96)

    def test_fault_matrix_includes_vr_external_fd_and_all_copy_protocols(self):
        from argparse import Namespace
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "matrix.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                integration.matrix(Namespace(stage="fault", protocols="VR,O00", output=str(path)))
            import csv
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 30 * len(integration.FAILURE_METHODS))
            self.assertEqual({r["fd"] for r in rows if r["protocol"] == "VR"}, set(integration.FAILURE_METHODS))
            self.assertIn("POST", {r["copy"] for r in rows})

    def test_controlled_role_command_keeps_paths_as_arguments(self):
        main, args = integration.role_command(Path('/tmp/a b'), Path('/tmp/run space'), "replica-g1-n2")
        self.assertEqual(main, integration.REPLICA)
        self.assertEqual(args, ['/tmp/run space/input/cluster.properties', '1', '2', '/tmp/run space/replica-g1-n2'])

    def test_policy_matrix_covers_every_supported_b1_b2_policy(self):
        from argparse import Namespace
        import csv
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "policies.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                integration.matrix(Namespace(stage="policies", protocols="O00", output=str(path)))
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 708)
            self.assertEqual({r["distribution"] for r in rows}, set(integration.DISTRIBUTIONS))
            self.assertEqual(len({r["placement"] for r in rows}), 9)
            self.assertTrue(all(r["split"] == "NONE" for r in rows if r["distribution"] not in ("RANGE", "TAG", "ORDER")))
            self.assertTrue(all(r["placement"] == "NONE" for r in rows if r["distribution"].startswith("RING")))

    def test_covering_array_contains_every_method_and_scheduler_copy_pair(self):
        rows = integration.covering_rows(["O00", "RAFT"])
        self.assertTrue(all(integration.valid_combination(r) for r in rows))
        self.assertEqual({r["scaler"] for r in rows}, set(integration.SCALERS))
        self.assertEqual({r["distribution"] for r in rows}, set(integration.DISTRIBUTIONS))
        self.assertEqual({(r["scheduler"], r["copy"]) for r in rows}, {(s, c) for s in integration.SCHEDULERS for c in ("STOP", "PRE", "POST")})
        self.assertEqual({(r["protocol"], r["scaler"]) for r in rows}, {(p, s) for p in ("O00", "RAFT") for s in integration.SCALERS})

    def test_joint_coverage_has_independent_a1_a2_axes_and_every_pair(self):
        rows = integration.covering_rows()
        self.assertEqual({(r["a1"], r["a2"]) for r in rows},
                         {(a, b) for a in integration.A1_PROTOCOLS for b in integration.A2_PROTOCOLS})
        self.assertTrue(all("protocol" not in r for r in rows))
        self.assertEqual({(r["a1"], r["copy"]) for r in rows},
                         {(a, c) for a in integration.A1_PROTOCOLS for c in ("STOP", "PRE", "POST")})

    def test_default_joint_smoke_matrix_keeps_both_roles_in_every_row(self):
        from argparse import Namespace
        import csv
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "joint.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                integration.matrix(Namespace(stage="smoke", protocols=None, output=str(path)))
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 7 * 9 * 3)
            self.assertTrue(all(r["a1"] in integration.A1_PROTOCOLS and r["a2"] in integration.A2_PROTOCOLS for r in rows))
            self.assertTrue(all("protocol" not in r for r in rows))

    def test_detector_and_a2_read_candidates_are_independent_and_complete(self):
        rows=integration.covering_rows(a1s=["VR"],a2s=["O00","V06"])
        self.assertEqual({r["fd"] for r in rows},set(integration.FAILURE_METHODS))
        self.assertEqual({(r["fd"],r["read_selection"]) for r in rows},{(f,r) for f in integration.FAILURE_METHODS for r in integration.READ_SELECTIONS})
        self.assertTrue(all(r["a2"]=="V06" for r in rows if r["read_selection"]=="ROUND_ROBIN"))

    def test_ignite_coordinator_fault_is_rejected_before_kill(self):
        c=integration.Controller.__new__(integration.Controller);c.cluster={"failure.detector":"IGNITE_TCP_RING","fd.coordinator.index":"2"}
        with self.assertRaisesRegex(ValueError,"UNSUPPORTED"):
            c.check_fault_supported(2)
        c.check_fault_supported(0)

    def test_batch_dry_run_never_starts_processes_or_creates_output(self):
        from argparse import Namespace
        import csv
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);matrix=root/"matrix.csv";output=root/"untouched"
            with matrix.open("w",newline="") as f:
                writer=csv.DictWriter(f,fieldnames=["a1","a2","fd"]);writer.writeheader();writer.writerow({"a1":"VR","a2":"O00","fd":"F7C"})
            with patch.object(integration.subprocess,"run") as run,contextlib.redirect_stdout(io.StringIO()):
                integration.batch(Namespace(matrix=str(matrix),bundle="unused",output=str(output),mode="local",execute=False,start=0,limit=10,allow_legacy=False,plan_arg=[]))
                run.assert_not_called()
            self.assertFalse(output.exists())

    def test_batch_refuses_isolated_engines_and_unsupported_joint_combinations(self):
        with self.assertRaisesRegex(ValueError,"isolated legacy"):
            integration.row_plan_arguments({"protocol":"O00"})
        with self.assertRaisesRegex(ValueError,"incompatible"):
            integration.row_plan_arguments({"a1":"VR","a2":"O00","placement":"FIXED_LPT","distribution":"RESIZABLE","split":"NONE"})
        with self.assertRaisesRegex(ValueError,"incompatible"):
            integration.row_plan_arguments({"a1":"VR","a2":"O00","read_selection":"ROUND_ROBIN"})

    def test_detector_runtime_failure_is_not_reported_as_coverage(self):
        c=integration.Controller.__new__(integration.Controller);c.cluster={"failure.detector":"F0"};c.fault=None;c.plan={"scenario":"NORMAL","joint_cache":True}
        c.metrics=[{"nodes":[{"group":0,"index":0,"fd":{"runtimeHealthy":False},"a1_authority":{"fd":{"runtimeHealthy":True}}}]}]
        self.assertFalse(c.fd_evidence()["valid"])

    def test_bundle_detects_changed_controller_tool(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d); (p / "lib").mkdir(); (p / "tools").mkdir()
            jar = p / "lib/experiment-runner-test.jar"; jar.write_bytes(b"fixture")
            tool = p / "tools/integration.py"; tool.write_bytes(b"original")
            (p / "BUNDLE_SHA256SUMS").write_text(integration.sha(jar) + "  lib/experiment-runner-test.jar\n")
            (p / "TOOLS_SHA256SUMS").write_text(integration.sha(tool) + "  tools/integration.py\n")
            integration.verify_bundle(p)
            tool.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "tool hash mismatch"):
                integration.verify_bundle(p)


if __name__ == "__main__":
    unittest.main()
