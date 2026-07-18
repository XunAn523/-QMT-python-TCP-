import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_PATH = ROOT / "tools" / "benchmark_local_bridge.py"
SPEC = importlib.util.spec_from_file_location("benchmark_local_bridge", BENCHMARK_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load offline benchmark module")
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class OfflineBenchmarkTest(unittest.TestCase):
    def run_offline(self, **overrides):
        arguments = {"samples": 3, "repeats": 2, "warmup": 1, "seed": 20260718}
        arguments.update(overrides)
        with mock.patch.object(
            socket,
            "socket",
            side_effect=AssertionError("offline benchmark attempted to open a socket"),
        ):
            return benchmark.run_benchmark(**arguments)

    def test_runner_is_offline_and_safety_invariants_hold(self):
        previous_dont_write_bytecode = sys.dont_write_bytecode
        result = self.run_offline()
        self.assertIs(sys.dont_write_bytecode, previous_dont_write_bytecode)
        records = result["records"]
        summary = result["summary"]

        self.assertEqual(len(records), 6)
        self.assertEqual(benchmark.SCHEMA_VERSION, 2)
        self.assertTrue(all(record["schema_version"] == 2 for record in records))
        self.assertTrue(all(record["record_type"] == "sample" for record in records))
        self.assertTrue(all(record["ok"] for record in records))
        self.assertTrue(
            all(record["intent_hash"].startswith("sha256:") for record in records)
        )
        self.assertTrue(
            all(
                record["gateway_effect_fingerprint"].startswith("sha256:")
                for record in records
            )
        )
        self.assertTrue(all(record["effect_kind"] == "order" for record in records))
        self.assertTrue(
            all(record["effect_state"] == "ENQUEUED" for record in records)
        )
        self.assertTrue(
            all(
                record["effect_result_stage"] == "BRIDGE_QUEUED"
                for record in records
            )
        )
        self.assertTrue(
            all(record["effect_registry_duplicate_replayed"] for record in records)
        )
        self.assertTrue(
            all(record["effect_registry_record_valid"] for record in records)
        )
        self.assertEqual(summary["record_type"], "summary")
        self.assertEqual(summary["schema_version"], 2)
        self.assertEqual(summary["errors"], 0)
        self.assertEqual(summary["sample_records"], 6)
        self.assertEqual(summary["normal_unique_intents_including_warmup"], 8)
        self.assertEqual(summary["normal_mock_passorder_calls"], 8)
        self.assertTrue(summary["every_unique_intent_called_once"])
        self.assertTrue(summary["temporary_directory_cleanup_verified"])
        self.assertFalse(summary["network_used"])
        self.assertFalse(summary["qmt_or_broker_connected"])
        self.assertTrue(summary["all_safety_checks_passed"])
        self.assertTrue(summary["safety"]["guard_layer_duplicate_no_retry"])
        self.assertEqual(summary["safety"]["guard_layer_replays_checked"], 1)

        durable = summary["safety"]["durable_effect_registry"]
        self.assertTrue(durable["passed"])
        self.assertEqual(durable["measured_records_checked"], 6)
        self.assertTrue(durable["measured_records_valid"])
        self.assertEqual(durable["success_state"], "ENQUEUED")
        self.assertEqual(durable["success_result_stage"], "BRIDGE_QUEUED")
        self.assertEqual(durable["fault_state"], "UNKNOWN")
        self.assertEqual(durable["fault_result_stage"], "SUBMIT_UNKNOWN")
        self.assertTrue(durable["exact_replays_deduplicated"])
        self.assertTrue(durable["conflict_rejected"])

        success = summary["safety"]["success_guard_probe"]
        self.assertTrue(success["passed"])
        self.assertEqual(success["mock_passorder_calls"], 1)
        self.assertTrue(success["guard_visible_before_passorder"])
        self.assertEqual(success["final_guard_state"], "submitted")
        self.assertTrue(success["response_present_before_ack"])
        self.assertEqual(success["response_status"], "accepted")
        self.assertEqual(success["second_drain_count"], 0)
        self.assertEqual(success["effect_state"], "ENQUEUED")
        self.assertEqual(success["effect_result_stage"], "BRIDGE_QUEUED")
        self.assertTrue(success["effect_replay_duplicate"])
        self.assertTrue(success["effect_conflict_rejected"])
        self.assertEqual(success["guard_replay_dedupe_layer"], "helper_guard")

        fault = summary["safety"]["fault_injection"]
        self.assertTrue(fault["passed"])
        self.assertEqual(fault["mock_passorder_calls"], 1)
        self.assertTrue(fault["guard_visible_before_exception"])
        self.assertEqual(fault["final_guard_state"], "unknown")
        self.assertEqual(fault["response_status"], "submit_unknown")
        self.assertEqual(fault["second_drain_count"], 0)
        self.assertEqual(fault["effect_state"], "UNKNOWN")
        self.assertEqual(fault["effect_result_stage"], "SUBMIT_UNKNOWN")
        self.assertTrue(fault["effect_replay_duplicate"])

    def test_seed_determines_record_identity_and_structure(self):
        first = self.run_offline(samples=2, repeats=1, warmup=0)
        second = self.run_offline(samples=2, repeats=1, warmup=0)
        first_ids = [record["request_id"] for record in first["records"]]
        second_ids = [record["request_id"] for record in second["records"]]
        self.assertEqual(first_ids, second_ids)
        workload_fields = ("symbol", "side", "quantity", "price")
        first_workload = [
            tuple(record[field] for field in workload_fields)
            for record in first["records"]
        ]
        second_workload = [
            tuple(record[field] for field in workload_fields)
            for record in second["records"]
        ]
        different_seed = self.run_offline(
            samples=2, repeats=1, warmup=0, seed=20260719
        )
        different_workload = [
            tuple(record[field] for field in workload_fields)
            for record in different_seed["records"]
        ]
        self.assertEqual(first_workload, second_workload)
        self.assertNotEqual(first_workload, different_workload)
        self.assertEqual(
            [record["intent_hash"] for record in first["records"]],
            [record["intent_hash"] for record in second["records"]],
        )
        self.assertEqual(
            [record["gateway_effect_fingerprint"] for record in first["records"]],
            [record["gateway_effect_fingerprint"] for record in second["records"]],
        )
        self.assertEqual(
            set(first["records"][0]),
            set(second["records"][0]),
        )
        expected_metrics = {
            "enqueue_ms",
            "helper_drain_ms",
            "response_read_ms",
            "ack_and_invariant_ms",
            "total_to_response_ms",
            "queue_wait_ms",
            "mock_passorder_elapsed_ms",
            "effect_reserve_ms",
            "effect_dispatch_barrier_ms",
            "effect_result_commit_ms",
            "effect_replay_probe_ms",
            "durable_total_to_final_state_ms",
        }
        self.assertEqual(set(first["summary"]["metrics_ms"]), expected_metrics)
        for metric in first["summary"]["metrics_ms"].values():
            self.assertEqual(metric["count"], 2)
            self.assertEqual(
                set(metric),
                {"count", "mean", "p50", "p95", "p99", "max"},
            )

    def test_cli_self_test_writes_jsonl(self):
        with tempfile.TemporaryDirectory(prefix="qmt-benchmark-cli-test-") as temp:
            output = Path(temp) / "result.jsonl"
            stdout = io.StringIO()
            with mock.patch.object(
                socket,
                "socket",
                side_effect=AssertionError("offline benchmark attempted to open a socket"),
            ), contextlib.redirect_stdout(stdout):
                exit_code = benchmark.main(
                    ["--self-test", "--seed", "20260718", "--output", str(output)]
                )
            self.assertEqual(exit_code, 0)
            lines = output.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 4)
            decoded = [json.loads(line) for line in lines]
            self.assertTrue(all(item["record_type"] == "sample" for item in decoded[:-1]))
            self.assertEqual(decoded[-1]["record_type"], "summary")
            self.assertTrue(decoded[-1]["all_safety_checks_passed"])
            self.assertTrue(stdout.getvalue().strip())

    def test_metric_summary_has_no_absolute_speed_expectation(self):
        summary = benchmark.metric_summary([4.0, 1.0, 3.0, 2.0])
        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["p50"], 2.5)
        self.assertEqual(summary["max"], 4.0)

    def test_invalid_sample_counts_fail_before_work(self):
        with self.assertRaisesRegex(ValueError, "samples"):
            benchmark.run_benchmark(samples=0, repeats=1, warmup=0)
        with self.assertRaisesRegex(ValueError, "repeats"):
            benchmark.run_benchmark(samples=1, repeats=0, warmup=0)
        with self.assertRaisesRegex(ValueError, "warmup"):
            benchmark.run_benchmark(samples=1, repeats=1, warmup=-1)


if __name__ == "__main__":
    unittest.main()
