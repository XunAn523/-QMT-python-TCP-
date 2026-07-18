from __future__ import annotations

import concurrent.futures
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / "网关"
if str(GATEWAY_DIR) not in sys.path:
    sys.path.insert(0, str(GATEWAY_DIR))

from order_correlation import IdempotencyConflict, OrderCorrelationStore  # noqa: E402


class OrderCorrelationStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "gateway_state.sqlite3"
        self.store = OrderCorrelationStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self.tempdir.cleanup()

    @staticmethod
    def item(identity: str, stage: str = "RESERVED") -> dict:
        return {
            "account_id": "account-a",
            "client_order_id": identity,
            "trace_id": identity + "-trace",
            "msg_id": identity + "-msg",
            "request_id": identity + "-request",
            "qmt_user_order_id": "qmt-" + identity,
            "trader_name": "trader-a",
            "authenticated_trader_key": "key-a",
            "side": "buy",
            "order_type": 23,
            "intent_hash": "hash-" + identity,
            "stage": stage,
        }

    def raw_execute(self, sql: str, params=()) -> None:
        with self.store._lock:  # Test-only fault injection and timestamp setup.
            self.store._db.execute(sql, params)
            self.store._db.commit()

    def test_wal_normal_and_cleanup_indexes_are_preserved(self) -> None:
        with self.store._lock:
            journal_mode = self.store._db.execute("PRAGMA journal_mode").fetchone()[0]
            synchronous = self.store._db.execute("PRAGMA synchronous").fetchone()[0]
            order_indexes = {
                row[1]: row
                for row in self.store._db.execute(
                    "PRAGMA index_list(order_correlation)"
                ).fetchall()
            }
            event_indexes = {
                row[1]
                for row in self.store._db.execute(
                    "PRAGMA index_list(gateway_event_dedupe)"
                ).fetchall()
            }
        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(int(synchronous), 1)  # SQLite NORMAL.
        self.assertIn("idx_order_correlation_terminal_at", order_indexes)
        self.assertEqual(
            int(order_indexes["idx_order_correlation_terminal_at"][4]), 1
        )  # Partial index: pending inserts do not enter it.
        self.assertIn("idx_gateway_event_dedupe_created_at", event_indexes)

    def test_reserve_returns_inserted_record_without_followup_get(self) -> None:
        item = self.item("new-order")
        statements = []
        with self.store._lock:
            self.store._db.set_trace_callback(statements.append)
        try:
            with mock.patch.object(
                self.store,
                "get",
                side_effect=AssertionError("new reserve must not issue follow-up get"),
            ) as get_mock:
                row, duplicate = self.store.reserve(item)
                get_mock.assert_not_called()
        finally:
            with self.store._lock:
                self.store._db.set_trace_callback(None)

        self.assertFalse(duplicate)
        self.assertEqual(row["account_id"], item["account_id"])
        self.assertEqual(row["client_order_id"], item["client_order_id"])
        self.assertEqual(row["trace_id"], item["trace_id"])
        self.assertEqual(row["msg_id"], item["msg_id"])
        self.assertEqual(row["request_id"], item["request_id"])
        self.assertEqual(row["qmt_user_order_id"], item["qmt_user_order_id"])
        self.assertEqual(row["side"], "BUY")
        self.assertEqual(row["order_type"], 23)
        self.assertEqual(row["stage"], "RESERVED")
        self.assertIsNone(row["order_id"])
        self.assertIsNone(row["order_sysid"])
        self.assertIsNone(row["terminal_at"])
        self.assertEqual(row["created_at"], row["updated_at"])
        # The pre-insert lookup is required for idempotency. The optimization
        # removes only the old post-commit SELECT used to rebuild the row.
        insert_index = next(
            index for index, statement in enumerate(statements)
            if statement.lstrip().upper().startswith("INSERT INTO ORDER_CORRELATION")
        )
        self.assertFalse(any(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements[insert_index + 1:]
        ))
        self.assertEqual(self.store.get("account-a", "new-order"), row)

    def test_reserve_duplicate_and_conflict_semantics_are_unchanged(self) -> None:
        item = self.item("duplicate")
        original, duplicate = self.store.reserve(item)
        self.assertFalse(duplicate)
        replay, duplicate = self.store.reserve(dict(item))
        self.assertTrue(duplicate)
        self.assertEqual(replay, original)

        conflict = dict(item)
        conflict["intent_hash"] = "different-hash"
        with self.assertRaisesRegex(IdempotencyConflict, "different intent"):
            self.store.reserve(conflict)

    def test_release_reservation_only_deletes_the_exact_unexecuted_intent(self) -> None:
        item = self.item("release-me")
        self.store.reserve(item)

        self.assertFalse(
            self.store.release_reservation(
                "wrong-account", item["client_order_id"], item["intent_hash"]
            )
        )
        self.assertFalse(
            self.store.release_reservation(
                item["account_id"], "wrong-client", item["intent_hash"]
            )
        )
        self.assertFalse(
            self.store.release_reservation(
                item["account_id"], item["client_order_id"], "wrong-intent"
            )
        )
        self.assertIsNotNone(
            self.store.get(item["account_id"], item["client_order_id"])
        )

        statements = []
        with self.store._lock:
            self.store._db.set_trace_callback(statements.append)
        try:
            self.assertTrue(
                self.store.release_reservation(
                    item["account_id"],
                    item["client_order_id"],
                    item["intent_hash"],
                )
            )
        finally:
            with self.store._lock:
                self.store._db.set_trace_callback(None)
        self.assertFalse(any(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements
        ))
        self.assertEqual(sum(
            statement.lstrip().upper().startswith("DELETE FROM ORDER_CORRELATION")
            for statement in statements
        ), 1)
        self.assertIsNone(
            self.store.get(item["account_id"], item["client_order_id"])
        )
        self.assertFalse(
            self.store.release_reservation(
                item["account_id"], item["client_order_id"], item["intent_hash"]
            )
        )

        executing = self.item("already-executing")
        self.store.reserve(executing)
        self.store.update_stage(
            executing["account_id"],
            executing["client_order_id"],
            "BRIDGE_QUEUED",
        )
        self.assertFalse(
            self.store.release_reservation(
                executing["account_id"],
                executing["client_order_id"],
                executing["intent_hash"],
            )
        )
        self.assertEqual(
            self.store.get(
                executing["account_id"], executing["client_order_id"]
            )["stage"],
            "BRIDGE_QUEUED",
        )

        for values in (("", "client", "intent"), ("account", "", "intent"), ("account", "client", "")):
            with self.assertRaisesRegex(ValueError, "required"):
                self.store.release_reservation(*values)

    def test_release_unstarted_order_is_atomic_with_effect_and_pending_ledgers(self) -> None:
        item = self.item("atomic-release")
        request_id = item["request_id"]
        fingerprint = "effect-atomic-release"
        self.store.reserve(item)
        self.store.reserve_effect_request(
            item["account_id"], request_id, "order", fingerprint
        )
        self.assertTrue(self.store.transition_effect_request(
            item["account_id"],
            request_id,
            fingerprint,
            "DISPATCHING",
        ))
        self.store.save_pending_response(
            item["account_id"],
            request_id,
            {
                "kind": "order",
                "fingerprint": fingerprint,
                "payload": {"request_id": request_id},
            },
        )

        self.assertTrue(self.store.release_unstarted_order(
            item["account_id"],
            item["client_order_id"],
            item["intent_hash"],
            request_id,
            fingerprint,
        ))
        self.assertIsNone(
            self.store.get(item["account_id"], item["client_order_id"])
        )
        self.assertEqual(
            self.store.load_pending_responses(item["account_id"]), []
        )
        effect = self.store.get_effect_request(item["account_id"], request_id)
        self.assertEqual(effect["state"], "PREPARED")
        self.assertIsNone(effect["result"])

        protected = self.item("atomic-protected")
        protected_request = protected["request_id"]
        protected_fingerprint = "effect-atomic-protected"
        self.store.reserve(protected)
        self.store.update_stage(
            protected["account_id"], protected["client_order_id"], "BRIDGE_QUEUED"
        )
        self.store.reserve_effect_request(
            protected["account_id"],
            protected_request,
            "order",
            protected_fingerprint,
        )
        self.assertTrue(self.store.transition_effect_request(
            protected["account_id"],
            protected_request,
            protected_fingerprint,
            "DISPATCHING",
        ))
        self.store.save_pending_response(
            protected["account_id"],
            protected_request,
            {
                "kind": "order",
                "fingerprint": protected_fingerprint,
                "payload": {"request_id": protected_request},
            },
        )

        with self.assertRaisesRegex(IdempotencyConflict, "exact RESERVED"):
            self.store.release_unstarted_order(
                protected["account_id"],
                protected["client_order_id"],
                protected["intent_hash"],
                protected_request,
                protected_fingerprint,
            )
        self.assertEqual(
            self.store.get(
                protected["account_id"], protected["client_order_id"]
            )["stage"],
            "BRIDGE_QUEUED",
        )
        self.assertEqual(
            self.store.load_pending_responses(protected["account_id"])[0][
                "fingerprint"
            ],
            protected_fingerprint,
        )
        self.assertEqual(
            self.store.get_effect_request(
                protected["account_id"], protected_request
            )["state"],
            "DISPATCHING",
        )

        mismatched = self.item("atomic-pending-conflict")
        mismatched_request = mismatched["request_id"]
        mismatched_fingerprint = "effect-atomic-pending-conflict"
        self.store.reserve(mismatched)
        self.store.reserve_effect_request(
            mismatched["account_id"],
            mismatched_request,
            "order",
            mismatched_fingerprint,
        )
        self.assertTrue(self.store.transition_effect_request(
            mismatched["account_id"],
            mismatched_request,
            mismatched_fingerprint,
            "DISPATCHING",
        ))
        self.store.save_pending_response(
            mismatched["account_id"],
            mismatched_request,
            {
                "kind": "order",
                "fingerprint": "different-effect",
                "payload": {"request_id": mismatched_request},
            },
        )
        with self.assertRaisesRegex(IdempotencyConflict, "different effect"):
            self.store.release_unstarted_order(
                mismatched["account_id"],
                mismatched["client_order_id"],
                mismatched["intent_hash"],
                mismatched_request,
                mismatched_fingerprint,
            )
        self.assertIsNotNone(self.store.get(
            mismatched["account_id"], mismatched["client_order_id"]
        ))
        pending_by_request = {
            row["request_id"]: row
            for row in self.store.load_pending_responses(
                mismatched["account_id"]
            )
        }
        self.assertEqual(
            pending_by_request[mismatched_request]["fingerprint"],
            "different-effect",
        )
        self.assertEqual(
            self.store.get_effect_request(
                mismatched["account_id"], mismatched_request
            )["state"],
            "DISPATCHING",
        )

    def test_update_stage_is_one_conditional_update_and_never_regresses(self) -> None:
        self.store.reserve(self.item("stage-order"))
        statements = []
        with self.store._lock:
            self.store._db.set_trace_callback(statements.append)
        try:
            self.store.update_stage(
                "account-a", "stage-order", "QMT_SUBMITTED", order_id="order-1"
            )
        finally:
            with self.store._lock:
                self.store._db.set_trace_callback(None)

        self.assertFalse(any(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements
        ))
        self.assertEqual(sum(
            statement.lstrip().upper().startswith("UPDATE ORDER_CORRELATION")
            for statement in statements
        ), 1)
        row = self.store.get("account-a", "stage-order")
        self.assertEqual(row["stage"], "QMT_SUBMITTED")
        self.assertEqual(row["order_id"], "order-1")

        self.store.update_stage("account-a", "stage-order", "RESERVED", order_id="wrong")
        row = self.store.get("account-a", "stage-order")
        self.assertEqual(row["stage"], "QMT_SUBMITTED")
        self.assertEqual(row["order_id"], "order-1")

        self.store.update_stage("account-a", "stage-order", "PARTIAL")
        self.store.update_stage("account-a", "stage-order", "QMT_ORDER_CREATED")
        self.assertEqual(self.store.get("account-a", "stage-order")["stage"], "PARTIAL")

        self.store.update_stage(
            "account-a", "stage-order", "FILLED", order_sysid="sys-1"
        )
        terminal = self.store.get("account-a", "stage-order")
        self.assertEqual(terminal["stage"], "FILLED")
        self.assertIsNotNone(terminal["terminal_at"])
        self.store.update_stage(
            "account-a", "stage-order", "CANCELLED", order_sysid="wrong-sys"
        )
        unchanged = self.store.get("account-a", "stage-order")
        self.assertEqual(unchanged["stage"], "FILLED")
        self.assertEqual(unchanged["order_sysid"], "sys-1")
        self.assertEqual(unchanged["terminal_at"], terminal["terminal_at"])

        # Missing rows remain a no-op.
        self.store.update_stage("account-a", "missing", "FILLED")

    def test_update_stage_rolls_back_sql_errors(self) -> None:
        self.store.reserve(self.item("stage-error"))
        self.raw_execute(
            """
            CREATE TRIGGER reject_stage_update
            BEFORE UPDATE ON order_correlation
            WHEN NEW.stage='BROKER_ACCEPTED'
            BEGIN
                SELECT RAISE(ABORT, 'injected stage failure');
            END
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.update_stage("account-a", "stage-error", "BROKER_ACCEPTED")
        self.assertFalse(self.store._db.in_transaction)
        self.assertEqual(
            self.store.get("account-a", "stage-error")["stage"], "RESERVED"
        )
        self.raw_execute("DROP TRIGGER reject_stage_update")
        self.store.update_stage("account-a", "stage-error", "QMT_SUBMITTED")
        self.assertEqual(
            self.store.get("account-a", "stage-error")["stage"], "QMT_SUBMITTED"
        )

    def test_event_batch_mark_and_lookup_are_bounded_and_compatible(self) -> None:
        self.assertEqual(self.store.mark_events("account-a", []), 0)
        self.assertEqual(
            self.store.mark_events("account-a", ["e1", "e2", "e1", ""]), 2
        )
        self.assertEqual(self.store.mark_events("account-a", ["e1", "e2"]), 0)
        self.assertEqual(
            self.store.events_seen_many("account-a", ["e0", "e1", "e2"]),
            {"e1", "e2"},
        )
        self.assertTrue(self.store.event_seen("account-a", "e1"))
        self.assertFalse(self.store.event_seen("account-a", "e0"))
        self.assertIsNone(self.store.mark_event("account-a", "e3"))
        self.assertTrue(self.store.event_seen("account-a", "e3"))
        self.assertIsNone(self.store.mark_event("account-a", ""))

        with self.assertRaises(TypeError):
            self.store.mark_events("account-a", "not-a-batch")
        with self.assertRaisesRegex(ValueError, "maximum size"):
            self.store.mark_events(
                "account-a",
                [
                    "overflow-%d" % index
                    for index in range(self.store.MAX_EVENT_BATCH_SIZE + 1)
                ],
            )

    def test_pending_response_ledger_survives_reopen_and_is_explicitly_removed(self) -> None:
        item = {
            "kind": "cancel",
            "fingerprint": "cancel-persist-fingerprint",
            "payload": {"request_id": "cancel-persist", "order_id": "1001"},
            "queued_at": time.time(),
            "deadline_at": time.time() + 8.0,
        }
        self.store.save_pending_response("account-a", "cancel-persist", item)
        self.assertEqual(
            self.store.load_pending_responses("account-a")[0]["kind"],
            "cancel",
        )
        self.store.close()
        self.store = OrderCorrelationStore(self.db_path)
        recovered = self.store.load_pending_responses("account-a")
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["request_id"], "cancel-persist")
        self.assertEqual(recovered[0]["payload"]["order_id"], "1001")
        self.store.remove_pending_response("account-a", "cancel-persist")
        self.assertEqual(self.store.load_pending_responses("account-a"), [])

    def test_pending_response_insert_is_select_free_and_conflicts_preserve_original(self) -> None:
        request_id = "pending-fingerprint"
        original_item = {
            "kind": "order",
            "fingerprint": "fingerprint-a",
            "payload": {"version": 1},
        }
        statements = []
        with self.store._lock:
            self.store._db.set_trace_callback(statements.append)
        try:
            self.store.save_pending_response(
                "account-a", request_id, original_item
            )
        finally:
            with self.store._lock:
                self.store._db.set_trace_callback(None)
        self.assertFalse(any(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements
        ))
        self.assertEqual(sum(
            statement.lstrip().upper().startswith(
                "INSERT INTO GATEWAY_PENDING_RESPONSE"
            )
            for statement in statements
        ), 1)

        with self.store._lock:
            before = dict(self.store._db.execute(
                "SELECT * FROM gateway_pending_response "
                "WHERE account_id=? AND request_id=?",
                ("account-a", request_id),
            ).fetchone())

        replacement = dict(original_item)
        replacement["kind"] = "ORDER"
        replacement["payload"] = {"version": 2}
        self.store.save_pending_response("account-a", request_id, replacement)
        loaded = self.store.load_pending_responses("account-a")
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["kind"], "order")
        self.assertEqual(loaded[0]["fingerprint"], "fingerprint-a")
        self.assertEqual(loaded[0]["payload"], {"version": 2})
        with self.store._lock:
            allowed_update = dict(self.store._db.execute(
                "SELECT * FROM gateway_pending_response "
                "WHERE account_id=? AND request_id=?",
                ("account-a", request_id),
            ).fetchone())
        self.assertEqual(allowed_update["created_at"], before["created_at"])
        self.assertGreaterEqual(allowed_update["updated_at"], before["updated_at"])

        conflict_items = (
            {
                "kind": "order",
                "fingerprint": "fingerprint-b",
                "payload": {"version": 3},
            },
            {
                "kind": "cancel",
                "fingerprint": "fingerprint-a",
                "payload": {"version": 3},
            },
            {"kind": "order", "payload": {"version": 3}},
            {"fingerprint": "fingerprint-a", "payload": {"version": 3}},
        )
        for conflict_item in conflict_items:
            with self.assertRaises(IdempotencyConflict):
                self.store.save_pending_response(
                    "account-a", request_id, conflict_item
                )
            with self.store._lock:
                after = dict(self.store._db.execute(
                    "SELECT * FROM gateway_pending_response "
                    "WHERE account_id=? AND request_id=?",
                    ("account-a", request_id),
                ).fetchone())
            self.assertEqual(after, allowed_update)

    def test_pending_response_legacy_missing_fingerprint_is_fail_closed(self) -> None:
        legacy_path = Path(self.tempdir.name) / "legacy.sqlite3"
        legacy_db = sqlite3.connect(str(legacy_path))
        legacy_db.executescript(
            """
            CREATE TABLE gateway_pending_response (
                account_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                item_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (account_id, request_id)
            );
            INSERT INTO gateway_pending_response
            (account_id,request_id,kind,item_json,created_at,updated_at)
            VALUES ('account-a','legacy-request','order','{}',1.0,1.0);
            """
        )
        legacy_db.close()
        legacy_store = OrderCorrelationStore(legacy_path)
        try:
            with legacy_store._lock:
                migrated = dict(legacy_store._db.execute(
                    "SELECT * FROM gateway_pending_response "
                    "WHERE account_id='account-a' AND request_id='legacy-request'"
                ).fetchone())
            self.assertEqual(migrated["fingerprint"], "")
            with self.assertRaises(IdempotencyConflict):
                legacy_store.save_pending_response(
                    "account-a",
                    "legacy-request",
                    {"kind": "order", "fingerprint": "new-fingerprint"},
                )
            with legacy_store._lock:
                after = dict(legacy_store._db.execute(
                    "SELECT * FROM gateway_pending_response "
                    "WHERE account_id='account-a' AND request_id='legacy-request'"
                ).fetchone())
            self.assertEqual(after, migrated)
        finally:
            legacy_store.close()

    def test_effect_request_registry_is_atomic_idempotent_and_permanent(self) -> None:
        statements = []
        with self.store._lock:
            self.store._db.set_trace_callback(statements.append)
        try:
            inserted, duplicate = self.store.reserve_effect_request(
                "account-a", "effect-1", "NEW", "effect-fingerprint"
            )
        finally:
            with self.store._lock:
                self.store._db.set_trace_callback(None)
        self.assertFalse(duplicate)
        self.assertEqual(inserted["kind"], "new")
        self.assertEqual(inserted["fingerprint"], "effect-fingerprint")
        insert_index = next(
            index for index, statement in enumerate(statements)
            if statement.lstrip().upper().startswith(
                "INSERT OR IGNORE INTO GATEWAY_EFFECT_REQUEST"
            )
        )
        self.assertFalse(any(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements[insert_index + 1:]
        ))

        replay, duplicate = self.store.reserve_effect_request(
            "account-a", "effect-1", "new", "effect-fingerprint"
        )
        self.assertTrue(duplicate)
        self.assertEqual(replay, inserted)

        for kind, fingerprint in (
            ("cancel", "effect-fingerprint"),
            ("new", "different-fingerprint"),
        ):
            with self.assertRaises(IdempotencyConflict):
                self.store.reserve_effect_request(
                    "account-a", "effect-1", kind, fingerprint
                )
            self.assertEqual(
                self.store.get_effect_request("account-a", "effect-1"),
                inserted,
            )
        for kind, fingerprint in (("", "fingerprint"), ("new", "")):
            with self.assertRaises(IdempotencyConflict):
                self.store.reserve_effect_request(
                    "account-a", "missing-effect-field", kind, fingerprint
                )

        old = time.time() - 30 * 24 * 60 * 60
        self.raw_execute(
            "UPDATE gateway_effect_request SET created_at=? "
            "WHERE account_id=? AND request_id=?",
            (old, "account-a", "effect-1"),
        )
        self.store.cleanup_completed(time.time() - 8 * 24 * 60 * 60)
        persisted = self.store.get_effect_request("account-a", "effect-1")
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted["kind"], "new")
        self.store.close()
        self.store = OrderCorrelationStore(self.db_path)
        self.assertEqual(
            self.store.get_effect_request("account-a", "effect-1"),
            persisted,
        )

    def test_event_batch_is_one_atomic_transaction(self) -> None:
        self.raw_execute(
            """
            CREATE TRIGGER reject_bad_event
            BEFORE INSERT ON gateway_event_dedupe
            WHEN NEW.event_id='bad-event'
            BEGIN
                SELECT RAISE(ABORT, 'injected event failure');
            END
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.mark_events("account-a", ["good-event", "bad-event"])
        self.assertEqual(
            self.store.events_seen_many(
                "account-a", ["good-event", "bad-event"]
            ),
            set(),
        )
        self.assertFalse(self.store._db.in_transaction)

    def test_event_batches_are_thread_safe(self) -> None:
        batches = [
            ["thread-%02d-%03d" % (worker, index) for index in range(50)]
            for worker in range(8)
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            inserted = list(executor.map(
                lambda batch: self.store.mark_events("account-a", batch), batches
            ))
        self.assertEqual(sum(inserted), 400)
        all_ids = [event_id for batch in batches for event_id in batch]
        self.assertEqual(
            self.store.events_seen_many("account-a", all_ids), set(all_ids)
        )

    def test_cleanup_is_old_terminal_only_and_strictly_bounded(self) -> None:
        now = time.time()
        cutoff = now - 8 * 24 * 60 * 60
        old = now - 10 * 24 * 60 * 60
        recent = now - 24 * 60 * 60

        self.store.mark_events("account-a", ["event-old-1", "event-old-2", "event-recent"])
        self.raw_execute(
            "UPDATE gateway_event_dedupe SET created_at=? WHERE event_id IN (?,?)",
            (old, "event-old-1", "event-old-2"),
        )
        self.raw_execute(
            "UPDATE gateway_event_dedupe SET created_at=? WHERE event_id=?",
            (recent, "event-recent"),
        )

        for identity in ("terminal-old", "terminal-recent", "pending-old"):
            self.store.reserve(self.item(identity))
        self.store.update_stage("account-a", "terminal-old", "FILLED")
        self.store.update_stage("account-a", "terminal-recent", "REJECTED")
        self.raw_execute(
            "UPDATE order_correlation SET terminal_at=? WHERE client_order_id=?",
            (old, "terminal-old"),
        )
        self.raw_execute(
            "UPDATE order_correlation SET terminal_at=? WHERE client_order_id=?",
            (recent, "terminal-recent"),
        )
        # A malformed/nonterminal old timestamp must still never be deleted.
        self.raw_execute(
            "UPDATE order_correlation SET terminal_at=? WHERE client_order_id=?",
            (old, "pending-old"),
        )

        first = self.store.cleanup_completed(cutoff, event_limit=1, order_limit=1)
        self.assertEqual(first["events_deleted"], 1)
        self.assertEqual(first["orders_deleted"], 1)
        self.assertIsNone(self.store.get("account-a", "terminal-old"))
        self.assertIsNotNone(self.store.get("account-a", "terminal-recent"))
        self.assertIsNotNone(self.store.get("account-a", "pending-old"))
        self.assertTrue(self.store.event_seen("account-a", "event-recent"))
        old_events_left = self.store.events_seen_many(
            "account-a", ["event-old-1", "event-old-2"]
        )
        self.assertEqual(len(old_events_left), 1)

        second = self.store.cleanup_completed(cutoff, event_limit=1, order_limit=1)
        self.assertEqual(second["events_deleted"], 1)
        self.assertEqual(second["orders_deleted"], 0)
        self.assertEqual(
            self.store.events_seen_many(
                "account-a", ["event-old-1", "event-old-2", "event-recent"]
            ),
            {"event-recent"},
        )

        with self.assertRaisesRegex(ValueError, "retain at least"):
            self.store.cleanup_completed(now - 60)
        with self.assertRaisesRegex(ValueError, "event_limit"):
            self.store.cleanup_completed(
                cutoff,
                event_limit=self.store.MAX_CLEANUP_ROWS_PER_KIND + 1,
            )
        with self.assertRaisesRegex(ValueError, "order_limit"):
            self.store.cleanup_completed(cutoff, order_limit=-1)
        with self.assertRaises(TypeError):
            self.store.cleanup_completed(cutoff, event_limit=True)
        with self.assertRaisesRegex(ValueError, "finite"):
            self.store.cleanup_completed(float("nan"))

    def test_cleanup_is_atomic_across_events_and_orders(self) -> None:
        now = time.time()
        cutoff = now - 8 * 24 * 60 * 60
        old = now - 10 * 24 * 60 * 60
        self.store.mark_event("account-a", "rollback-event")
        self.raw_execute(
            "UPDATE gateway_event_dedupe SET created_at=? WHERE event_id=?",
            (old, "rollback-event"),
        )
        self.store.reserve(self.item("rollback-order"))
        self.store.update_stage("account-a", "rollback-order", "FILLED")
        self.raw_execute(
            "UPDATE order_correlation SET terminal_at=? WHERE client_order_id=?",
            (old, "rollback-order"),
        )
        self.raw_execute(
            """
            CREATE TRIGGER reject_order_cleanup
            BEFORE DELETE ON order_correlation
            WHEN OLD.client_order_id='rollback-order'
            BEGIN
                SELECT RAISE(ABORT, 'injected cleanup failure');
            END
            """
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.store.cleanup_completed(cutoff)
        self.assertTrue(self.store.event_seen("account-a", "rollback-event"))
        self.assertIsNotNone(self.store.get("account-a", "rollback-order"))
        self.assertFalse(self.store._db.in_transaction)

    def test_passive_checkpoint_is_explicit_and_reports_result(self) -> None:
        self.store.reserve(self.item("checkpoint"))
        result = self.store.checkpoint_wal()
        self.assertEqual(
            set(result), {"busy", "log_frames", "checkpointed_frames"}
        )
        self.assertGreaterEqual(result["busy"], 0)
        self.assertGreaterEqual(result["log_frames"], 0)
        self.assertGreaterEqual(result["checkpointed_frames"], 0)
        self.assertEqual(
            self.store._db.execute("PRAGMA synchronous").fetchone()[0], 1
        )
        self.assertEqual(
            str(self.store._db.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "wal",
        )


if __name__ == "__main__":
    unittest.main()
