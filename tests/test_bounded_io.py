import asyncio
import sys
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / (chr(0x7F51) + chr(0x5173))
if str(GATEWAY_DIR) not in sys.path:
    sys.path.insert(0, str(GATEWAY_DIR))

from bounded_io import BoundedExecutorLane, IoLaneFull


class BoundedExecutorLaneTest(unittest.TestCase):
    def test_capacity_is_rejected_before_executor_submission(self):
        asyncio.run(self._capacity_contract())

    async def _capacity_contract(self):
        lane = BoundedExecutorLane("test", max_workers=1, max_pending=1)
        entered = threading.Event()
        release = threading.Event()

        def block():
            entered.set()
            release.wait(2.0)
            return "done"

        first = asyncio.create_task(lane.run(block))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1.0))
            self.assertEqual(lane.pending, 1)
            with self.assertRaises(IoLaneFull):
                await lane.run(lambda: "must-not-run")
            self.assertEqual(lane.pending, 1)
            release.set()
            self.assertEqual(await first, "done")
            self.assertEqual(lane.pending, 0)
        finally:
            release.set()
            await asyncio.gather(first, return_exceptions=True)
            lane.close()

    def test_exception_releases_capacity_and_close_is_idempotent(self):
        async def exercise():
            lane = BoundedExecutorLane("failure", 1, 1)

            def fail():
                raise ValueError("boom")

            with self.assertRaisesRegex(ValueError, "boom"):
                await lane.run(fail)
            self.assertEqual(lane.pending, 0)
            lane.close()
            lane.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                await lane.run(lambda: None)

        asyncio.run(exercise())

    def test_cancelled_waiter_keeps_capacity_until_worker_finishes(self):
        asyncio.run(self._cancel_contract())

    async def _cancel_contract(self):
        lane = BoundedExecutorLane("cancel", 1, 1)
        entered = threading.Event()
        release = threading.Event()

        def block():
            entered.set()
            release.wait(2.0)

        task = asyncio.create_task(lane.run(block))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1.0))
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(lane.pending, 1)
            with self.assertRaises(IoLaneFull):
                await lane.run(lambda: None)
            release.set()
            deadline = asyncio.get_running_loop().time() + 1.0
            while lane.pending:
                if asyncio.get_running_loop().time() >= deadline:
                    self.fail("worker capacity was not released")
                await asyncio.sleep(0.002)
        finally:
            release.set()
            lane.close()


if __name__ == "__main__":
    unittest.main()
