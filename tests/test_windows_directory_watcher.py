import asyncio
import importlib.util
from pathlib import Path
import threading
import unittest


ROOT = Path(__file__).resolve().parents[1]
WATCHER_PATHS = list(ROOT.rglob("windows_directory_watcher.py"))
if len(WATCHER_PATHS) != 1:
    raise RuntimeError("expected exactly one windows_directory_watcher.py")
SPEC = importlib.util.spec_from_file_location(
    "windows_directory_watcher",
    WATCHER_PATHS[0],
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load windows directory watcher")
watcher_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watcher_module)

WakeReason = watcher_module.WakeReason
WindowsDirectoryWatcher = watcher_module.WindowsDirectoryWatcher


class BlockingBackend:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._notifications = 0
        self._closed = False
        self.close_calls = 0
        self.ready = threading.Event()

    def wait(self) -> bool:
        self.ready.set()
        with self._condition:
            while not self._notifications and not self._closed:
                self._condition.wait()
            if self._closed:
                return False
            self._notifications -= 1
            return True

    def notify(self) -> None:
        with self._condition:
            self._notifications += 1
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            if not self._closed:
                self._closed = True
                self.close_calls += 1
            self._condition.notify_all()


class FailingBackend:
    def __init__(self) -> None:
        self.closed = False

    def wait(self) -> bool:
        raise OSError("simulated invalid directory handle")

    def close(self) -> None:
        self.closed = True


async def wait_for_thread_event(event: threading.Event, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not event.is_set():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("worker thread event was not set")
        await asyncio.sleep(0.002)


class WindowsDirectoryWatcherTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.watchers = []

    async def asyncTearDown(self) -> None:
        for watcher in self.watchers:
            self.assertTrue(await watcher.aclose(1.0))

    def create_watcher(self, factory, **overrides):
        arguments = {
            "backend_factory": factory,
            "retry_interval": 0.01,
            "default_timeout": 0.05,
        }
        arguments.update(overrides)
        watcher = WindowsDirectoryWatcher(ROOT, **arguments)
        self.watchers.append(watcher)
        return watcher

    async def test_notification_is_only_an_async_wake_hint(self):
        backend = BlockingBackend()
        watcher = self.create_watcher(lambda path: backend)
        await wait_for_thread_event(backend.ready)

        pending = asyncio.create_task(watcher.wait(0.5))
        await asyncio.sleep(0)
        backend.notify()
        self.assertEqual(await pending, WakeReason.CHANGED)

    async def test_timeout_fallback_does_not_block_the_event_loop(self):
        backend = BlockingBackend()
        watcher = self.create_watcher(lambda path: backend)
        await wait_for_thread_event(backend.ready)
        marker = []

        async def mark_progress():
            await asyncio.sleep(0.005)
            marker.append("event-loop-ran")

        reason, _ = await asyncio.gather(watcher.wait(0.03), mark_progress())
        self.assertEqual(reason, WakeReason.TIMEOUT)
        self.assertEqual(marker, ["event-loop-ran"])
        self.assertTrue(watcher.thread_alive)

    async def test_stop_is_thread_safe_and_closes_pending_wait(self):
        backend = BlockingBackend()
        watcher = self.create_watcher(lambda path: backend)
        await wait_for_thread_event(backend.ready)
        pending = asyncio.create_task(watcher.wait(5.0))
        await asyncio.sleep(0)

        closer = threading.Thread(target=watcher.stop)
        closer.start()
        closer.join(1.0)
        self.assertFalse(closer.is_alive())
        self.assertEqual(await asyncio.wait_for(pending, 0.5), WakeReason.CLOSED)
        watcher.close()
        self.assertEqual(await watcher.wait(0.5), WakeReason.CLOSED)
        self.assertTrue(await watcher.aclose(1.0))
        self.assertEqual(backend.close_calls, 1)

    async def test_missing_directory_and_handle_failure_retry_to_notification(self):
        directory_available = threading.Event()
        stable = BlockingBackend()
        failing = FailingBackend()
        calls = []
        failing_returned = False
        factory_lock = threading.Lock()

        def factory(path):
            nonlocal failing_returned
            del path
            with factory_lock:
                calls.append("factory")
                if not directory_available.is_set():
                    raise FileNotFoundError("simulated missing directory")
                if not failing_returned:
                    failing_returned = True
                    return failing
                return stable

        watcher = self.create_watcher(factory, retry_interval=0.005)
        self.assertEqual(await watcher.wait(0.03), WakeReason.TIMEOUT)
        self.assertTrue(watcher.degraded)
        self.assertIn("FileNotFoundError", watcher.last_error or "")

        directory_available.set()
        await wait_for_thread_event(stable.ready)
        self.assertTrue(failing.closed)
        stable.notify()
        self.assertEqual(await watcher.wait(0.5), WakeReason.CHANGED)
        self.assertIsNone(watcher.last_error)


if __name__ == "__main__":
    unittest.main()
