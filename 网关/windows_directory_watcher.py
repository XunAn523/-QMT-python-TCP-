#!/usr/bin/env python3
"""Lossy directory-change wakeups for the Windows Gateway.

This module deliberately does *not* turn directory notifications into a source
of truth.  ``WakeReason.CHANGED`` is only a coalesced hint, and
``WakeReason.TIMEOUT`` is the polling fallback.  A caller must perform its own
bounded directory scan after either result.

The blocking Win32 operation runs on one private daemon thread.  Importing this
module and using an injected backend is safe on non-Windows platforms.
"""

from __future__ import annotations

import asyncio
import ctypes
from ctypes import wintypes
from enum import Enum
import errno
import os
from pathlib import Path
import threading
from typing import Callable, Dict, Optional, Protocol, Tuple


class WakeReason(str, Enum):
    """Why an async watcher wait completed."""

    CHANGED = "changed"
    TIMEOUT = "timeout"
    CLOSED = "closed"


class DirectoryWatchBackend(Protocol):
    """Blocking backend contract used by the watcher worker thread."""

    def wait(self) -> bool:
        """Block until a change; return false when the backend is closed."""

    def close(self) -> None:
        """Cancel a pending wait and release resources; must be idempotent."""


BackendFactory = Callable[[str], DirectoryWatchBackend]
_AsyncWaiter = Tuple[asyncio.AbstractEventLoop, asyncio.Future[WakeReason]]


if os.name == "nt":
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class _OVERLAPPED(ctypes.Structure):
        _fields_ = (
            ("Internal", ctypes.c_size_t),
            ("InternalHigh", ctypes.c_size_t),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        )

    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    _CreateFileW.restype = wintypes.HANDLE

    _CreateEventW = _kernel32.CreateEventW
    _CreateEventW.argtypes = (
        wintypes.LPVOID,
        wintypes.BOOL,
        wintypes.BOOL,
        wintypes.LPCWSTR,
    )
    _CreateEventW.restype = wintypes.HANDLE

    _ReadDirectoryChangesW = _kernel32.ReadDirectoryChangesW
    _ReadDirectoryChangesW.argtypes = (
        wintypes.HANDLE,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(_OVERLAPPED),
        wintypes.LPVOID,
    )
    _ReadDirectoryChangesW.restype = wintypes.BOOL

    _GetOverlappedResult = _kernel32.GetOverlappedResult
    _GetOverlappedResult.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_OVERLAPPED),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.BOOL,
    )
    _GetOverlappedResult.restype = wintypes.BOOL

    _WaitForSingleObject = _kernel32.WaitForSingleObject
    _WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    _WaitForSingleObject.restype = wintypes.DWORD

    _ResetEvent = _kernel32.ResetEvent
    _ResetEvent.argtypes = (wintypes.HANDLE,)
    _ResetEvent.restype = wintypes.BOOL

    _CancelIoEx = _kernel32.CancelIoEx
    _CancelIoEx.argtypes = (wintypes.HANDLE, ctypes.c_void_p)
    _CancelIoEx.restype = wintypes.BOOL

    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = (wintypes.HANDLE,)
    _CloseHandle.restype = wintypes.BOOL


_FILE_LIST_DIRECTORY = 0x0001
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_OPEN_EXISTING = 3
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_FLAG_OVERLAPPED = 0x40000000
_FILE_NOTIFY_CHANGE_FILE_NAME = 0x00000001
_FILE_NOTIFY_CHANGE_DIR_NAME = 0x00000002
_FILE_NOTIFY_CHANGE_ATTRIBUTES = 0x00000004
_FILE_NOTIFY_CHANGE_SIZE = 0x00000008
_FILE_NOTIFY_CHANGE_LAST_WRITE = 0x00000010
_FILE_NOTIFY_CHANGE_CREATION = 0x00000040
_NOTIFY_FILTER = (
    _FILE_NOTIFY_CHANGE_FILE_NAME
    | _FILE_NOTIFY_CHANGE_DIR_NAME
    | _FILE_NOTIFY_CHANGE_ATTRIBUTES
    | _FILE_NOTIFY_CHANGE_SIZE
    | _FILE_NOTIFY_CHANGE_LAST_WRITE
    | _FILE_NOTIFY_CHANGE_CREATION
)
_ERROR_INVALID_HANDLE = 6
_ERROR_OPERATION_ABORTED = 995
_ERROR_IO_PENDING = 997
_ERROR_NOT_FOUND = 1168
_WAIT_OBJECT_0 = 0
_WAIT_FAILED = 0xFFFFFFFF
_INFINITE = 0xFFFFFFFF
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


def _win_error(operation: str, code: Optional[int] = None) -> OSError:
    error_code = ctypes.get_last_error() if code is None else int(code)
    return OSError(error_code, "%s failed: %s" % (operation, ctypes.FormatError(error_code)))


class _ReadDirectoryChangesBackend:
    """One overlapped ``ReadDirectoryChangesW`` directory handle."""

    def __init__(
        self,
        path: str,
        *,
        watch_subtree: bool = False,
        buffer_size: int = 64 * 1024,
    ) -> None:
        if os.name != "nt":
            raise OSError(errno.ENOSYS, "ReadDirectoryChangesW is only available on Windows")
        if buffer_size <= 0 or buffer_size > 64 * 1024:
            raise ValueError("buffer_size must be in 1..65536")

        self._lock = threading.Lock()
        self._closed = False
        self._waiting = False
        self._overlapped: Optional[_OVERLAPPED] = None
        self._buffer = ctypes.create_string_buffer(buffer_size)
        self._buffer_size = buffer_size
        self._watch_subtree = bool(watch_subtree)
        self._directory_handle: Optional[int] = None
        self._event_handle: Optional[int] = None

        directory_handle = _CreateFileW(
            path,
            _FILE_LIST_DIRECTORY,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
            None,
            _OPEN_EXISTING,
            _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OVERLAPPED,
            None,
        )
        if directory_handle == _INVALID_HANDLE_VALUE:
            raise _win_error("CreateFileW(directory)")
        self._directory_handle = directory_handle

        event_handle = _CreateEventW(None, True, False, None)
        if not event_handle:
            error = _win_error("CreateEventW")
            _CloseHandle(directory_handle)
            self._directory_handle = None
            raise error
        self._event_handle = event_handle

    def wait(self) -> bool:
        try:
            # Issue the overlapped request while holding the same lock used by
            # close().  This prevents close from observing ``_waiting`` before
            # there is an I/O operation for CancelIoEx to cancel.
            with self._lock:
                if self._closed:
                    return False
                if self._waiting:
                    raise RuntimeError("concurrent backend waits are not supported")
                directory_handle = self._directory_handle
                event_handle = self._event_handle
                if not directory_handle or not event_handle:
                    return False
                if not _ResetEvent(event_handle):
                    raise _win_error("ResetEvent")
                overlapped = _OVERLAPPED()
                overlapped.hEvent = event_handle
                self._overlapped = overlapped
                self._waiting = True

                returned = wintypes.DWORD(0)
                started = _ReadDirectoryChangesW(
                    directory_handle,
                    self._buffer,
                    self._buffer_size,
                    self._watch_subtree,
                    _NOTIFY_FILTER,
                    ctypes.byref(returned),
                    ctypes.byref(overlapped),
                    None,
                )
                if not started:
                    error_code = ctypes.get_last_error()
                    if error_code != _ERROR_IO_PENDING:
                        raise _win_error("ReadDirectoryChangesW", error_code)

            wait_result = _WaitForSingleObject(event_handle, _INFINITE)
            if wait_result == _WAIT_FAILED:
                raise _win_error("WaitForSingleObject")
            if wait_result != _WAIT_OBJECT_0:
                raise OSError(int(wait_result), "unexpected directory wait result")

            transferred = wintypes.DWORD(0)
            completed = _GetOverlappedResult(
                directory_handle,
                ctypes.byref(overlapped),
                ctypes.byref(transferred),
                True,
            )
            if not completed:
                error_code = ctypes.get_last_error()
                with self._lock:
                    closed = self._closed
                if closed and error_code in (
                    _ERROR_INVALID_HANDLE,
                    _ERROR_OPERATION_ABORTED,
                ):
                    return False
                raise _win_error("GetOverlappedResult", error_code)
            with self._lock:
                return not self._closed
        finally:
            close_handles: Tuple[Optional[int], Optional[int]] = (None, None)
            with self._lock:
                self._waiting = False
                self._overlapped = None
                if self._closed:
                    close_handles = self._detach_handles_locked()
            self._close_handles(close_handles)

    def close(self) -> None:
        close_handles: Tuple[Optional[int], Optional[int]] = (None, None)
        cancel: Tuple[Optional[int], Optional[_OVERLAPPED]] = (None, None)
        with self._lock:
            if not self._closed:
                self._closed = True
            if self._waiting:
                cancel = (self._directory_handle, self._overlapped)
            else:
                close_handles = self._detach_handles_locked()

        directory_handle, overlapped = cancel
        if directory_handle and overlapped is not None:
            cancelled = _CancelIoEx(directory_handle, ctypes.byref(overlapped))
            if not cancelled:
                error_code = ctypes.get_last_error()
                if error_code not in (
                    _ERROR_INVALID_HANDLE,
                    _ERROR_NOT_FOUND,
                    _ERROR_OPERATION_ABORTED,
                ):
                    # The worker still owns the OVERLAPPED memory and will close
                    # both handles after the pending call settles.
                    pass
        self._close_handles(close_handles)

    def _detach_handles_locked(self) -> Tuple[Optional[int], Optional[int]]:
        handles = (self._directory_handle, self._event_handle)
        self._directory_handle = None
        self._event_handle = None
        return handles

    @staticmethod
    def _close_handles(handles: Tuple[Optional[int], Optional[int]]) -> None:
        directory_handle, event_handle = handles
        if directory_handle:
            _CloseHandle(directory_handle)
        if event_handle:
            _CloseHandle(event_handle)


def _default_backend_factory(
    path: str,
    *,
    watch_subtree: bool,
    buffer_size: int,
) -> DirectoryWatchBackend:
    if os.name != "nt":
        raise OSError(errno.ENOSYS, "directory notifications are unavailable on this platform")
    return _ReadDirectoryChangesBackend(
        path,
        watch_subtree=watch_subtree,
        buffer_size=buffer_size,
    )


class WindowsDirectoryWatcher:
    """Async wake hints backed by a restartable blocking directory watcher.

    Backend creation and waits happen only on the private worker thread.  If a
    directory is absent, is recreated, or its handle fails, the worker retries
    after ``retry_interval``.  Meanwhile ``wait`` returns ``TIMEOUT`` at the
    caller's requested interval, preserving bounded polling behavior.
    """

    def __init__(
        self,
        path: os.PathLike[str] | str,
        *,
        backend_factory: Optional[BackendFactory] = None,
        retry_interval: float = 0.25,
        default_timeout: float = 0.25,
        watch_subtree: bool = False,
        buffer_size: int = 64 * 1024,
        thread_name: Optional[str] = None,
    ) -> None:
        if retry_interval <= 0:
            raise ValueError("retry_interval must be positive")
        if default_timeout < 0:
            raise ValueError("default_timeout cannot be negative")
        if buffer_size <= 0 or buffer_size > 64 * 1024:
            raise ValueError("buffer_size must be in 1..65536")

        self.path = str(Path(path).resolve())
        self.retry_interval = float(retry_interval)
        self.default_timeout = float(default_timeout)
        if backend_factory is None:
            self._backend_factory: BackendFactory = lambda current_path: _default_backend_factory(
                current_path,
                watch_subtree=watch_subtree,
                buffer_size=buffer_size,
            )
        else:
            self._backend_factory = backend_factory

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._done_event = threading.Event()
        self._closed = False
        self._pending_hint = False
        self._backend: Optional[DirectoryWatchBackend] = None
        self._last_error: Optional[str] = None
        self._waiter_sequence = 0
        self._waiters: Dict[int, _AsyncWaiter] = {}
        self._thread = threading.Thread(
            target=self._worker,
            name=thread_name or "qmt-directory-watcher",
            daemon=True,
        )
        self._thread.start()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def degraded(self) -> bool:
        with self._lock:
            return not self._closed and self._backend is None

    @property
    def last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    @property
    def thread_alive(self) -> bool:
        return self._thread.is_alive()

    async def wait(self, timeout: Optional[float] = None) -> WakeReason:
        """Wait asynchronously for a hint, timeout poll, or watcher close."""
        effective_timeout = self.default_timeout if timeout is None else float(timeout)
        if effective_timeout < 0:
            raise ValueError("timeout cannot be negative")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[WakeReason] = loop.create_future()
        with self._lock:
            if self._closed:
                return WakeReason.CLOSED
            if self._pending_hint:
                self._pending_hint = False
                return WakeReason.CHANGED
            self._waiter_sequence += 1
            token = self._waiter_sequence
            self._waiters[token] = (loop, future)

        try:
            return await asyncio.wait_for(asyncio.shield(future), effective_timeout)
        except asyncio.TimeoutError:
            with self._lock:
                self._waiters.pop(token, None)
                closed = self._closed
            if not future.done():
                future.cancel()
            return WakeReason.CLOSED if closed else WakeReason.TIMEOUT
        except asyncio.CancelledError:
            with self._lock:
                self._waiters.pop(token, None)
            if not future.done():
                future.cancel()
            raise

    def stop(self) -> None:
        """Thread-safe, non-blocking alias for ``close``."""
        self.close()

    def close(self) -> None:
        """Cancel backend I/O and wake async waiters without blocking a loop."""
        backend: Optional[DirectoryWatchBackend]
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._pending_hint = False
            self._stop_event.set()
            backend = self._backend
            waiters = list(self._waiters.values())
            self._waiters.clear()
        self._complete_waiters(waiters, WakeReason.CLOSED)
        if backend is not None:
            try:
                backend.close()
            except Exception as exc:
                self._record_error(exc)

    def join(self, timeout: Optional[float] = None) -> bool:
        """Synchronously join the worker; never call this on an event loop."""
        if threading.current_thread() is self._thread:
            return self._done_event.is_set()
        self._thread.join(timeout)
        return not self._thread.is_alive()

    async def aclose(self, timeout: Optional[float] = 1.0) -> bool:
        """Close and join without blocking the asyncio event-loop thread."""
        self.close()
        return await asyncio.to_thread(self.join, timeout)

    def __enter__(self) -> "WindowsDirectoryWatcher":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.close()

    def _worker(self) -> None:
        final_waiters: list[_AsyncWaiter] = []
        try:
            while not self._stop_event.is_set():
                backend: Optional[DirectoryWatchBackend] = None
                try:
                    backend = self._backend_factory(self.path)
                    with self._lock:
                        if self._closed:
                            backend.close()
                            break
                        self._backend = backend
                        self._last_error = None

                    while not self._stop_event.is_set():
                        changed = backend.wait()
                        if self._stop_event.is_set():
                            break
                        if not changed:
                            raise OSError(errno.EIO, "directory watcher backend stopped")
                        self._publish_hint()
                except Exception as exc:
                    if not self._stop_event.is_set():
                        self._record_error(exc)
                finally:
                    if backend is not None:
                        with self._lock:
                            if self._backend is backend:
                                self._backend = None
                        try:
                            backend.close()
                        except Exception as exc:
                            self._record_error(exc)

                if not self._stop_event.is_set():
                    self._stop_event.wait(self.retry_interval)
        finally:
            with self._lock:
                self._backend = None
                if not self._closed:
                    self._closed = True
                    self._stop_event.set()
                    final_waiters = list(self._waiters.values())
                    self._waiters.clear()
            self._complete_waiters(final_waiters, WakeReason.CLOSED)
            self._done_event.set()

    def _publish_hint(self) -> None:
        with self._lock:
            if self._closed:
                return
            if not self._waiters:
                self._pending_hint = True
                return
            waiters = list(self._waiters.values())
            self._waiters.clear()
        self._complete_waiters(waiters, WakeReason.CHANGED)

    def _record_error(self, exc: BaseException) -> None:
        with self._lock:
            if not self._closed:
                self._last_error = "%s: %s" % (type(exc).__name__, exc)

    @staticmethod
    def _complete_waiters(waiters: list[_AsyncWaiter], result: WakeReason) -> None:
        for loop, future in waiters:
            try:
                loop.call_soon_threadsafe(
                    WindowsDirectoryWatcher._set_future_result,
                    future,
                    result,
                )
            except RuntimeError:
                # The owning event loop has already closed; its Future is no
                # longer observable and must not affect watcher shutdown.
                continue

    @staticmethod
    def _set_future_result(
        future: asyncio.Future[WakeReason],
        result: WakeReason,
    ) -> None:
        if not future.done():
            future.set_result(result)


__all__ = [
    "BackendFactory",
    "DirectoryWatchBackend",
    "WakeReason",
    "WindowsDirectoryWatcher",
]
