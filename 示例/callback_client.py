"""Long-running callback client with durable-before-ACK persistence."""

from __future__ import annotations

import argparse
import time

from common import (
    ENV_FILE,
    EventJournal,
    configure_logging,
    connect_or_raise,
    register_journal_handlers,
)

from qmt_local_api import LocalQmtApi


def parse_args():
    parser = argparse.ArgumentParser(description="run the durable local QMT callback client")
    parser.add_argument("--query-on-start", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api = LocalQmtApi.from_env(ENV_FILE)
    runtime = api.runtime
    assert runtime is not None
    configure_logging(runtime)
    journal = EventJournal(runtime.signal_journal)
    register_journal_handlers(api, journal)
    try:
        connect_or_raise(api)
        if args.query_on_start:
            api.query(args.query_on_start)
        print("callback client ready; press Ctrl+C to stop", flush=True)
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        return 0
    finally:
        try:
            api.stop()
        finally:
            journal.close()


if __name__ == "__main__":
    raise SystemExit(main())
