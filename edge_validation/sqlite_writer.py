"""Dedicated background writer thread keeping SQLite I/O off asyncio WS event loops."""
from __future__ import annotations

from pathlib import Path
import queue
import sqlite3
import threading
from typing import Any, List, Optional, Tuple


def _apply_sql_op(con: sqlite3.Connection, kind: str, sql: str, arg: Any) -> bool:
    try:
        if kind == "exec":
            con.execute(sql, arg)
        elif kind == "many":
            con.executemany(sql, arg)
        elif kind == "commit":
            con.commit()
        return True
    except sqlite3.Error:
        return False


class AsyncSQLiteWriter:
    """Thread-backed SQLite queue writer (`WAL`, `synchronous=NORMAL`) for non-blocking 1Hz ingestion."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._q: queue.Queue[Tuple[str, Any, Any]] = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self.writes_failed: int = 0

    def start(self) -> None:
        """Start the daemon writer thread if not already active."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="recorder42-sqlite-writer", daemon=True)
        self._thread.start()

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        """Enqueue one parameterized SQL statement."""
        self._q.put_nowait(("exec", sql, params))

    def executemany(self, sql: str, rows: List[Tuple[Any, ...]]) -> None:
        """Enqueue a batch of parameterized SQL statements."""
        if rows:
            self._q.put_nowait(("many", sql, list(rows)))

    def commit(self) -> None:
        """Enqueue a transaction commit."""
        self._q.put_nowait(("commit", None, None))

    def close(self, timeout: float = 10.0) -> None:
        """Flush queued writes cleanly and join the writer thread."""
        if self._thread is None:
            return
        self._q.put_nowait(("stop", None, None))
        self._thread.join(timeout=timeout)
        self._thread = None

    def _run(self) -> None:
        con = sqlite3.connect(self.db_path, timeout=30.0)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=10000;")
        try:
            while True:
                kind, sql, arg = self._q.get()
                if kind == "stop":
                    break
                if not _apply_sql_op(con, kind, sql, arg):
                    self.writes_failed += 1
        finally:
            try:
                con.commit()
            except sqlite3.Error:
                self.writes_failed += 1
            con.close()


def query_pending_windows(db_path: Path, cutoff_ts: int) -> List[Tuple[int, str]]:
    """Fetch unresolved windows on a short read-only connection (safe for `asyncio.to_thread`)."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = con.execute(
            "SELECT window_ts, symbol FROM windows_42 WHERE window_ts < ? "
            "AND (resolved_outcome IS NULL OR gamma_final_price IS NULL) LIMIT 30",
            (cutoff_ts,),
        ).fetchall()
        return [(int(wt), str(sym)) for wt, sym in rows]
    finally:
        con.close()
