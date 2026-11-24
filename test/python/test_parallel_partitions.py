"""Independent partition streams through the real loadable scanner, no remote server."""

import ctypes
from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import adbc_driver_manager
import duckdb
import pytest


@pytest.fixture
def partition_driver(tmp_path):
    library = tmp_path / "partition-driver.so"
    subprocess.run(
        [
            os.environ.get("CC", "cc"),
            "-shared",
            "-fPIC",
            "-std=c11",
            "-pthread",
            "-I" + str(Path(adbc_driver_manager.__file__).parent),
            str(Path(__file__).with_name("partition_driver.c")),
            "-o",
            str(library),
        ],
        check=True,
    )
    control = ctypes.CDLL(str(library))
    control.AdbcPartitionCounter.argtypes = [ctypes.c_int]
    control.AdbcPartitionCounter.restype = ctypes.c_int
    control.AdbcPartitionSQL.restype = ctypes.c_char_p
    return library, control


def open_scanner(driver, *, mode="normal", scan_threads=4, threads=4):
    path = os.environ.get("ADBC_SCANNER_EXTENSION")
    if not path:
        pytest.skip("ADBC_SCANNER_EXTENSION must select a compatible locally built extension")
    conn = duckdb.connect(config={"allow_unsigned_extensions": True})
    try:
        conn.execute("LOAD " + "'" + path.replace("'", "''") + "'")
        conn.execute(f"SET threads={threads}")
        escaped = str(driver).replace("'", "''")
        conn.execute(
            f"ATTACH '' AS r (TYPE adbc, driver '{escaped}', entrypoint 'AdbcDriverPartitionTestInit', "
            f"mode '{mode}', READ_ONLY, query_pushdown false, scan_threads {scan_threads})"
        )
        return conn
    except BaseException:
        conn.close()
        raise


def expected(minimum=0):
    return [(i, None if i % 3 == 0 else Decimal(i % 4) * Decimal("1.25")) for i in range(minimum, 16)]


def assert_closed(conn, control):
    conn.close()
    assert control.AdbcPartitionCounter(5) == 0  # no live streams
    assert control.AdbcPartitionCounter(12) == 0  # producer statement survives all reader streams
    assert control.AdbcPartitionCounter(13) == 0  # database survives every connection
    assert control.AdbcPartitionCounter(15) == 0  # no connection released while active
    assert control.AdbcPartitionCounter(0) == control.AdbcPartitionCounter(1)
    assert control.AdbcPartitionCounter(2) == control.AdbcPartitionCounter(3)


def test_actual_parallel_partitions_copy_tokens_and_preserve_null_decimal_bag(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library)
    # EXPLAIN/binding must not execute or create a partition set.
    conn.execute("EXPLAIN SELECT * FROM r.main.items").fetchall()
    assert control.AdbcPartitionCounter(4) == 0
    assert conn.execute("SELECT * FROM r.main.items ORDER BY id").fetchall() == expected()
    assert control.AdbcPartitionCounter(4) == 1
    assert control.AdbcPartitionCounter(7) == 4
    assert 2 <= control.AdbcPartitionCounter(6) <= 4  # actual independent concurrent readers
    assert control.AdbcPartitionCounter(10) == 1  # descriptors released/poisoned before reads
    assert control.AdbcPartitionCounter(11) == 0  # no ExecuteQuery replay
    assert_closed(conn, control)


def test_filters_bind_once_and_projection_match_every_partition(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library)
    assert conn.execute("SELECT id, v FROM r.main.items WHERE id >= 7 ORDER BY id").fetchall() == expected(7)
    assert b"WHERE" in control.AdbcPartitionSQL()
    assert control.AdbcPartitionCounter(14) == 1
    assert control.AdbcPartitionCounter(4) == 1
    assert_closed(conn, control)


def test_projected_partition_schema_and_scheduler_cap(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library, threads=1)
    assert sorted(conn.execute("SELECT v FROM r.main.items").fetchall(), key=str) == sorted(
        [(v,) for _, v in expected()], key=str
    )
    assert control.AdbcPartitionCounter(6) == 1
    assert b'SELECT "v" FROM' in control.AdbcPartitionSQL()
    assert_closed(conn, control)


@pytest.mark.parametrize(
    "mode", ["error", "partial_unsupported", "schema", "error_after", "too_many", "too_large", "malformed"]
)
def test_execution_schema_or_late_failure_never_replays(partition_driver, mode):
    library, control = partition_driver
    conn = open_scanner(library, mode=mode)
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT * FROM r.main.items").fetchall()
    assert control.AdbcPartitionCounter(4) == 1
    assert control.AdbcPartitionCounter(11) == 0
    assert_closed(conn, control)


def test_only_unexposed_unsupported_execution_falls_back_once(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library, mode="unsupported")
    assert conn.execute("SELECT * FROM r.main.items ORDER BY id").fetchall() == expected()
    assert control.AdbcPartitionCounter(4) == 1
    assert control.AdbcPartitionCounter(11) == 1
    assert control.AdbcPartitionCounter(7) == 0
    assert_closed(conn, control)


def test_zero_partitions_is_successful_empty_without_replay(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library, mode="empty")
    assert conn.execute("SELECT * FROM r.main.items").fetchall() == []
    assert control.AdbcPartitionCounter(11) == control.AdbcPartitionCounter(7) == 0
    assert_closed(conn, control)


@pytest.mark.parametrize("mode", ["block", "block_open"])
def test_interrupt_cancels_reader_connections_and_joins_before_release(partition_driver, mode):
    library, control = partition_driver
    conn = open_scanner(library, mode=mode)
    errors = []

    def run():
        try:
            conn.execute("SELECT * FROM r.main.items ORDER BY id").fetchall()
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    deadline = time.monotonic() + 5
    while control.AdbcPartitionCounter(7) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    started = control.AdbcPartitionCounter(7)
    conn.interrupt()
    worker.join(5)
    assert started >= 2, "fixture did not activate independent blocked readers"
    assert not worker.is_alive(), "independent blocked partition readers did not cancel"
    assert errors
    assert control.AdbcPartitionCounter(9) >= 1
    assert_closed(conn, control)


def test_explicit_transaction_rejected_before_partition_execution(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library)
    conn.execute("BEGIN")
    with pytest.raises(duckdb.Error, match="autocommit"):
        conn.execute("SELECT * FROM r.main.items").fetchall()
    conn.execute("ROLLBACK")
    assert control.AdbcPartitionCounter(4) == 0
    assert_closed(conn, control)


def test_peer_failure_preserves_original_error_and_cancels_blocked_peer(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library, mode="peer_error")
    with pytest.raises(duckdb.Error, match="synthetic first partition source failure"):
        conn.execute("SELECT * FROM r.main.items ORDER BY id").fetchall()
    assert control.AdbcPartitionCounter(6) >= 2
    assert control.AdbcPartitionCounter(9) >= 1
    assert control.AdbcPartitionCounter(11) == 0
    assert_closed(conn, control)


def test_early_limit_releases_readers_before_producer(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library)
    assert len(conn.execute("SELECT * FROM r.main.items LIMIT 1").fetchall()) == 1
    assert_closed(conn, control)


def test_default_single_thread_keeps_original_query_execution(partition_driver):
    library, control = partition_driver
    conn = open_scanner(library, scan_threads=1)
    assert conn.execute("SELECT * FROM r.main.items ORDER BY id").fetchall() == expected()
    assert control.AdbcPartitionCounter(4) == 0
    assert control.AdbcPartitionCounter(11) == 1
    assert_closed(conn, control)


@pytest.mark.parametrize(
    "options",
    [
        "scan_threads 4",
        "READ_ONLY, scan_threads 4",
        "query_pushdown false, scan_threads 4",
        "READ_ONLY, query_pushdown false, scan_threads 0",
        "READ_ONLY, query_pushdown false, scan_threads 1025",
        "READ_ONLY, query_pushdown false, scan_threads '4'",
        "READ_ONLY, query_pushdown false, scan_threads true",
    ],
)
def test_attach_gate_rejects_unsafe_or_invalid_configuration_before_driver_open(partition_driver, options):
    library, control = partition_driver
    path = os.environ.get("ADBC_SCANNER_EXTENSION")
    if not path:
        pytest.skip("ADBC_SCANNER_EXTENSION must select a compatible locally built extension")
    conn = duckdb.connect(config={"allow_unsigned_extensions": True})
    try:
        conn.execute("LOAD '" + path.replace("'", "''") + "'")
        escaped = str(library).replace("'", "''")
        with pytest.raises(duckdb.Error, match="scan_threads"):
            conn.execute(
                f"ATTACH '' AS r (TYPE adbc, driver '{escaped}', entrypoint 'AdbcDriverPartitionTestInit', {options})"
            )
        assert control.AdbcPartitionCounter(0) == 0
    finally:
        conn.close()


def test_parallel_streaming_limit_does_not_start_blocked_peer(partition_driver):
    library, _ = partition_driver
    script = """
import ctypes, sys
from test_parallel_partitions import open_scanner, assert_closed
library=sys.argv[1]
control=ctypes.CDLL(library)
control.AdbcPartitionCounter.argtypes=[ctypes.c_int]
control.AdbcPartitionCounter.restype=ctypes.c_int
conn=open_scanner(library,mode='limit_peer')
conn.execute('SET preserve_insertion_order=false')
assert len(conn.execute('SELECT * FROM r.main.items LIMIT 1').fetchall()) == 1
assert control.AdbcPartitionCounter(7) == 1
assert_closed(conn,control)
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    subprocess.run([sys.executable, "-c", script, str(library)], env=env, check=True, timeout=10)
