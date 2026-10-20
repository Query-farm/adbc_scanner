"""Runtime-only commands, safe planning, and attached-database lifetime regressions.

Run against a locally built extension, with the optimizer enabled:
ADBC_SCANNER_EXTENSION=/path/adbc_scanner.duckdb_extension pytest test/python
Requires duckdb, pytest and adbc-driver-sqlite.
"""

import ctypes
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import adbc_driver_manager
import adbc_driver_sqlite
import pytest

import duckdb


@pytest.fixture(scope="module")
def counting_driver(tmp_path_factory):
    directory = tmp_path_factory.mktemp("counting-driver")
    library = directory / "counting.so"
    subprocess.run(
        [
            os.environ.get("CC", "cc"),
            "-shared",
            "-fPIC",
            "-std=c99",
            "-I" + str(Path(adbc_driver_manager.__file__).parent),
            str(Path(__file__).with_name("counting_driver.c")),
            "-o",
            str(library),
        ],
        check=True,
    )
    counter = ctypes.CDLL(str(library))
    counter.AdbcTestReset.argtypes = []
    counter.AdbcTestReset.restype = None
    counter.AdbcTestCounter.argtypes = [ctypes.c_int]
    counter.AdbcTestCounter.restype = ctypes.c_int
    yield str(library), counter


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def attach_counting(db, path, alias, entrypoint, options=""):
    db.execute(
        f"ATTACH '' AS {alias} (TYPE adbc, driver {quote(path)}, "
        f"entrypoint '{entrypoint}'{options})"
    )


def test_unknown_row_count_and_typed_options(database, counting_driver):
    db, _, _, _ = database
    path, counters = counting_driver
    counters.AdbcTestReset()
    attach_counting(
        db,
        path,
        "counting",
        "AdbcDriverTestInit",
        """, "test.string" 'example', "test.int" 42,
        "test.double" 1.25::DOUBLE, "test.bytes" from_hex('0001ff')""",
    )
    assert [counters.AdbcTestCounter(i) for i in range(5, 9)] == [1, 1, 1, 1]
    db.execute("PREPARE unknown_count AS SELECT * FROM adbc_execute('counting', 'command')")
    assert counters.AdbcTestCounter(4) == 0
    assert db.execute("EXECUTE unknown_count").fetchall() == [(None,)]
    assert db.execute("EXECUTE unknown_count").fetchall() == [(None,)]
    assert counters.AdbcTestCounter(4) == 2
    assert counters.AdbcTestCounter(9) == 0
    db.execute("DEALLOCATE unknown_count")
    db.execute("DETACH counting")
    # Every connection the attachment opened is released with it, then the database.
    assert counters.AdbcTestCounter(0) == counters.AdbcTestCounter(1) == 1
    assert counters.AdbcTestCounter(2) == counters.AdbcTestCounter(3) >= 1


@pytest.mark.parametrize("target", ["bulk", "fail_bind", "fail_execute", "fail_producer"])
def test_bulk_eager_binding_does_not_deadlock(counting_driver, target):
    """Bound startup, backpressure and failure cleanup with an eager driver."""
    path, _ = counting_driver
    # A subprocess watchdog also catches native deadlocks without wedging CI.
    script = r'''
import ctypes
import os
import sys
import duckdb

path, target = sys.argv[1:]
counts = ctypes.CDLL(path)
counts.AdbcTestCounter.argtypes = [ctypes.c_int]
counts.AdbcTestCounter.restype = ctypes.c_int
with duckdb.connect(config={"allow_unsigned_extensions": True}) as db:
    db.execute("LOAD '" + os.environ["ADBC_SCANNER_EXTENSION"].replace("'", "''") + "'")
    db.execute("ATTACH '' AS eager (TYPE adbc, driver '" + path.replace("'", "''") + "', entrypoint 'AdbcDriverEagerInit')")
    expression = "i" if target != "fail_producer" else "CASE WHEN i >= 4096 THEN error('synthetic producer failure') ELSE i END"
    try:
        result = db.execute(f"SELECT * FROM adbc_insert('eager', '{target}', (SELECT {expression} AS id FROM range(20000) t(i)), mode := 'create', max_batches := 1)").fetchall()
    except duckdb.Error:
        assert target != "bulk"
    else:
        assert target == "bulk"
        assert result == [(20000,)]
        assert counts.AdbcTestCounter(10) == 20000
        assert counts.AdbcTestCounter(11) > 1
        assert counts.AdbcTestCounter(4) == 1
    if target in {"fail_bind", "fail_producer"}:
        assert counts.AdbcTestCounter(4) == 0
    # The connection remains usable after failed ingestion.
    assert db.execute("CALL adbc_execute('eager', 'probe')").fetchall() == [(None,)]
    db.execute("DETACH eager")
'''
    subprocess.run([sys.executable, "-c", script, path, target], check=True, timeout=30)


@pytest.mark.parametrize(
    "option,expected",
    [
        ("fail_database", [1, 1, 0, 0]),
        ("fail_connection", [1, 1, 1, 1]),
    ],
)
def test_partial_initialization_released(database, counting_driver, option, expected):
    db, _, _, _ = database
    path, counters = counting_driver
    counters.AdbcTestReset()
    with pytest.raises(duckdb.IOException):
        attach_counting(db, path, "partial", "AdbcDriverTestInit", f", {option} 'true'")
    assert [counters.AdbcTestCounter(i) for i in range(4)] == expected
    assert db.execute(
        "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'partial'"
    ).fetchone() == (0,)


@pytest.fixture
def database(tmp_path):
    extension = str(Path(os.environ["ADBC_SCANNER_EXTENSION"]).resolve(strict=True))
    path = tmp_path / "shared.sqlite"
    with sqlite3.connect(path) as observer:
        observer.execute(
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)"
        )
        observer.execute("INSERT INTO counters VALUES (1, 0)")
        observer.commit()
        with duckdb.connect(config={"allow_unsigned_extensions": True}) as db:
            db.execute("LOAD " + quote(extension))
            db.execute(
                f"ATTACH {quote(path)} AS remote "
                f"(TYPE adbc, driver {quote(adbc_driver_sqlite._driver_path())})"
            )
            yield db, "'remote'", observer, path


def count(observer):
    return observer.execute("SELECT value FROM counters WHERE id = 1").fetchone()[0]


def test_explain_and_prepare_do_not_execute(database):
    db, handle, observer, _ = database
    command = f"adbc_execute({handle}, 'UPDATE counters SET value = value + 1')"
    db.execute("EXPLAIN CALL " + command).fetchall()
    assert count(observer) == 0
    db.execute("PREPARE increment AS SELECT * FROM " + command)
    assert count(observer) == 0
    assert db.execute("EXECUTE increment").fetchall() == [(1,)]
    assert count(observer) == 1
    assert db.execute("EXECUTE increment").fetchall() == [(1,)]
    assert count(observer) == 2


def test_affected_rows_and_error_recovery(database):
    db, handle, observer, _ = database
    assert db.execute(
        f"CALL adbc_execute({handle}, 'INSERT INTO counters VALUES (2, 0), (3, 0)')"
    ).fetchall() == [(2,)]
    assert db.execute(
        f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 1 WHERE id = 99')"
    ).fetchall() == [(0,)]
    with pytest.raises(duckdb.Error):
        db.execute(
            f"CALL adbc_execute({handle}, 'INSERT INTO counters VALUES (1, 0)')"
        ).fetchall()
    assert db.execute(
        f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 3 WHERE id = 1')"
    ).fetchall() == [(1,)]
    assert count(observer) == 3


@pytest.mark.parametrize(
    "name,args",
    [
        ("adbc_execute", ", 'DELETE FROM counters'"),
        ("adbc_clear_cache", None),
    ],
)
def test_scalar_commands_removed(database, name, args):
    db, handle, observer, _ = database
    call = f"{name}()" if args is None else f"{name}({handle}{args})"
    with pytest.raises(duckdb.BinderException, match="table function"):
        db.execute(f"SELECT {call}").fetchall()
    assert count(observer) == 0


@pytest.mark.parametrize(
    "call",
    [
        "adbc_connect({'driver': 'sqlite'})",
        "adbc_disconnect(1)",
        "adbc_commit(1)",
        "adbc_rollback(1)",
        "adbc_set_autocommit(1, false)",
    ],
)
def test_handle_functions_removed(database, call):
    db, _, _, _ = database
    with pytest.raises(duckdb.CatalogException, match="does not exist"):
        db.execute(f"CALL {call}")


def test_commands_join_duckdb_transactions(database):
    db, handle, observer, _ = database
    db.execute("BEGIN")
    db.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 1')")
    assert count(observer) == 0
    # Reads inside the transaction see its uncommitted writes.
    assert db.execute(
        f"SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'BIGINT'}})"
    ).fetchall() == [(1,)]
    db.execute("COMMIT")
    assert count(observer) == 1
    db.execute("BEGIN")
    db.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 2')")
    db.execute("ROLLBACK")
    assert count(observer) == 1


def test_alias_shared_across_cursors_until_detached(database):
    db, handle, _, path = database
    with db.cursor() as other:
        assert other.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 5')").fetchall() == [(1,)]
    with pytest.raises(duckdb.BinderException, match='no attached database named "missing"'):
        db.execute("CALL adbc_execute('missing', 'SELECT 1')")
    db.execute("ATTACH ':memory:' AS plain")
    with pytest.raises(duckdb.BinderException, match="not an ADBC one"):
        db.execute("CALL adbc_execute('plain', 'SELECT 1')")
    db.execute("DETACH remote")
    with pytest.raises(duckdb.BinderException, match='no attached database named "remote"'):
        db.execute(f"CALL adbc_execute({handle}, 'SELECT 1')")


def test_read_only_attachment_rejects_commands(database):
    db, _, observer, path = database
    db.execute(
        f"ATTACH {quote(path)} AS remote_ro "
        f"(TYPE adbc, driver {quote(adbc_driver_sqlite._driver_path())}, READ_ONLY)"
    )
    with pytest.raises(duckdb.PermissionException, match="read-only"):
        db.execute("CALL adbc_execute('remote_ro', 'UPDATE counters SET value = 7')")
    assert count(observer) == 0
    db.execute("DETACH remote_ro")


def test_context_close_rolls_back_and_releases_connection(database):
    db, handle, observer, _ = database
    with db.cursor() as other:
        other.execute("BEGIN")
        other.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 10')")
    assert count(observer) == 0
    observer.execute("UPDATE counters SET value = 11")
    observer.commit()
    assert count(observer) == 11


def test_explicit_scan_schema_planning_and_reexecution(database):
    db, handle, observer, _ = database
    query = f"SELECT * FROM adbc_scan({handle}, 'UPDATE counters SET value = value + 1 RETURNING value', columns := {{'value': 'BIGINT'}})"
    db.execute("EXPLAIN " + query).fetchall()
    assert count(observer) == 0
    db.execute("PREPARE scan_update AS " + query)
    assert count(observer) == 0
    assert db.execute("EXECUTE scan_update").fetchall() == [(1,)]
    assert db.execute("EXECUTE scan_update").fetchall() == [(2,)]
    assert count(observer) == 2


def test_schema_mismatch_rejected_and_statement_released(database):
    db, handle, _, _ = database
    with pytest.raises(duckdb.IOException, match="result type differs"):
        db.execute(
            f"SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'VARCHAR'}})"
        ).fetchall()
    assert db.execute(
        f"SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'BIGINT'}})"
    ).fetchall() == [(0,)]


def test_overlapping_statements_fail_without_deadlock(database):
    db, handle, _, _ = database
    scan = f"adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'BIGINT'}})"
    with pytest.raises(duckdb.InvalidInputException, match="active operation"):
        db.execute(f"SELECT * FROM {scan} a CROSS JOIN {scan} b").fetchall()
    assert db.execute(
        f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 1')"
    ).fetchall() == [(1,)]


def test_extra_secret_options_are_redacted(database):
    db, _, _, _ = database
    db.execute(
        "CREATE SECRET test_secret (TYPE adbc, SCOPE 'test://', DRIVER 'sqlite', EXTRA_OPTIONS MAP {'grainlift.iroh.secret_key': 'redaction-sentinel'})"
    )
    rows = db.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 'test_secret'"
    ).fetchall()
    assert rows and "redaction-sentinel" not in str(rows)


def test_empty_results_use_declared_types(database):
    db, handle, _, _ = database
    result = db.execute(
        f"SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters WHERE 0', columns := {{'value': 'VARCHAR'}})"
    )
    assert result.fetchall() == []
    assert str(result.description[0][1]) == "VARCHAR"


def test_missing_query_metadata_never_executes(database):
    db, handle, observer, _ = database
    with pytest.raises(duckdb.NotImplementedException, match="supply columns"):
        db.execute(
            f"EXPLAIN SELECT * FROM adbc_scan({handle}, 'UPDATE counters SET value = 100 RETURNING value')"
        )
    assert count(observer) == 0


def test_result_column_count_mismatch(database):
    db, handle, _, _ = database
    with pytest.raises(duckdb.IOException, match="column count differs"):
        db.execute(
            f"SELECT * FROM adbc_scan({handle}, 'SELECT id, value FROM counters', columns := {{'value': 'BIGINT'}})"
        ).fetchall()
    assert db.execute(
        f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 1')"
    ).fetchall() == [(1,)]


def test_detached_prepared_scan_fails(database):
    db, handle, _, _ = database
    db.execute(
        f"PREPARE old_scan AS SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'BIGINT'}})"
    )
    db.execute("DETACH remote")
    with pytest.raises(duckdb.Error, match="remote"):
        db.execute("EXECUTE old_scan").fetchall()


def test_clear_cache_is_table_function(database):
    db, _, _, _ = database
    assert db.execute("SELECT value FROM remote.main.counters").fetchall() == [(0,)]
    assert db.execute("CALL adbc_clear_cache()").fetchall() == [(True,)]
    db.execute("DETACH remote")
    assert db.execute("CALL adbc_clear_cache()").fetchall() == [(False,)]
