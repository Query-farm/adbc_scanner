"""Runtime-only commands, safe planning, and connection lifetime regressions.

Run against a locally built extension, with the optimizer enabled:
ADBC_SCANNER_EXTENSION=/path/adbc_scanner.duckdb_extension pytest test/python
Requires duckdb, pytest and adbc-driver-sqlite.
"""

import ctypes
import os
import sqlite3
import subprocess
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


def test_unknown_row_count_and_typed_options(database, counting_driver):
    db, _, _, _ = database
    path, counters = counting_driver
    counters.AdbcTestReset()
    handle = db.execute(
        """SELECT adbc_connect({
        'driver': ?, 'entrypoint': 'AdbcDriverTestInit',
        'test.string': 'example', 'test.int': 42,
        'test.double': 1.25::DOUBLE, 'test.bytes': from_hex('0001ff')
    })""",
        [path],
    ).fetchone()[0]
    assert [counters.AdbcTestCounter(i) for i in range(5, 9)] == [1, 1, 1, 1]
    db.execute(
        f"PREPARE unknown_count AS SELECT * FROM adbc_execute({handle}, 'command')"
    )
    assert counters.AdbcTestCounter(4) == 0
    assert db.execute("EXECUTE unknown_count").fetchall() == [(None,)]
    assert db.execute("EXECUTE unknown_count").fetchall() == [(None,)]
    assert counters.AdbcTestCounter(4) == 2
    assert counters.AdbcTestCounter(9) == 0
    db.execute(f"CALL adbc_disconnect({handle})")
    assert [counters.AdbcTestCounter(i) for i in range(4)] == [1, 1, 1, 1]


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
        db.execute(
            "SELECT adbc_connect({'driver': ?, 'entrypoint': 'AdbcDriverTestInit', '"
            + option
            + "': 'true'})",
            [path],
        )
    assert [counters.AdbcTestCounter(i) for i in range(4)] == expected


def test_failed_connect_vector_releases_unreturned_handles(database, counting_driver):
    db, _, _, _ = database
    path, counters = counting_driver
    counters.AdbcTestReset()
    with pytest.raises(duckdb.IOException):
        db.execute(
            """SELECT adbc_connect({
            'driver': ?, 'entrypoint': 'AdbcDriverTestInit',
            'fail_database': CASE WHEN i = 1 THEN 'true' ELSE NULL END
        }) FROM range(2) t(i)""",
            [path],
        )
    assert [counters.AdbcTestCounter(i) for i in range(4)] == [2, 2, 1, 1]


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
            db.execute("LOAD '" + extension.replace("'", "''") + "'")
            handle = db.execute(
                "SELECT adbc_connect({'driver': ?, 'uri': ?})",
                [adbc_driver_sqlite._driver_path(), str(path)],
            ).fetchone()[0]
            yield db, handle, observer, path


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
        ("adbc_disconnect", ""),
        ("adbc_commit", ""),
        ("adbc_rollback", ""),
        ("adbc_set_autocommit", ", false"),
    ],
)
def test_scalar_commands_removed(database, name, args):
    db, handle, observer, _ = database
    with pytest.raises(duckdb.BinderException, match="table function"):
        db.execute(f"SELECT {name}({handle}{args})").fetchall()
    assert count(observer) == 0


def test_transaction_commands_execute_only_at_runtime(database):
    db, handle, observer, _ = database
    db.execute(f"CALL adbc_set_autocommit({handle}, false)")
    db.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 1')")
    db.execute(f"EXPLAIN CALL adbc_commit({handle})").fetchall()
    assert count(observer) == 0
    db.execute(f"EXPLAIN CALL adbc_rollback({handle})").fetchall()
    db.execute(f"EXPLAIN CALL adbc_disconnect({handle})").fetchall()
    db.execute(f"EXPLAIN CALL adbc_set_autocommit({handle}, true)").fetchall()
    assert count(observer) == 0
    db.execute(f"CALL adbc_commit({handle})")
    assert count(observer) == 1
    db.execute(f"CALL adbc_execute({handle}, 'UPDATE counters SET value = 2')")
    db.execute(f"CALL adbc_rollback({handle})")
    assert count(observer) == 1


def test_handles_owned_by_context_and_never_reused(database):
    db, handle, _, path = database
    with (
        db.cursor() as other,
        pytest.raises(duckdb.InvalidInputException, match="Invalid connection handle"),
    ):
        other.execute(f"CALL adbc_disconnect({handle})")
    db.execute(f"CALL adbc_disconnect({handle})")
    handles = db.execute(
        "SELECT adbc_connect({'driver': ?, 'uri': ?}) FROM range(3)",
        [adbc_driver_sqlite._driver_path(), str(path)],
    ).fetchall()
    assert len(set(handles)) == 3
    assert all(row[0] > handle for row in handles)
    with pytest.raises(duckdb.InvalidInputException, match="Invalid connection handle"):
        db.execute(f"CALL adbc_execute({handle}, 'SELECT 1')")
    for (new_handle,) in handles:
        db.execute(f"CALL adbc_disconnect({new_handle})")


def test_context_close_rolls_back_and_releases_connection(database):
    db, _, observer, path = database
    with db.cursor() as other:
        handle = other.execute(
            "SELECT adbc_connect({'driver': ?, 'uri': ?})",
            [adbc_driver_sqlite._driver_path(), str(path)],
        ).fetchone()[0]
        other.execute(f"CALL adbc_set_autocommit({handle}, false)")
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


def test_connect_is_volatile_and_explain_does_not_open(database, tmp_path):
    db, _, _, _ = database
    nonexistent = tmp_path / "not-created.sqlite"
    db.execute(
        "EXPLAIN SELECT adbc_connect({'driver': ?, 'uri': ?})",
        [adbc_driver_sqlite._driver_path(), str(nonexistent)],
    ).fetchall()
    assert not nonexistent.exists()
    assert db.execute(
        "SELECT stability FROM duckdb_functions() WHERE function_name = 'adbc_connect'"
    ).fetchone() == ("VOLATILE",)


def test_extra_secret_options_are_redacted(database):
    db, _, _, _ = database
    db.execute(
        "CREATE SECRET test_secret (TYPE adbc, SCOPE 'test://', DRIVER 'sqlite', EXTRA_OPTIONS MAP {'grainlift.iroh.secret_key': 'redaction-sentinel'})"
    )
    rows = db.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 'test_secret'"
    ).fetchall()
    assert rows and "redaction-sentinel" not in str(rows)


def test_nested_options_rejected(database):
    db, _, _, _ = database
    with pytest.raises(duckdb.InvalidInputException, match="nested option values"):
        db.execute(
            "SELECT adbc_connect({'driver': 'sqlite', 'options': {'unknown': 1}})"
        )


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


def test_disconnected_prepared_scan_fails(database):
    db, handle, _, _ = database
    db.execute(
        f"PREPARE old_scan AS SELECT * FROM adbc_scan({handle}, 'SELECT value FROM counters', columns := {{'value': 'BIGINT'}})"
    )
    db.execute(f"CALL adbc_disconnect({handle})")
    with pytest.raises(duckdb.InvalidInputException, match="closed"):
        db.execute("EXECUTE old_scan").fetchall()


def test_clear_cache_is_table_function(database):
    db, _, _, _ = database
    with pytest.raises(duckdb.BinderException, match="table function"):
        db.execute("SELECT adbc_clear_cache()")
    assert db.execute("CALL adbc_clear_cache()").fetchall() == [(False,)]


def test_internal_catalog_handles_cannot_be_used_as_client_handles(database):
    db, handle, _, path = database
    db.execute(
        "ATTACH '"
        + str(path).replace("'", "''")
        + "' AS remote (TYPE adbc, driver '"
        + adbc_driver_sqlite._driver_path().replace("'", "''")
        + "')"
    )
    # The next registry ID belongs to the internal catalog connection. Knowing
    # its numeric ID must not grant command access to that internal connection.
    with pytest.raises(duckdb.InvalidInputException, match="Invalid connection handle"):
        db.execute(f"CALL adbc_disconnect({handle + 1})")
    assert db.execute("SELECT value FROM remote.main.counters").fetchall() == [(0,)]
    assert db.execute("CALL adbc_clear_cache()").fetchall() == [(True,)]
    db.execute("DETACH remote")
