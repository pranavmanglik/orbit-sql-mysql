"""MySQL adapter resource, query, transaction, and failure behavior with fake driver objects."""

from __future__ import annotations

import asyncio
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asyncmy
import pytest
from orbit_sql import DatabaseError, SQLDatabase, SQLReadPreference, SQLReadRouting
from pydantic import SecretStr, ValidationError

from orbit_sql_mysql import MySQLConfig, MySQLDatabase, MySQLReadReplica
from orbit_sql_mysql import database as database_module
from orbit_sql_mysql.plugin import MySQLPlugin


def test_asyncmy_dependency_floor_includes_cancelled_read_connection_discard_fix() -> None:
    """Do not allow a driver version that can reuse stale results after task cancellation."""
    manifest = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    asyncmy_requirement = next(
        requirement
        for requirement in manifest["project"]["dependencies"]
        if requirement.startswith("asyncmy")
    )
    assert asyncmy_requirement == "asyncmy>=0.2.12,<0.3"


def test_core_lifecycle_dependency_is_optional() -> None:
    """The SQL provider can be used without Core unless its plugin extra is installed."""
    manifest = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]
    assert not any(dep.startswith("orbit-core") for dep in manifest["dependencies"])
    assert manifest["optional-dependencies"]["core"] == ["orbit-core>=0.1.0a1,<0.2"]


class FakeCursor:
    """Small asyncmy DictCursor facade recording SQL and bounded reads."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.description = (("id",), ("name",))
        self.rowcount = 1
        self.lastrowid = 42

    async def __aenter__(self) -> FakeCursor:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, query: str, args: Sequence[object] = ()) -> int:
        self.connection.query = query
        self.connection.executed_queries.append(query)
        self.connection.args = tuple(args)
        return self.rowcount

    async def fetchone(self) -> Mapping[str, object] | None:
        return {"id": 42, "name": "Orbit"}

    async def fetchmany(self, size: int = 1) -> list[Mapping[str, object]]:
        self.connection.read_bound = size
        return [{"id": 42, "name": "Orbit"}]


class FakeConnection:
    """Record SQL and explicit transaction calls."""

    def __init__(self) -> None:
        self.query = ""
        self.executed_queries: list[str] = []
        self.args: tuple[object, ...] = ()
        self.read_bound = 0
        self.begun = 0
        self.committed = 0
        self.rolled_back = 0

    def cursor(self, _cursor: type[Any]) -> FakeCursor:
        return FakeCursor(self)

    async def begin(self) -> None:
        self.begun += 1

    async def commit(self) -> None:
        self.committed += 1

    async def rollback(self) -> None:
        self.rolled_back += 1


class FakeAcquire:
    """Async context manager tracking pool connection release."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.released = 0

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        self.released += 1


class FakePool:
    """Pool fake with close, termination, and a single pinned connection."""

    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.acquire_context = FakeAcquire(self.connection)
        self.closed = 0
        self.terminated = 0

    def acquire(self) -> FakeAcquire:
        return self.acquire_context

    def close(self) -> None:
        self.closed += 1

    async def wait_closed(self) -> None:
        return None

    def terminate(self) -> None:
        self.terminated += 1


async def test_mysql_implements_contract_and_binds_numbered_values() -> None:
    pool = FakePool()
    database = MySQLDatabase(pool, command_timeout=2, close_timeout=1)
    assert isinstance(database, SQLDatabase)
    assert isinstance(database, SQLReadRouting)

    result = await database.execute("INSERT INTO notes (name) VALUES ($1)", ("Orbit",))
    row = await database.fetch_one("SELECT id, name FROM notes WHERE id = $1", (42,))
    rows = await database.fetch_all("SELECT id, name FROM notes", limit=4)

    assert result.lastrowid == 42
    assert row is not None and row["name"] == "Orbit"
    assert len(rows) == 1
    assert pool.connection.query == "SELECT id, name FROM notes"
    assert pool.connection.read_bound == 5
    assert pool.acquire_context.released == 3
    await database.aclose()
    assert pool.closed == 1


async def test_explicit_replica_reads_are_weighted_and_server_read_only() -> None:
    """Replica reads use weighted pools and issue a server-enforced read-only transaction."""
    primary, replica_one, replica_two = FakePool(), FakePool(), FakePool()
    database = MySQLDatabase(
        primary,
        read_replica_pools=((replica_one, 1), (replica_two, 2)),
        command_timeout=2,
        close_timeout=1,
    )
    try:
        for _ in range(3):
            row = await database.fetch_one(
                "SELECT id FROM notes", read_preference=SQLReadPreference.REPLICA
            )
            assert row is not None
        assert primary.connection.executed_queries == []
        assert replica_one.connection.executed_queries == [
            "START TRANSACTION READ ONLY",
            "SELECT id FROM notes",
        ]
        assert replica_two.connection.executed_queries == [
            "START TRANSACTION READ ONLY",
            "SELECT id FROM notes",
            "START TRANSACTION READ ONLY",
            "SELECT id FROM notes",
        ]
        assert replica_one.connection.committed == 1
        assert replica_two.connection.committed == 2
    finally:
        await database.aclose()
    assert primary.closed == replica_one.closed == replica_two.closed == 1


async def test_replica_reads_fail_closed_without_replica_configuration() -> None:
    """Replica intent never silently routes to primary when no replica is configured."""
    pool = FakePool()
    database = MySQLDatabase(pool, command_timeout=2, close_timeout=1)
    try:
        with pytest.raises(DatabaseError) as caught:
            await database.fetch_one("SELECT 1", read_preference=SQLReadPreference.REPLICA)
        assert caught.value.code == "database.replica-unavailable"
        assert pool.connection.executed_queries == []
    finally:
        await database.aclose()


def test_mysql_replica_configuration_bounds_and_credential_pairing() -> None:
    """Replica topology limits and optional credential overrides are validated and redacted."""
    replica = MySQLReadReplica(
        host="reader.example.test",
        username="reader",
        password="private-password",
        weight=3,
    )
    config = MySQLConfig(
        host="writer.example.test",
        username="writer",
        password="private-password",
        database="orbit",
        read_replicas=(replica,),
    )
    assert "private-password" not in repr(config)
    with pytest.raises(ValidationError):
        MySQLReadReplica(host="reader.example.test")
    with pytest.raises(ValidationError, match="1,024 connections"):
        MySQLConfig(
            host="writer.example.test",
            username="writer",
            password="private-password",
            database="orbit",
            max_pool_size=32,
            read_replicas=tuple(
                MySQLReadReplica(
                    host=f"reader-{index}.example.test",
                    username="reader",
                    password="private-password",
                )
                for index in range(32)
            ),
        )


async def test_transactions_commit_or_rollback_and_release_connection() -> None:
    pool = FakePool()
    database = MySQLDatabase(pool, command_timeout=2, close_timeout=1)
    async with database.transaction() as transaction:
        await transaction.execute("UPDATE notes SET name = $1 WHERE id = $2", ("x", 42))
    assert pool.connection.begun == 1
    assert pool.connection.committed == 1
    assert pool.connection.rolled_back == 0
    assert pool.connection.args == ("x", 42)

    with pytest.raises(RuntimeError, match="rollback"):
        async with database.transaction():
            raise RuntimeError("rollback")
    assert pool.connection.rolled_back == 1
    assert pool.acquire_context.released == 2


async def test_transaction_cancellation_rolls_back_before_releasing() -> None:
    pool = FakePool()
    database = MySQLDatabase(pool, command_timeout=2, close_timeout=1)

    async def cancel_scope() -> None:
        async with database.transaction():
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await cancel_scope()
    assert pool.connection.rolled_back == 1
    assert pool.acquire_context.released == 1


async def test_open_uses_tls_bounded_pool_and_secret_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = FakePool()
    observed: dict[str, object] = {}

    async def create_pool(**settings: object) -> FakePool:
        observed.update(settings)
        return pool

    monkeypatch.setattr(asyncmy, "create_pool", create_pool)
    context = database_module.ssl.create_default_context()
    requested_ca: dict[str, str | None] = {}

    def create_context(*, cafile: str | None = None) -> database_module.ssl.SSLContext:
        requested_ca["cafile"] = cafile
        return context

    monkeypatch.setattr(database_module.ssl, "create_default_context", create_context)
    config = MySQLConfig(
        host="db.example.test",
        username="orbit_app",
        password=SecretStr("private-password"),
        database="app",
        ssl_ca="/etc/orbit-ca/mysql.pem",
    )
    database = await MySQLDatabase.open(config)
    assert observed["minsize"] == 1
    assert observed["maxsize"] == 10
    assert observed["autocommit"] is True
    assert observed["ssl"].verify_mode == database_module.ssl.CERT_REQUIRED  # type: ignore[union-attr]
    assert requested_ca["cafile"] == "/etc/orbit-ca/mysql.pem"
    assert "private-password" not in repr(config)
    await database.aclose()


async def test_open_creates_and_closes_primary_and_replica_pools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Opening the provider owns every configured endpoint and its credential settings."""
    pools = [FakePool(), FakePool()]
    settings: list[dict[str, object]] = []

    async def create_pool(**kwargs: object) -> FakePool:
        settings.append(kwargs)
        return pools[len(settings) - 1]

    monkeypatch.setattr(asyncmy, "create_pool", create_pool)
    config = MySQLConfig(
        host="writer.example.test",
        username="writer",
        password="writer-secret",
        database="orbit",
        read_replicas=(
            MySQLReadReplica(
                host="reader.example.test",
                username="reader",
                password="reader-secret",
                weight=2,
            ),
        ),
    )
    database = await MySQLDatabase.open(config)
    assert [entry["host"] for entry in settings] == [
        "writer.example.test",
        "reader.example.test",
    ]
    assert [entry["user"] for entry in settings] == ["writer", "reader"]
    await database.aclose()
    assert [pool.closed for pool in pools] == [1, 1]


def test_pool_bounds_and_unverified_tls_are_explicit() -> None:
    with pytest.raises(ValidationError):
        MySQLConfig(
            host="localhost",
            username="app",
            password="secret",
            database="app",
            min_pool_size=2,
            max_pool_size=1,
        )

    config = MySQLConfig(
        host="localhost", username="app", password="secret", database="app", ssl=False
    )
    assert config.ssl is False

    with pytest.raises(ValidationError, match="ssl_ca requires TLS"):
        MySQLConfig(
            host="localhost",
            username="app",
            password="secret",
            database="app",
            ssl=False,
            ssl_ca="/etc/orbit-ca/mysql.pem",
        )


async def test_close_timeout_terminates_pool_without_leaking_driver_error() -> None:
    pool = FakePool()

    async def never_close() -> None:
        await asyncio.Event().wait()

    pool.wait_closed = never_close  # type: ignore[method-assign]
    database = MySQLDatabase(pool, command_timeout=2, close_timeout=0.01)
    with pytest.raises(DatabaseError) as caught:
        await database.aclose()
    assert caught.value.code == "database.close-timeout"
    assert pool.terminated == 1


def test_mysql_plugin_registers_lazily_under_shared_capability_key() -> None:
    """Plugin setup is deferred to Core resource resolution and does not open sockets."""
    from orbit import Application, ApplicationConfig
    from orbit_sql import SQL_DATABASE_KEY

    application = Application(ApplicationConfig(name="mysql-plugin"))
    plugin = MySQLPlugin(
        MySQLConfig(host="localhost", username="app", password="secret", database="app", ssl=False)
    )
    application.plugins.register(plugin)
    application.plugins.setup(application)
    assert SQL_DATABASE_KEY in application.container.providers
    assert application.container.providers[SQL_DATABASE_KEY].resource


async def test_mysql_plugin_contributes_a_read_only_control_room_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI check executes SELECT 1 and releases its temporary pool."""
    pool = FakePool()

    async def create_pool(**_settings: object) -> FakePool:
        return pool

    monkeypatch.setattr(asyncmy, "create_pool", create_pool)
    plugin = MySQLPlugin(
        MySQLConfig(host="localhost", username="app", password="secret", database="app", ssl=False)
    )

    result = await plugin.control_actions[0].handler({})

    assert result is not None
    assert result.summary == "MySQL database is reachable."
    assert pool.connection.query == "SELECT 1"
    assert pool.closed == 1
