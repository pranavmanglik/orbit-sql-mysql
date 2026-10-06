# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Asyncmy-backed pooled MySQL database implementing Orbit's SQL contract."""

from __future__ import annotations

import asyncio
import math
import ssl
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Protocol, cast
from uuid import UUID

import asyncmy
from asyncmy.cursors import SSDictCursor
from orbit_sql import DatabaseError, SQLExecution, SQLParameters, SQLRow, SQLTransaction, SQLValue

from orbit_sql_mysql.config import MySQLConfig

_MAX_SQL_BYTES = 1_000_000
_MAX_ROWS = 10_000


class _Cursor(Protocol):
    """Minimal async cursor surface consumed from asyncmy."""

    description: Sequence[tuple[Any, ...]] | None
    rowcount: int
    lastrowid: int | None

    async def execute(self, query: str, args: Sequence[SQLValue] = ()) -> int:
        """Run one statement with bound values and return its driver row count."""

    async def fetchone(self) -> Mapping[str, SQLValue] | Sequence[SQLValue] | None:
        """Fetch one row from the current result, if present."""

    async def fetchmany(
        self, size: int = 1
    ) -> list[Mapping[str, SQLValue] | Sequence[SQLValue]]:
        """Fetch at most the requested number of rows."""

    async def __aenter__(self) -> _Cursor:
        """Enter the async cursor scope."""

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool | None:
        """Release the cursor and propagate any operation failure."""


class _Connection(Protocol):
    """Minimal async connection surface consumed from asyncmy."""

    def cursor(self, cursor: type[SSDictCursor]) -> AbstractAsyncContextManager[_Cursor]:
        """Create a managed streaming dictionary cursor."""

    async def begin(self) -> None:
        """Begin an explicit transaction."""

    async def commit(self) -> None:
        """Commit the current transaction."""

    async def rollback(self) -> None:
        """Roll back the current transaction."""


class _Acquire(Protocol):
    """Async pool acquisition context used to return connections reliably."""

    async def __aenter__(self) -> _Connection:
        """Acquire and return a pooled connection."""

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool | None:
        """Return the connection to the pool."""


class _Pool(Protocol):
    """Pool operations required for bounded acquisition and shutdown."""

    def acquire(self) -> _Acquire:
        """Create a context that acquires and releases one connection."""

    def close(self) -> None:
        """Stop creating new connections and begin graceful shutdown."""

    async def wait_closed(self) -> None:
        """Wait for acquired connections to close."""

    def terminate(self) -> None:
        """Force-close pool connections after the graceful timeout."""


class MySQLDatabase:
    """Pooled, bounded MySQL access with redacted errors and explicit transaction scopes."""

    def __init__(self, pool: _Pool, *, command_timeout: float, close_timeout: float) -> None:
        """Wrap an open asyncmy pool; applications should normally use `open`."""
        for method in ("acquire", "close", "wait_closed", "terminate"):
            if not callable(getattr(pool, method, None)):
                raise TypeError(f"pool must provide callable {method} method.")
        self._pool = pool
        self._command_timeout = command_timeout
        self._close_timeout = close_timeout
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    async def open(cls, config: MySQLConfig) -> MySQLDatabase:
        """Open the configured pool without retaining credentials in diagnostics."""
        if not isinstance(config, MySQLConfig):
            raise TypeError("config must be MySQLConfig.")
        tls: ssl.SSLContext | None = (
            ssl.create_default_context(cafile=config.ssl_ca) if config.ssl else None
        )
        try:
            pool = cast(
                _Pool,
                await asyncmy.create_pool(
                    host=config.host,
                    port=config.port,
                    user=config.username,
                    password=config.password.get_secret_value(),
                    db=config.database,
                    minsize=config.min_pool_size,
                    maxsize=config.max_pool_size,
                    connect_timeout=config.connect_timeout,
                    read_timeout=config.command_timeout,
                    pool_recycle=config.pool_recycle_seconds,
                    autocommit=True,
                    ssl=tls,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DatabaseError(
                "database.connection-failed", "MySQL connection could not be opened."
            ) from None
        return cls(pool, command_timeout=config.command_timeout, close_timeout=config.close_timeout)

    async def execute(self, statement: str, parameters: SQLParameters = ()) -> SQLExecution:
        """Execute one bounded, parameterized MySQL statement."""
        """Execute one parameterized statement and report affected rows and generated id."""
        query, values = _prepare(statement, parameters)
        self._ensure_open()
        try:
            async with (
                self._pool.acquire() as connection,
                connection.cursor(SSDictCursor) as cursor,
            ):
                await asyncio.wait_for(cursor.execute(query, values), self._command_timeout)
                return SQLExecution(cursor.rowcount, cursor.lastrowid)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    async def fetch_one(self, statement: str, parameters: SQLParameters = ()) -> SQLRow | None:
        """Fetch one MySQL row or return ``None`` when there is no result."""
        """Fetch one detached result row from a server-side cursor."""
        query, values = _prepare(statement, parameters)
        self._ensure_open()
        try:
            async with (
                self._pool.acquire() as connection,
                connection.cursor(SSDictCursor) as cursor,
            ):
                await asyncio.wait_for(cursor.execute(query, values), self._command_timeout)
                row = await asyncio.wait_for(cursor.fetchone(), self._command_timeout)
                return None if row is None else _row(cursor.description, row)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    async def fetch_all(
        self, statement: str, parameters: SQLParameters = (), *, limit: int = 1_000
    ) -> tuple[SQLRow, ...]:
        """Fetch no more than ``limit`` rows and reject an oversized result."""
        """Read at most one row over the requested bound and reject overflow."""
        _validate_limit(limit)
        query, values = _prepare(statement, parameters)
        self._ensure_open()
        try:
            async with (
                self._pool.acquire() as connection,
                connection.cursor(SSDictCursor) as cursor,
            ):
                await asyncio.wait_for(cursor.execute(query, values), self._command_timeout)
                records = await asyncio.wait_for(cursor.fetchmany(limit + 1), self._command_timeout)
                if len(records) > limit:
                    raise DatabaseError(
                        "database.result-limit", "SQL result exceeds the configured row limit."
                    )
                return tuple(_row(cursor.description, record) for record in records)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLTransaction]:
        """Pin one pool connection; commit on success, rollback on any failure or cancellation."""
        self._ensure_open()
        acquire = self._pool.acquire()
        try:
            connection = await acquire.__aenter__()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DatabaseError(
                "database.acquire-failed", "MySQL connection could not be acquired."
            ) from None
        transaction = _MySQLTransaction(self, connection)
        try:
            try:
                await connection.begin()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DatabaseError(
                    "database.transaction-start-failed", "MySQL transaction could not start."
                ) from None
            transaction._active = True
            yield transaction
        except BaseException as original:
            if transaction._active:
                rollback = asyncio.create_task(connection.rollback())
                while not rollback.done():
                    try:
                        await asyncio.shield(rollback)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if rollback.done() and not rollback.cancelled():
                    try:
                        rollback.result()
                    except Exception:
                        if not isinstance(original, asyncio.CancelledError):
                            raise DatabaseError(
                                "database.rollback-failed", "MySQL transaction rollback failed."
                            ) from None
                else:
                    if not isinstance(original, asyncio.CancelledError):
                        raise DatabaseError(
                            "database.rollback-failed", "MySQL transaction rollback failed."
                        ) from None
            raise
        else:
            try:
                await connection.commit()
            except asyncio.CancelledError:
                rollback = asyncio.create_task(connection.rollback())
                while not rollback.done():
                    try:
                        await asyncio.shield(rollback)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                raise
            except Exception as exc:
                if _is_conflict(exc):
                    raise DatabaseError(
                        "database.constraint-conflict",
                        "Database uniqueness constraint was violated.",
                    ) from None
                raise DatabaseError(
                    "database.commit-failed", "MySQL transaction commit failed."
                ) from None
        finally:
            transaction._active = False
            try:
                await acquire.__aexit__(None, None, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DatabaseError(
                    "database.release-failed", "MySQL connection could not be released."
                ) from None

    async def aclose(self) -> None:
        """Drain and close the pool; force termination when bounded shutdown fails."""
        self._ensure_loop()
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._pool.close()
                await asyncio.wait_for(self._pool.wait_closed(), timeout=self._close_timeout)
            except asyncio.CancelledError:
                self._pool.terminate()
                raise
            except TimeoutError:
                self._pool.terminate()
                raise DatabaseError(
                    "database.close-timeout", "MySQL pool did not drain before shutdown."
                ) from None
            except Exception:
                self._pool.terminate()
                raise DatabaseError(
                    "database.close-failed", "MySQL pool could not close cleanly."
                ) from None

    def _ensure_open(self) -> None:
        self._ensure_loop()
        if self._closed:
            raise DatabaseError("database.closed", "The database is closed.")

    def _ensure_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise DatabaseError(
                "database.loop-mismatch", "MySQL resource belongs to another event loop."
            )


class _MySQLTransaction:
    """Task-bound operations on a connection owned by one transaction context."""

    def __init__(self, database: MySQLDatabase, connection: _Connection) -> None:
        self._database = database
        self._connection = connection
        self._owner = asyncio.current_task()
        self._active = False

    async def execute(self, statement: str, parameters: SQLParameters = ()) -> SQLExecution:
        """Execute one bounded statement using the transaction's pinned connection."""
        self._check()
        query, values = _prepare(statement, parameters)
        try:
            async with self._connection.cursor(SSDictCursor) as cursor:
                await asyncio.wait_for(
                    cursor.execute(query, values), self._database._command_timeout
                )
                return SQLExecution(cursor.rowcount, cursor.lastrowid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    async def fetch_one(self, statement: str, parameters: SQLParameters = ()) -> SQLRow | None:
        """Fetch one row inside the active transaction, if present."""
        self._check()
        query, values = _prepare(statement, parameters)
        try:
            async with self._connection.cursor(SSDictCursor) as cursor:
                await asyncio.wait_for(
                    cursor.execute(query, values), self._database._command_timeout
                )
                row = await asyncio.wait_for(cursor.fetchone(), self._database._command_timeout)
                return None if row is None else _row(cursor.description, row)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    async def fetch_all(
        self, statement: str, parameters: SQLParameters = (), *, limit: int = 1_000
    ) -> tuple[SQLRow, ...]:
        """Fetch a bounded result inside the active transaction."""
        self._check()
        _validate_limit(limit)
        query, values = _prepare(statement, parameters)
        try:
            async with self._connection.cursor(SSDictCursor) as cursor:
                await asyncio.wait_for(
                    cursor.execute(query, values), self._database._command_timeout
                )
                records = await asyncio.wait_for(
                    cursor.fetchmany(limit + 1), self._database._command_timeout
                )
                if len(records) > limit:
                    raise DatabaseError(
                        "database.result-limit", "SQL result exceeds the configured row limit."
                    )
                return tuple(_row(cursor.description, record) for record in records)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception as exc:
            raise _mysql_error(exc) from None

    def _check(self) -> None:
        self._database._ensure_open()
        if not self._active or asyncio.current_task() is not self._owner:
            raise DatabaseError(
                "database.transaction-scope", "Transaction is inactive or used by another task."
            )


def _prepare(statement: str, parameters: SQLParameters) -> tuple[str, tuple[SQLValue, ...]]:
    """Validate SQL and rewrite numbered parameters outside string/comment regions."""
    if not isinstance(statement, str) or not statement.strip():
        raise ValueError("SQL statement must be a non-empty string.")
    if len(statement.encode("utf-8")) > _MAX_SQL_BYTES:
        raise ValueError("SQL statement exceeds the 1,000,000-byte limit.")
    if not isinstance(parameters, Sequence) or isinstance(parameters, (str, bytes, bytearray)):
        raise TypeError("SQL parameters must be a positional sequence of scalar values.")
    supplied = tuple(parameters)
    if len(supplied) > 10_000:
        raise ValueError("SQL parameter count cannot exceed 10,000.")
    for value in supplied:
        if value is not None and not isinstance(
            value, (bool, int, float, str, bytes, date, datetime, time, Decimal, UUID)
        ):
            raise TypeError("SQL parameters must use supported scalar values.")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("SQL floating-point parameters must be finite.")
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("SQL decimal parameters must be finite.")
    output: list[str] = []
    used: list[int] = []
    i = 0
    quote: str | None = None
    while i < len(statement):
        char = statement[i]
        if quote is not None:
            output.append(char)
            if char == "\\" and i + 1 < len(statement):
                i += 1
                output.append(statement[i])
            elif char == quote:
                if i + 1 < len(statement) and statement[i + 1] == quote:
                    i += 1
                    output.append(statement[i])
                else:
                    quote = None
        elif char in ("'", '"', "`"):
            quote = char
            output.append(char)
        elif char == "#" or (
            char == "-"
            and statement[i : i + 2] == "--"
            and (i + 2 == len(statement) or statement[i + 2].isspace())
        ):
            end = statement.find("\n", i)
            end = len(statement) if end < 0 else end
            output.append(statement[i:end])
            i = end - 1
        elif char == "/" and statement[i : i + 2] == "/*":
            end = statement.find("*/", i + 2)
            if end < 0:
                output.append(statement[i:])
                break
            output.append(statement[i : end + 2])
            i = end + 1
        elif char == "$" and i + 1 < len(statement) and statement[i + 1].isdigit():
            end = i + 1
            while end < len(statement) and statement[end].isdigit():
                end += 1
            index = int(statement[i + 1 : end])
            if not 1 <= index <= len(supplied):
                raise ValueError("SQL placeholder index is outside the supplied parameters.")
            used.append(index)
            output.append("%s")
            i = end - 1
        else:
            output.append(char)
        i += 1
    if set(used) != set(range(1, len(supplied) + 1)) and (used or supplied):
        raise ValueError("Every supplied SQL parameter must have a numbered placeholder.")
    return "".join(output), tuple(supplied[index - 1] for index in used)


def _row(
    description: Sequence[tuple[Any, ...]] | None,
    values: Mapping[str, SQLValue] | Sequence[SQLValue],
) -> SQLRow:
    if description is None:
        raise DatabaseError("database.result-invalid", "MySQL returned malformed result metadata.")
    columns = tuple(str(column[0]) for column in description)
    if isinstance(values, Mapping):
        row_values = tuple(values.get(column) for column in columns)
    elif isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        if len(values) != len(columns):
            raise DatabaseError("database.result-invalid", "MySQL returned a malformed row.")
        row_values = tuple(values)
    else:
        raise DatabaseError("database.result-invalid", "MySQL returned a malformed row.")
    return SQLRow(columns, row_values)


def _is_conflict(error: BaseException) -> bool:
    return getattr(error, "args", (None,))[0] in (1022, 1062, 1169)


def _mysql_error(error: BaseException) -> DatabaseError:
    if _is_conflict(error):
        return DatabaseError(
            "database.constraint-conflict", "Database uniqueness constraint was violated."
        )
    return DatabaseError("database.operation-failed", "MySQL operation failed.")


def _validate_limit(limit: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _MAX_ROWS:
        raise ValueError("SQL result limit must be between 1 and 10,000 rows.")


__all__ = ["MySQLDatabase"]
