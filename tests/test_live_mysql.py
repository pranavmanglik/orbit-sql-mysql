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
"""Opt-in behavior checks against a real MySQL service."""

from __future__ import annotations

import os
from contextlib import suppress
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytest

from orbit_sql_mysql import MySQLConfig, MySQLDatabase


@pytest.mark.asyncio
async def test_live_mysql_queries_transaction_rollback_and_cleanup() -> None:
    """Verify parameter binding, bounded reads, rollback, and pool close against MySQL."""
    dsn = os.environ.get("ORBIT_MYSQL_DSN")
    if not dsn:
        pytest.skip("Set ORBIT_MYSQL_DSN to run the live MySQL integration check.")
    parsed = urlsplit(dsn)
    if parsed.scheme not in {"mysql", "mariadb"} or not parsed.hostname or not parsed.path:
        raise ValueError("ORBIT_MYSQL_DSN must use mysql:// or mariadb:// and include a database.")
    local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if not local and os.environ.get("ORBIT_MYSQL_ALLOW_REMOTE_TESTS") != "1":
        pytest.skip("Set ORBIT_MYSQL_ALLOW_REMOTE_TESTS=1 to target a non-local test database.")
    if parsed.username is None or parsed.password is None:
        raise ValueError("ORBIT_MYSQL_DSN must include a test username and password.")
    ssl_ca = os.environ.get("ORBIT_MYSQL_SSL_CA")
    if local and not ssl_ca:
        pytest.skip("Set ORBIT_MYSQL_SSL_CA to a CA file for the local TLS service.")

    database = await MySQLDatabase.open(
        MySQLConfig(
            host=parsed.hostname,
            port=parsed.port or 3306,
            username=unquote(parsed.username),
            password=unquote(parsed.password),
            database=parsed.path.removeprefix("/"),
            min_pool_size=1,
            max_pool_size=2,
            ssl=True,
            ssl_ca=ssl_ca,
        )
    )
    table = f"orbit_live_{uuid4().hex}"
    quoted_table = f"`{table}`"
    try:
        await database.execute(
            f"CREATE TABLE {quoted_table} (id INTEGER PRIMARY KEY, value VARCHAR(255) NOT NULL)"
        )
        inserted = await database.execute(
            f"INSERT INTO {quoted_table} (id, value) VALUES ($1, $2)", (1, "committed")
        )
        assert inserted.rowcount == 1

        with pytest.raises(RuntimeError, match="rollback sentinel"):
            async with database.transaction() as transaction:
                await transaction.execute(
                    f"INSERT INTO {quoted_table} (id, value) VALUES ($1, $2)",
                    (2, "rolled back"),
                )
                raise RuntimeError("rollback sentinel")

        row = await database.fetch_one(
            f"SELECT id, value FROM {quoted_table} WHERE id = $1", (1,)
        )
        rows = await database.fetch_all(
            f"SELECT id, value FROM {quoted_table} ORDER BY id", limit=10
        )
        rolled_back = await database.fetch_one(
            f"SELECT id FROM {quoted_table} WHERE id = $1", (2,)
        )
        assert row is not None and dict(row) == {"id": 1, "value": "committed"}
        assert tuple(dict(result) for result in rows) == ({"id": 1, "value": "committed"},)
        assert rolled_back is None
    finally:
        with suppress(Exception):
            await database.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        await database.aclose()
