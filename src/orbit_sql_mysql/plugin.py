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
"""Optional Core plugin wiring the MySQL provider under Core resource ownership."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from orbit import Application
from orbit.plugins import ControlAction, ControlResult, Plugin, PluginMetadata
from orbit_sql import SQL_DATABASE_KEY

from orbit_sql_mysql.config import MySQLConfig
from orbit_sql_mysql.database import MySQLDatabase


class MySQLPlugin(Plugin):
    """Register a lazy MySQL pool managed by the application's resource container."""

    metadata = PluginMetadata(
        name="orbit-sql-mysql",
        version="0.1.0a1",
        capabilities=frozenset({"database.sql.mysql"}),
    )

    def __init__(self, config: MySQLConfig) -> None:
        """Capture validated configuration without opening a network connection."""
        if not isinstance(config, MySQLConfig):
            raise TypeError("MySQLPlugin requires MySQLConfig.")
        self._config = config
        self.control_actions = (
            ControlAction(
                id="check-connection",
                title="Check MySQL database",
                description="Run a read-only SELECT 1 against a temporary bounded pool.",
                handler=self._check_connection,
            ),
        )

    def setup(self, application: Application) -> None:
        """Register a lazy SQL resource during plugin setup."""
        if not isinstance(application, Application):
            raise TypeError("MySQLPlugin requires an Orbit Application.")
        application.container.register_resource(SQL_DATABASE_KEY, lambda _: self._database())

    @asynccontextmanager
    async def _database(self) -> AsyncIterator[MySQLDatabase]:
        """Create the pool on first resource resolution and close it during app shutdown."""
        database = await MySQLDatabase.open(self._config)
        try:
            yield database
        finally:
            await database.aclose()

    async def _check_connection(self, _: Mapping[str, object]) -> ControlResult:
        """Run a read-only connectivity query and close all temporary pool resources."""
        async with self._database() as database:
            await database.fetch_one("SELECT 1")
        return ControlResult("MySQL database is reachable.")


__all__ = ["MySQLPlugin"]
