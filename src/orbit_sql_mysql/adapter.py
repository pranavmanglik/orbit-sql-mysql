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
"""MySQL implementation of Orbit SQL's provider adapter contract."""

from orbit_sql import SQLAdapter
from pydantic import BaseModel

from orbit_sql_mysql.config import MySQLConfig
from orbit_sql_mysql.database import MySQLDatabase


class MySQLAdapter:
    """Open a pooled MySQL SQLDatabase through explicit provider selection."""

    @property
    def name(self) -> str:
        """Return the stable SQL registry key."""
        return "mysql"

    @property
    def configuration_model(self) -> type[BaseModel]:
        """Return the validated secret-aware provider configuration type."""
        return MySQLConfig

    async def connect(self, configuration: BaseModel) -> MySQLDatabase:
        """Open the MySQL pool using the supplied validated settings."""
        if not isinstance(configuration, MySQLConfig):
            raise TypeError("MySQLAdapter requires MySQLConfig.")
        return await MySQLDatabase.open(configuration)


assert isinstance(MySQLAdapter(), SQLAdapter)

__all__ = ["MySQLAdapter"]
