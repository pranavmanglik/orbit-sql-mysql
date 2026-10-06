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
"""Validated, secret-aware MySQL connection and pool settings."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictBool, model_validator


class MySQLConfig(BaseModel):
    """Bounded pool settings with TLS peer verification enabled by default."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=3306, ge=1, le=65535, strict=True)
    username: str = Field(min_length=1, max_length=128)
    password: SecretStr
    database: str = Field(min_length=1, max_length=64)
    min_pool_size: int = Field(default=1, ge=1, le=128, strict=True)
    max_pool_size: int = Field(default=10, ge=1, le=128, strict=True)
    connect_timeout: float = Field(default=10.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    command_timeout: float = Field(default=30.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    close_timeout: float = Field(default=10.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    pool_recycle_seconds: int = Field(default=1_800, ge=60, le=86_400, strict=True)
    ssl: StrictBool = True
    ssl_ca: str | None = Field(default=None, min_length=1, max_length=4_096)

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> MySQLConfig:
        """Require ordered pool bounds before handing settings to asyncmy."""
        if self.min_pool_size > self.max_pool_size:
            raise ValueError("min_pool_size must not exceed max_pool_size.")
        if self.ssl_ca is not None and not self.ssl:
            raise ValueError("ssl_ca requires TLS to be enabled.")
        return self


__all__ = ["MySQLConfig"]
