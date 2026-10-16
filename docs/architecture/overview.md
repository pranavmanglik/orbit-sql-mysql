# Orbit SQL MySQL: architecture and boundaries

## Responsibility

`orbit-sql-mysql` implements Orbit's provider-neutral SQL contract with the asynchronous
[`asyncmy`](https://github.com/long2ice/asyncmy) driver. It owns MySQL settings, TLS, its bounded
connection pool, transaction scopes and provider error mapping. It does not bundle a framework,
ORM, migration engine, or SQL abstraction.
The adapter can be used with `orbit-sql` without Core. `MySQLPlugin` is an optional Core lifecycle
integration imported from `orbit_sql_mysql.plugin` and installed with the `[core]` extra.

```bash
pip install orbit-data orbit-sql orbit-sql-mysql
# Add the Core lifecycle plugin only when the application uses it:
pip install 'orbit-sql-mysql[core]'
```

TLS peer verification is enabled by default. Set `ssl_ca` when the server uses a private CA; the
adapter still verifies the certificate chain and hostname. Credentials are represented as secret
values in configuration and never included in adapter errors. Configure a least-privilege MySQL
account and provide its password through a protected secret source. Set `ssl=False` only for a
trusted local development service. Pool size, connect/command/close bounds, and idle connection
recycling are explicit configuration values. The Core plugin opens the pool on first resolution and
closes it under application lifecycle ownership.

Orbit SQL query parameters use numbered `$1`, `$2`, … placeholders. This adapter converts those
placeholders to the driver's bound `%s` form while ignoring strings, quoted identifiers and SQL
comments; values are always passed separately. Result rows are detached and bounded. Explicit
transactions pin one pool connection, commit on successful exit and roll back on exceptions or
cancellation. MySQL DDL may have implicit-commit behavior defined by the server; do not assume it is
fully transactional. This package does not claim distributed transactions or cross-provider SQL
parity.

Primary reads are the default. An explicit `SQLReadPreference.REPLICA` request selects a configured
weighted replica using separate reader credentials and starts a `START TRANSACTION READ ONLY`
transaction. MySQL still permits modifications to temporary tables in read-only mode, so restricted
reader grants and read-only replica server configuration remain part of the security boundary.
Missing replicas and replica errors fail closed; operations are not replayed on primary. Use primary
reads where read-after-write consistency matters. See the official
[MySQL transaction documentation](https://dev.mysql.com/doc/refman/8.4/en/commit.html).

The adapter requires `asyncmy` 0.2.12 or newer. That driver closes a pooled connection when a
query is cancelled during result reading, preventing stale results from reaching the next caller.
This adapter propagates cancellation and relies on the driver's connection-discard behavior.

```python
from orbit import Application, ApplicationConfig
from orbit_sql_mysql import MySQLConfig
from orbit_sql_mysql.plugin import MySQLPlugin

app = Application(ApplicationConfig(name="orders"))
app.plugins.register(
    MySQLPlugin(
        MySQLConfig(
            host="127.0.0.1",
            username="orders_app",
            password="loaded-from-a-secret-manager",
            database="orders",
            ssl=False,  # local development only
        )
    )
)
```

The default tests use a fake asyncmy pool. An opt-in live check covers parameter binding, bounded
reads, transaction rollback, and pool cleanup against a disposable MySQL database. It creates and
removes only a uniquely named table. Remote targets are skipped unless explicitly enabled; TLS
remains on when testing a remote endpoint. Release validation must still include supported MySQL
versions, TLS verification, cancellation behavior and hosted CI wheel builds for the asyncmy Cython
extension. This package is in alpha and supports Python 3.11 through 3.14; the package suite passes
on each version (the live-service test skips unless a DSN is supplied). The live check passed on
Python 3.11 against TLS-enabled MySQL 8.4.11 with asyncmy 0.2.15; it does not validate the other
supported Python or MySQL versions.

For a disposable local TLS-enabled MySQL service, set the DSN and CA file, then run only the live
test:

```bash
ORBIT_MYSQL_DSN='mysql://orbit_test:orbit_test@127.0.0.1:53306/orbit_test' \
ORBIT_MYSQL_SSL_CA='/path/to/test-ca.pem' \
  pytest -q tests/test_live_mysql.py
```

Licensed under Apache-2.0.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `orbit-sql>=0.1.0a1,<0.2`
- `asyncmy>=0.2.12,<0.3`
- `pydantic>=2.8,<3`
- Optional `core` extra: `orbit-core>=0.1.0a1,<0.2` for `MySQLPlugin`.
- Development dependencies: Core for plugin tests, plus pytest, pytest-asyncio, Ruff, and mypy.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_sql_mysql/__init__.py`
- `src/orbit_sql_mysql/adapter.py`
- `src/orbit_sql_mysql/config.py`
- `src/orbit_sql_mysql/database.py`
- `src/orbit_sql_mysql/plugin.py`

## Public contract and scope

`MySQLDatabase` implements `SQLDatabase`: standalone `execute()` uses pool autocommit, fetches
return detached bounded `SQLRow` values, and `transaction()` pins a connection until commit or
rollback completes. Cursor and pool acquisition scopes return resources through async context
managers. Driver failures are translated to stable `DatabaseError` codes without exposing raw
server messages.

Cancellation is propagated to callers. asyncmy 0.2.12 and later discard connections interrupted
while reading results, so the pool cannot reuse their stale protocol state. The adapter's minimum
dependency version encodes this safety requirement. A cancelled command or commit may still have
reached the server; callers must treat write outcome as potentially unknown.

## Boundary rules

The host application owns request deadlines, rate limits, server-side database permissions, and
the policy for reconciling uncertain write outcomes. This package owns pool resources and closes
them through Core's resource lifecycle when used with `MySQLPlugin`. It does not provide migrations,
distributed transactions, encryption at rest, or automatic retries.
