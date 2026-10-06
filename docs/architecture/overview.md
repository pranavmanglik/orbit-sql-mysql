# Orbit SQL MySQL: architecture and boundaries

## Responsibility

`orbit-sql-mysql` implements Orbit's provider-neutral SQL contract with the asynchronous
[`asyncmy`](https://github.com/long2ice/asyncmy) driver. It owns MySQL settings, TLS, its bounded
connection pool, transaction scopes and provider error mapping. It does not bundle a framework,
ORM, migration engine, or SQL abstraction.

```bash
pip install orbit-core orbit-data orbit-sql orbit-sql-mysql
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

```python
from orbit import Application, ApplicationConfig
from orbit_sql_mysql import MySQLConfig, MySQLPlugin

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
extension. This package is pre-alpha and supports Python 3.11 through 3.14; the package suite passes
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
- `orbit-core>=0.1.0a1,<0.2`
- `orbit-sql>=0.1.0a1,<0.2`
- `asyncmy>=0.2.11,<0.3`
- `pydantic>=2.8,<3`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_sql_mysql/__init__.py`
- `src/orbit_sql_mysql/adapter.py`
- `src/orbit_sql_mysql/config.py`
- `src/orbit_sql_mysql/database.py`
- `src/orbit_sql_mysql/plugin.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
