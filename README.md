# Orbit SQL MySQL

`orbit-sql-mysql` implements Orbit's provider-neutral SQL contract with the asynchronous
[`asyncmy`](https://github.com/long2ice/asyncmy) driver. It owns MySQL settings, TLS, its bounded
connection pool, transaction scopes and provider error mapping. It does not bundle a framework,
ORM, migration engine, or SQL abstraction.
The adapter can be used with `orbit-sql` without Core. `MySQLPlugin` is an optional Core lifecycle
integration imported from `orbit_sql_mysql.plugin` and installed with the `[core]` extra.

The adapter requires `asyncmy` 0.2.12 or later. That driver release
[closes a pooled connection](https://github.com/long2ice/asyncmy/releases/tag/v0.2.12) when a query
is cancelled during result reading, preventing stale results from being reused by another caller.
The adapter propagates cancellation and relies on this driver behavior to keep the pool safe.

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

The Core plugin also contributes a read-only **Check MySQL database** action to Orbit CLI. It runs
SELECT 1 using a temporary pool and closes all primary and replica resources before returning;
connection strings and driver error messages are not displayed.

Orbit SQL query parameters use numbered `$1`, `$2`, … placeholders. This adapter converts those
placeholders to the driver's bound `%s` form while ignoring strings, quoted identifiers and SQL
comments; values are always passed separately. Result rows are detached and bounded. Explicit
transactions pin one pool connection, commit on successful exit and roll back on exceptions or
cancellation. MySQL DDL may have implicit-commit behavior defined by the server; do not assume it is
fully transactional. This package does not claim distributed transactions or cross-provider SQL
parity.

Reads use the primary pool by default. Configure weighted `MySQLReadReplica` endpoints with
separate least-privilege reader credentials and opt in per read with
`SQLReadPreference.REPLICA`; replica reads and transactions start with MySQL's server-enforced
`START TRANSACTION READ ONLY`. Replica selection uses weighted round-robin, and failures are
returned without replaying work on the primary. Replica lag is expected; use primary reads for
read-after-write paths. Pool sizes are bounded across all configured endpoints. MySQL permits
modifications to temporary tables in read-only transactions, so reader-account grants and replica
server configuration remain part of the security boundary. See the official
[MySQL transaction access-mode documentation](https://dev.mysql.com/doc/refman/8.4/en/commit.html).

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

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

