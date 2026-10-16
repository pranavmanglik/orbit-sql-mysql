# Orbit SQL MySQL: operations

`orbit-sql-mysql` implements Orbit's SQL contract with the asyncmy connection pool. Install it with
the Core runtime and SQL capability:

```bash
python -m pip install orbit-data orbit-sql orbit-sql-mysql
# Add the Core lifecycle plugin only when the application uses it:
python -m pip install 'orbit-sql-mysql[core]'
```

Configure `MySQLConfig` with `host`, `port`, `username`, `password`, and `database`. Pool size,
connection timeout, command timeout, close timeout, connection recycle interval, and TLS CA path
are explicit fields; see the [configuration model](../../src/orbit_sql_mysql/config.py) for their
bounds and defaults. Prefer a secret manager for the password. `ssl` defaults to true, and
`ssl_ca` adds a private CA while retaining certificate and hostname verification. Use `ssl=False`
only with an isolated local development server.

## Pool ownership and shutdown

`MySQLPlugin` registers a lazy `SQL_DATABASE_KEY` resource. Core owns its lifecycle and closes the
pool at application shutdown. With `MySQLAdapter` and the registry directly, explicitly call
`aclose()` when the application exits. Each database instance belongs to the event loop that opened
it; do not transfer it across loops.

Pool shutdown first prevents new acquisitions and waits up to `close_timeout` for checked-out
connections. On timeout or driver failure it terminates the pool and returns a sanitized
`DatabaseError`. Cancellation during shutdown also terminates the pool before propagating
cancellation.

## Statements and transaction outcomes

Standalone writes use driver autocommit. Use `async with database.transaction()` to make several
statements atomic. The transaction pins one pooled connection, commits on normal exit, and rolls
back on errors and cancellation. MySQL DDL may implicitly commit according to server rules, even
inside an explicit transaction.

Configure at most 32 weighted `MySQLReadReplica` entries with separate reader credentials.
Pool bounds apply to the combined maximum across primary and replicas. Reads use primary by default;
pass `read_preference=SQLReadPreference.REPLICA` for eventual-consistency reads. The adapter enforces
the server's read-only transaction mode and fails closed if no replica is configured. MySQL allows
temporary-table modifications inside read-only transactions, so keep replica credentials restricted
to read-only access and configure the server accordingly. Replica
failures do not cause query replay on primary. Monitor replication lag and use primary reads for
read-after-write paths.

The adapter requires asyncmy 0.2.12 or later because that release closes a pooled connection when
a query is cancelled during result reading. This prevents an interrupted result from being reused
by a later caller. The adapter propagates cancellation; do not catch cancellation and continue
using the same request's database operation as if it completed successfully.

Command timeout and task cancellation can occur after the server accepted a write. The application
may not know whether that write committed. Reconcile before retrying non-idempotent operations, or
use application-level idempotency keys and database constraints.

## Live verification

Unit tests use a fake pool and do not validate server versions, TLS behavior, permissions, or
durability. `tests/test_live_mysql.py` is opt-in and must target a disposable test database; it
creates and removes a uniquely named table. For a TLS-enabled test target, set
`ORBIT_MYSQL_DSN` and `ORBIT_MYSQL_SSL_CA` as described in the package README. Never point the live
test at production data.
