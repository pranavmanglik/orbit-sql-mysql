# Orbit SQL MySQL: security

The MySQL adapter opens a bounded asyncmy pool, binds query values, enables verified TLS by
default, and sanitizes provider errors. Database permissions, credential delivery, server policy,
backup protection, and operational monitoring remain deployment responsibilities.

## Credentials and TLS

Use a least-privilege MySQL account and provide its password through a secret manager or protected
deployment configuration. Do not commit DSNs or passwords. `MySQLConfig.password` uses Pydantic's
`SecretStr`, and adapter-generated errors do not include connection credentials. Application logs
must still avoid logging configuration values or raw asyncmy exceptions.

TLS is enabled by default using the platform trust store. Set `ssl_ca` to trust a private CA; the
adapter keeps certificate-chain and hostname verification enabled. `ssl=False` disables transport
encryption and is suitable only for trusted local development. Never disable verification to work
around a certificate deployment problem.

## Queries and data

Bind runtime values with Orbit's `$1`, `$2`, … placeholders. The adapter translates them to
asyncmy's bound-value form while skipping quoted strings, identifiers, and comments. Placeholders
cannot bind table names or other SQL syntax; use fixed identifiers or a strict allowlist. Do not
log bound values or rows containing confidential information.

Grant runtime users only required table and operation privileges. Keep migration/administrative
credentials separate where possible, restrict network access to the server, and protect backups
and replicas to the same standard as the primary database. This package provides no encryption at
rest, backup, retention, or account lifecycle facility.

## Cancellation and pooled connections

The minimum supported asyncmy version is 0.2.12 because its cancellation fix closes a connection
when a query is cancelled during result reading. Older supported versions could return an
interrupted connection to the pool with stale protocol results, risking cross-request data mix-up.
Keep the dependency floor when updating package constraints. This package propagates cancellation;
callers should not treat a cancelled write as proof that the server rolled it back.

For vulnerability reporting, follow the repository [security policy](../../SECURITY.md). Do not
publish exploitable details in public issues.
