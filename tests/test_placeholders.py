"""Focused SQL parameter contract and sanitization tests."""

from decimal import Decimal

import pytest

from orbit_sql_mysql.database import _prepare


def test_rewrites_numbered_parameters_and_repeats_values_safely() -> None:
    query, values = _prepare("SELECT $1, '$2', `$3`, $1 /* $4 */", (Decimal("1.5"),))
    assert query == "SELECT %s, '$2', `$3`, %s /* $4 */"
    assert values == (Decimal("1.5"), Decimal("1.5"))


def test_rejects_missing_and_unused_parameters() -> None:
    with pytest.raises(ValueError, match="outside"):
        _prepare("SELECT $2", (1,))
    with pytest.raises(ValueError, match="Every supplied"):
        _prepare("SELECT $1", (1, 2))
    with pytest.raises(ValueError, match="Every supplied"):
        _prepare("SELECT 1", (1,))


def test_honors_mysql_hash_and_dash_comments() -> None:
    query, values = _prepare("SELECT $1 # $2\n, $2 -- $1\n", (1, 2))
    assert query == "SELECT %s # $2\n, %s -- $1\n"
    assert values == (1, 2)
