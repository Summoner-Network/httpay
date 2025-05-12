"""
Integration tests for httpay SQL migrations.

Works with either **psycopg2‑binary** (v2) *or* **psycopg[binary]** (v3).
If you’re on Python 3.13 where psycopg2 wheels aren’t published yet,
`pip install psycopg[binary]` and these tests will adapt automatically.

Run:
    pytest -v

Override DB connection via env vars:
    POSTGRES_DB=httpay
    POSTGRES_USER=httpay
    POSTGRES_PASSWORD=supersecret
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import List
from binascii import unhexlify

# ---------------------------------------------------------------------------
# Optional dual import: psycopg2 (v2) *or* psycopg (v3)
# ---------------------------------------------------------------------------
try:
    import psycopg2 as _pg
    from psycopg2 import errors as pg_errors  # type: ignore

    def _connect(**kw):  # noqa: D401
        """psycopg2 connector"""
        return _pg.connect(**kw)

except ModuleNotFoundError:
    import psycopg  # type: ignore[override]
    from psycopg import errors as pg_errors  # type: ignore

    def _connect(**kw):  # noqa: D401
        """psycopg3 connector (binary build)"""
        return psycopg.connect(**kw)

# ---------------------------------------------------------------------------
import pytest

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

DB_NAME = os.getenv("POSTGRES_DB", "httpay")
DB_USER = os.getenv("POSTGRES_USER", "httpay")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "supersecret")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", 5432))


def _get_conn():
    return _connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


@contextmanager
def db_cursor():
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT t;")
        yield cur
    finally:
        conn.rollback()
        cur.close()
        conn.close()


def _get_scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]

# ---------------------------------------------------------------------------
# transfer_funds tests
# ---------------------------------------------------------------------------

def test_transfer_basic():
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO balances(account_id, currency, balance)
            VALUES (1,'USD',100),(2,'USD',0);
            """
        )
        cur.execute("SELECT transfer_funds(1,2,'USD',30);")
        assert _get_scalar(cur,"SELECT balance FROM balances WHERE account_id=1 AND currency='USD';")==70
        assert _get_scalar(cur,"SELECT balance FROM balances WHERE account_id=2 AND currency='USD';")==30


def test_transfer_autocreate_receiver():
    with db_cursor() as cur:
        cur.execute("INSERT INTO balances VALUES (10,'EUR',50);")
        cur.execute("SELECT transfer_funds(10,11,'EUR',20);")
        assert _get_scalar(cur,"SELECT balance FROM balances WHERE account_id=10 AND currency='EUR';")==30
        assert _get_scalar(cur,"SELECT balance FROM balances WHERE account_id=11 AND currency='EUR';")==20


@pytest.mark.parametrize("args",[(7,7,'USD',1),(7,8,'USD',-5),(7,8,'USD',0),(7,8,None,1)])
def test_transfer_invalid(args):
    with db_cursor() as cur:
        cur.execute("INSERT INTO balances VALUES(7,'USD',10);")
        with pytest.raises(pg_errors.RaiseException):
            cur.execute("SELECT transfer_funds(%s,%s,%s,%s);",args)


def test_transfer_insufficient():
    with db_cursor() as cur:
        cur.execute("INSERT INTO balances VALUES(20,'GBP',10),(21,'GBP',0);")
        with pytest.raises(pg_errors.RaiseException):
            cur.execute("SELECT transfer_funds(20,21,'GBP',20);")

# ---------------------------------------------------------------------------
# append_block tests
# ---------------------------------------------------------------------------

def test_append_block_monotonic():
    with db_cursor() as cur:
        first_id=_get_scalar(cur,"SELECT append_block(decode('deadbeef','hex'));")
        second_id=_get_scalar(cur,"SELECT append_block(decode('cafebabe','hex'));")
        assert second_id==first_id+1
        assert _get_scalar(cur,"SELECT COUNT(*) FROM blocks;")==2


def test_append_block_null():
    with db_cursor() as cur:
        with pytest.raises(pg_errors.RaiseException):
            cur.execute("SELECT append_block(NULL);")


def test_append_block_concurrency():
    num=10
    per=20
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id),0) FROM blocks;")
            base=cur.fetchone()[0]
    def worker(payload:bytes):
        c=_get_conn();c.autocommit=True
        with c.cursor() as cur:
            for _ in range(per):
                cur.execute("SELECT append_block(%s);",(payload,))
        c.close()
    ts=[threading.Thread(target=worker,args=(f"t{i}".encode(),)) for i in range(num)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM blocks WHERE id>%s;",(base,))
            added=cur.fetchone()[0]
            assert added==num*per
            cur.execute("SELECT MIN(id),MAX(id) FROM blocks WHERE id>%s;",(base,))
            lo,hi=cur.fetchone();assert hi-lo+1==added
            cur.execute("DELETE FROM blocks WHERE id>%s;",(base,));conn.commit()

## ACCOUNTS

def _hex(b: str) -> bytes:
    """Helper: turn a compact hex string into bytes for INSERTs/queries."""
    return unhexlify(b)


def test_account_key_basic():
    """Insert one key and make sure we can read it back."""
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO account_keys(account_id, scheme, public_key)
            VALUES (%s, %s, %s);
            """,
            (42, "ed25519", _hex("deadbeef" * 4)),
        )
        cur.execute(
            """
            SELECT scheme, encode(public_key, 'hex')
              FROM account_keys
             WHERE account_id = 42;
            """
        )
        scheme, pub_hex = cur.fetchone()
        assert scheme == "ed25519"
        assert pub_hex == ("deadbeef" * 4)


def test_account_key_multiple_keys_per_account():
    """Same account & scheme, different keys → both rows allowed."""
    with db_cursor() as cur:
        keys = ["aabbccdd" * 4, "ffeeddcc" * 4]
        for k in keys:
            cur.execute(
                """
                INSERT INTO account_keys(account_id, scheme, public_key)
                VALUES (99, 'ed25519', %s);
                """,
                (_hex(k),),
            )

        cur.execute(
            "SELECT COUNT(*) FROM account_keys WHERE account_id = 99 AND scheme = 'ed25519';"
        )
        assert cur.fetchone()[0] == 2


def test_account_key_multiple_schemes_same_key():
    """Same key bytes with two schemes → both rows allowed."""
    with db_cursor() as cur:
        pk = _hex("11223344" * 4)
        cur.execute(
            """
            INSERT INTO account_keys(account_id, scheme, public_key)
            VALUES (123, 'ed25519', %s),
                   (123, 'secp256k1', %s);
            """,
            (pk, pk),
        )

        cur.execute(
            """
            SELECT ARRAY_AGG(scheme ORDER BY scheme)
              FROM account_keys
             WHERE account_id = 123;
            """
        )
        schemes = cur.fetchone()[0]
        assert schemes == ["ed25519", "secp256k1"]


def test_account_key_duplicate_rejected():
    """Exact duplicate (account, scheme, key) should raise unique_violation."""
    with db_cursor() as cur:
        pk = _hex("cafebabe" * 4)
        cur.execute(
            """
            INSERT INTO account_keys(account_id, scheme, public_key)
            VALUES (777, 'ed25519', %s);
            """,
            (pk,),
        )
        with pytest.raises(pg_errors.UniqueViolation):
            cur.execute(
                """
                INSERT INTO account_keys(account_id, scheme, public_key)
                VALUES (777, 'ed25519', %s);
                """,
                (pk,),
            )


@pytest.mark.parametrize(
    "columns,values",
    [
        ("(account_id, scheme, public_key)", (888, None, _hex("ab" * 16))),  # NULL scheme
        ("(account_id, scheme, public_key)", (888, "ed25519", None)),        # NULL key
        ("(account_id, scheme)", (888, "ed25519")),                          # missing key
    ],
)
def test_account_key_not_null_and_check(columns, values):
    """NULLs or missing mandatory fields should fail."""
    with db_cursor() as cur:
        with pytest.raises((pg_errors.NotNullViolation, pg_errors.CheckViolation)):
            cur.execute(
                f"INSERT INTO account_keys {columns} VALUES %s;",
                (values,),
            )