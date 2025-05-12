-- 0003_account_keys.sql
-- Cozy cryptography corner: mapping accounts ⇄ public keys 🔐🔥

BEGIN;

-------------------------------------------------
-- Table: account_keys
-------------------------------------------------
CREATE TABLE IF NOT EXISTS account_keys (
    account_id  BIGINT      NOT NULL,         -- who owns the key
    scheme      TEXT        NOT NULL,         -- e.g. 'ed25519', 'secp256k1'
    public_key  BYTEA       NOT NULL CHECK (octet_length(public_key) > 0),
    added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- one row per (account, scheme, key) — duplicates? not on our watch
    PRIMARY KEY (account_id, scheme, public_key)
);

COMMENT ON TABLE account_keys IS
$$An account may register many (scheme, public-key) combos; each combo is
recorded once.  Great for hot-swapping keys or supporting multiple crypto
algorithms without fuss.$$;

-------------------------------------------------
-- Dedicated indexes for fast look-ups in any direction
-------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_account_keys_account_id
    ON account_keys (account_id);

CREATE INDEX IF NOT EXISTS idx_account_keys_public_key
    ON account_keys (public_key);

CREATE INDEX IF NOT EXISTS idx_account_keys_scheme
    ON account_keys (scheme);

COMMIT;
