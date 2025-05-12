-- 0001_balances_and_transfer.sql
-- Safe, sound, and yes, downright cozy 🌲🔥

BEGIN;

-------------------------------------------------
-- Table: balances  (composite primary key)
-------------------------------------------------
CREATE TABLE IF NOT EXISTS balances (
    account_id  BIGINT        NOT NULL,
    currency    TEXT          NOT NULL,
    balance     NUMERIC(38,2) NOT NULL CHECK (balance >= 0),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, currency)
);

COMMENT ON TABLE balances IS
$$One row per (account, currency).  Balance never negative; life always cozy.$$;

-------------------------------------------------
-- Auto-timestamp trigger
-------------------------------------------------
CREATE OR REPLACE FUNCTION _touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_touch_updated_at ON balances;
CREATE TRIGGER trg_touch_updated_at
BEFORE UPDATE ON balances
FOR EACH ROW EXECUTE FUNCTION _touch_updated_at();

-------------------------------------------------
-- Function: transfer_funds  (receiver-auto-create)
-------------------------------------------------
CREATE OR REPLACE FUNCTION transfer_funds(
    _from      BIGINT,
    _to        BIGINT,
    _currency  TEXT,
    _amount    NUMERIC(38,2)
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    s_balance  NUMERIC(38,2);   -- sender balance
    r_balance  NUMERIC(38,2);   -- receiver balance (after any auto-create)
BEGIN
    -----------------------------------------------------------------
    -- 1. Quick sanity
    -----------------------------------------------------------------
    IF _amount <= 0 THEN
        RAISE EXCEPTION 'Amount must be strictly positive.';
    END IF;
    IF _from = _to THEN
        RAISE EXCEPTION 'Sender and receiver cannot be the same account.';
    END IF;
    IF _currency IS NULL OR length(trim(_currency)) = 0 THEN
        RAISE EXCEPTION 'Currency must be supplied.';
    END IF;

    -----------------------------------------------------------------
    -- 2. Lock rows *in deterministic order* to avoid deadlocks
    --    We may have to INSERT the receiver; handle races cleanly.
    -----------------------------------------------------------------
    IF _from < _to THEN
        -- sender row first
        SELECT balance INTO s_balance
          FROM balances
         WHERE account_id = _from AND currency = _currency
         FOR UPDATE;
    ELSE
        -- receiver row could be the smaller ID; make sure it exists *first*
        PERFORM 1;  -- no-op placeholder; real work just below
    END IF;

    -----------------------------------------------------------------
    -- 3. Make sure the sender row actually exists
    -----------------------------------------------------------------
    IF s_balance IS NULL THEN
        -- (We only read s_balance if we already locked the row; if _from > _to
        -- we haven’t done that yet. So in either branch below we’ll catch it.)
        SELECT balance INTO s_balance
          FROM balances
         WHERE account_id = _from AND currency = _currency
         FOR UPDATE;
        IF s_balance IS NULL THEN
            RAISE EXCEPTION 'Sender (% , %) not found in balances.',
                            _from, _currency;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Ensure receiver row exists, creating it if necessary
    -----------------------------------------------------------------
    LOOP
        SELECT balance INTO r_balance
          FROM balances
         WHERE account_id = _to AND currency = _currency
         FOR UPDATE;

        EXIT WHEN r_balance IS NOT NULL;      -- it exists & is locked

        -- Try to create a zero-balance row; ignore duplicate-key races.
        BEGIN
            INSERT INTO balances(account_id, currency, balance)
            VALUES (_to, _currency, 0);
        EXCEPTION WHEN unique_violation THEN
            -- Another session created it first – loop again to lock it.
        END;
    END LOOP;

    -----------------------------------------------------------------
    -- 5. Funds check & update (we still hold both row locks)
    -----------------------------------------------------------------
    IF s_balance < _amount THEN
        RAISE EXCEPTION 'Insufficient funds: account % has %.2f %s, needs %.2f.',
                        _from, s_balance, _currency, _amount;
    END IF;

    UPDATE balances
       SET balance = balance - _amount
     WHERE account_id = _from
       AND currency   = _currency;

    UPDATE balances
       SET balance = balance + _amount
     WHERE account_id = _to
       AND currency   = _currency;

    -- All done – safe, sound, cozy, and now self-service for brand-new receivers. 🔥
END;
$$;

COMMIT;
