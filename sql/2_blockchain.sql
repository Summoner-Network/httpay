-- 0002_blocks_and_append.sql
-- A snug little block-chain table, minus the chain 🪵🔥

BEGIN;

-------------------------------------------------
-- Table: blocks
-------------------------------------------------
CREATE TABLE IF NOT EXISTS blocks (
    id          BIGINT        NOT NULL,
    data        BYTEA         NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)                       -- unique & indexed
);

COMMENT ON TABLE blocks IS
$$Append-only binary records keyed by a monotonically-increasing BIGINT id.
No sequences, no holes (unless a concurrent insert grabs an id first),
and a timestamp so you know when each log was born.$$;

-------------------------------------------------
-- Index on created_at  (helps time-ordered queries / pruning)
-------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_blocks_created_at
    ON blocks (created_at);
    
-------------------------------------------------
-- Function: append_block
-------------------------------------------------
CREATE OR REPLACE FUNCTION append_block(_data BYTEA)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    next_id  BIGINT;
BEGIN
    IF _data IS NULL THEN
        RAISE EXCEPTION 'Data must not be NULL.';
    END IF;

    LOOP
        -----------------------------------------------------------------
        -- 1. Propose the next id
        -----------------------------------------------------------------
        SELECT COALESCE(MAX(id), 0) + 1
          INTO next_id
          FROM blocks;

        -----------------------------------------------------------------
        -- 2. Try to grab it; if someone else wins the race,
        --    the unique_violation rolls us back to LOOP.
        -----------------------------------------------------------------
        BEGIN
            INSERT INTO blocks(id, data)
            VALUES (next_id, _data);

            -- Success: return the new id to the caller.
            RETURN next_id;

        EXCEPTION WHEN unique_violation THEN
            -- Another session inserted the same id first – retry.
            -- (Optimistic concurrency in action.)
        END;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION append_block IS
$$Append a new block, choosing MAX(id)+1 without using a sequence.
Retries until the insert succeeds, guaranteeing a gap-free, strictly
increasing id series even under heavy concurrency.$$;

COMMIT;
