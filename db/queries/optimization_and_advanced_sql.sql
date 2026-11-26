/* =====================================================================
   PART 0  Setup (extension, helper function)
   PART 1  Baseline: EXPLAIN ANALYZE BEFORE any optimization
   PART 2  Indexes
   PART 3  Functions
   PART 4  Views
   PART 5  Materialized Views (+ refresh bookkeeping)
   PART 6  Triggers (audit, validation, stale-MV flag)
   PART 7  Procedures (optimized ETL, MV refresh)
   PART 8  Optimized queries: before vs after
   PART 9  Advanced query patterns (window / DISTINCT ON / ROLLUP)
   PART 10 Index health check
   PART 11 Demo: see the triggers and procedures work (rolled back, data untouched)
   ===================================================================== */

SET search_path TO food_waste, analytics, staging, public;


/* =====================================================================
   PART 0 - SETUP
   ===================================================================== */

-- pg_trgm makes  ILIKE '%text%'  searches indexable (needs CREATE privilege on the DB).
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- A "natural key" for source rows. (reference, url) can be NULL and very long, so we
-- hash it. IMMUTABLE is required so the function can be used inside an index.
CREATE OR REPLACE FUNCTION food_waste.fn_source_key(p_reference TEXT, p_url TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT md5(COALESCE(p_reference, '') || '|' || COALESCE(p_url, ''));
$$;


/* =====================================================================
   PART 1 - BASELINE PLANS (run this part BEFORE Part 2 to see "before")
   Look for: Seq Scan, Rows Removed by Filter, SubPlan loops, Execution Time.
   If you already created the indexes, run:  DROP INDEX ...  (see Part 2)  or
   temporarily force the old behaviour with:  SET enable_indexscan = off;
   SET enable_bitmapscan = off;  SET enable_indexonlyscan = off;
   ===================================================================== */

-- B1: average loss of one country per year (filter on a dimension column + join)
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.year, ROUND(AVG(o.loss_percentage), 3) AS avg_loss
FROM food_waste.observation o
JOIN food_waste.country c ON c.m49_code = o.m49_code
WHERE c.country_name = 'Egypt'
GROUP BY o.year
ORDER BY o.year;

-- B2: non-sargable year filter (casts the column, so no index could ever be used)
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), AVG(loss_percentage)
FROM food_waste.observation
WHERE year::TEXT LIKE '201%';

-- B3: correlated subquery - "records worse than their own commodity's average".
--     Plain EXPLAIN (no ANALYZE) on purpose: PostgreSQL re-runs the SubPlan once per row,
--     so executing it can take very long. Look at "SubPlan 1" and the huge cost.
EXPLAIN
SELECT o.record_id, o.cpc_code, o.loss_percentage
FROM food_waste.observation o
WHERE o.loss_percentage > (SELECT AVG(o2.loss_percentage)
                           FROM food_waste.observation o2
                           WHERE o2.cpc_code = o.cpc_code);

-- B4: substring search on long text
EXPLAIN (ANALYZE, BUFFERS)
SELECT source_id, reference
FROM food_waste.source
WHERE reference ILIKE '%study no. 17%';


/* =====================================================================
   PART 2 - INDEXES
   PostgreSQL indexes PRIMARY KEY / UNIQUE automatically but NOT foreign keys.
   Rule used here: one index per real access pattern, no redundant indexes.
   ===================================================================== */

-- 2.1 Source: integrity + fast lookup used by the ETL (replaces IS NOT DISTINCT FROM scans)
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_key
    ON food_waste.source (food_waste.fn_source_key(reference, url));

-- 2.2 Observation: composite + covering (INCLUDE) indexes -> Index Only Scans
--     "per country over time"  (also serves the FK observation.m49_code)
CREATE INDEX IF NOT EXISTS idx_obs_country_year
    ON food_waste.observation (m49_code, year) INCLUDE (loss_percentage);

--     "per commodity over time" (also serves the FK observation.cpc_code)
CREATE INDEX IF NOT EXISTS idx_obs_commodity_year
    ON food_waste.observation (cpc_code, year) INCLUDE (loss_percentage);

--     "global per year" / year-range filters
CREATE INDEX IF NOT EXISTS idx_obs_year
    ON food_waste.observation (year) INCLUDE (loss_percentage);

-- 2.3 FK indexes that no composite index above covers
CREATE INDEX IF NOT EXISTS idx_obs_source
    ON food_waste.observation (source_id);

--     Partial index: only rows that have a method (smaller, skips the NULLs)
CREATE INDEX IF NOT EXISTS idx_obs_method
    ON food_waste.observation (method_id) WHERE method_id IS NOT NULL;

-- 2.4 Partial index for the "hot" analytical question: the worst losses
CREATE INDEX IF NOT EXISTS idx_obs_high_loss
    ON food_waste.observation (loss_percentage DESC) WHERE loss_percentage >= 30;

-- 2.5 Junction table: the PK (record_id, activity_id) serves record -> activities.
--     The reverse direction (activity -> records) needs its own index.
CREATE INDEX IF NOT EXISTS idx_obsact_activity
    ON food_waste.observation_activity (activity_id, record_id);

-- 2.6 Trigram GIN index for ILIKE '%...%' searches on citations
CREATE INDEX IF NOT EXISTS idx_source_reference_trgm
    ON food_waste.source USING gin (reference gin_trgm_ops);

-- 2.7 Refresh planner statistics so the new indexes are costed correctly.
ANALYZE food_waste.observation;
ANALYZE food_waste.observation_activity;
ANALYZE food_waste.source;
ANALYZE food_waste.country;
ANALYZE food_waste.commodity;
-- Optional, run it ALONE (VACUUM cannot run inside a multi-statement transaction):
--   VACUUM (ANALYZE) food_waste.observation;
-- It refreshes the visibility map, which lets "Index Only Scan" skip the table pages.


/* =====================================================================
   PART 3 - FUNCTIONS
   ===================================================================== */

-- 3.1 Business rule in ONE place: classify a loss percentage.
--     (thresholds are a design choice - change them here and every view follows)
CREATE OR REPLACE FUNCTION analytics.fn_loss_category(p_pct NUMERIC)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
             WHEN p_pct IS NULL THEN NULL
             WHEN p_pct <  5    THEN 'Low'
             WHEN p_pct < 15    THEN 'Moderate'
             WHEN p_pct < 30    THEN 'High'
             ELSE                    'Critical'
           END;
$$;

-- 3.2 Parameterised report: loss trend of one country (optionally a year range).
--     Uses range predicates (sargable) so idx_obs_country_year is used.
CREATE OR REPLACE FUNCTION analytics.fn_country_loss_trend(
    p_country_name TEXT,
    p_from_year    INTEGER DEFAULT NULL,
    p_to_year      INTEGER DEFAULT NULL)
RETURNS TABLE (obs_year INTEGER, avg_loss NUMERIC, n_obs BIGINT)
LANGUAGE sql
STABLE
AS $$
    SELECT o.year,
           ROUND(AVG(o.loss_percentage), 3),
           COUNT(*)
    FROM food_waste.observation o
    JOIN food_waste.country c ON c.m49_code = o.m49_code
    WHERE c.country_name = p_country_name
      AND o.year >= COALESCE(p_from_year, 0)
      AND o.year <= COALESCE(p_to_year, 9999)
    GROUP BY o.year
    ORDER BY o.year;
$$;

-- 3.3 Top-N commodities by average loss in a given year.
CREATE OR REPLACE FUNCTION analytics.fn_top_commodities_by_loss(
    p_year  INTEGER,
    p_limit INTEGER DEFAULT 10)
RETURNS TABLE (cpc_code VARCHAR, commodity_name VARCHAR, avg_loss NUMERIC,
               n_obs BIGINT, n_countries BIGINT)
LANGUAGE sql
STABLE
AS $$
    SELECT cm.cpc_code,
           cm.commodity_name,
           ROUND(AVG(o.loss_percentage), 3),
           COUNT(*),
           COUNT(DISTINCT o.m49_code)
    FROM food_waste.observation o
    JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
    WHERE o.year = p_year
    GROUP BY cm.cpc_code, cm.commodity_name
    ORDER BY AVG(o.loss_percentage) DESC
    LIMIT GREATEST(p_limit, 0);
$$;


/* =====================================================================
   PART 4 - VIEWS  (always up to date, no storage)
   ===================================================================== */

-- 4.1 One flat, readable "fact" view. activity_count is the DERIVED attribute
--     from your ERD (computed, never stored). LATERAL = for every observation,
--     do an index lookup on the junction PK -> fast even with a WHERE filter.
CREATE OR REPLACE VIEW analytics.v_observation_detail AS
SELECT o.record_id,
       o.year,
       o.loss_percentage,
       analytics.fn_loss_category(o.loss_percentage) AS loss_category,
       c.m49_code,
       c.country_name,
       cm.cpc_code,
       cm.commodity_name,
       m.method_name,
       s.reference,
       s.url,
       ac.activity_count,
       ac.activities
FROM food_waste.observation o
JOIN food_waste.country   c  ON c.m49_code = o.m49_code
JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
JOIN food_waste.source    s  ON s.source_id = o.source_id
LEFT JOIN food_waste.method m ON m.method_id = o.method_id
JOIN LATERAL (
        SELECT COUNT(*)::INTEGER AS activity_count,
               string_agg(a.activity_name, ', ' ORDER BY a.activity_name) AS activities
        FROM food_waste.observation_activity oa
        JOIN food_waste.activity a ON a.activity_id = oa.activity_id
        WHERE oa.record_id = o.record_id
     ) ac ON TRUE;

-- 4.2 Most recent observation for every (country, commodity) pair.
CREATE OR REPLACE VIEW analytics.v_latest_observation AS
SELECT DISTINCT ON (o.m49_code, o.cpc_code)
       o.m49_code, c.country_name,
       o.cpc_code, cm.commodity_name,
       o.year, o.loss_percentage, o.record_id
FROM food_waste.observation o
JOIN food_waste.country   c  ON c.m49_code = o.m49_code
JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
ORDER BY o.m49_code, o.cpc_code, o.year DESC, o.record_id DESC;


/* =====================================================================
   PART 5 - MATERIALIZED VIEWS  (pre-computed, must be refreshed)
   Use them for heavy aggregates that are read often and change rarely.
   Each has a UNIQUE index -> required for REFRESH ... CONCURRENTLY
   (refresh without blocking readers).
   ===================================================================== */

-- 5.1 Country x year summary
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_country_year_loss AS
SELECT c.m49_code,
       c.country_name,
       o.year,
       COUNT(*)                          AS n_obs,
       ROUND(AVG(o.loss_percentage), 3)  AS avg_loss,
       MIN(o.loss_percentage)            AS min_loss,
       MAX(o.loss_percentage)            AS max_loss
FROM food_waste.observation o
JOIN food_waste.country c ON c.m49_code = o.m49_code
GROUP BY c.m49_code, c.country_name, o.year
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_country_year
    ON analytics.mv_country_year_loss (m49_code, year);

-- materialized views can be indexed like tables: lookup by country NAME
CREATE INDEX IF NOT EXISTS idx_mv_country_name_year
    ON analytics.mv_country_year_loss (country_name, year);

-- 5.2 Commodity ranking
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_commodity_loss_summary AS
SELECT cm.cpc_code,
       cm.commodity_name,
       COUNT(*)                                   AS n_obs,
       COUNT(DISTINCT o.m49_code)                 AS n_countries,
       ROUND(AVG(o.loss_percentage), 3)           AS avg_loss,
       ROUND(STDDEV_SAMP(o.loss_percentage), 3)   AS stddev_loss,
       MAX(o.loss_percentage)                     AS max_loss,
       RANK() OVER (ORDER BY AVG(o.loss_percentage) DESC) AS rank_by_avg_loss
FROM food_waste.observation o
JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
GROUP BY cm.cpc_code, cm.commodity_name
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_commodity
    ON analytics.mv_commodity_loss_summary (cpc_code);

-- 5.3 Activity (supply-chain stage) summary - goes through the M:N junction table
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_activity_loss_summary AS
SELECT a.activity_id,
       a.activity_name,
       COUNT(*)                          AS n_obs,
       ROUND(AVG(o.loss_percentage), 3)  AS avg_loss,
       MAX(o.loss_percentage)            AS max_loss
FROM food_waste.activity a
JOIN food_waste.observation_activity oa ON oa.activity_id = a.activity_id
JOIN food_waste.observation o           ON o.record_id    = oa.record_id
GROUP BY a.activity_id, a.activity_name
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_activity
    ON analytics.mv_activity_loss_summary (activity_id);

-- 5.4 Bookkeeping: which MVs are out of date? (filled by the trigger in Part 6,
--     consumed by the procedure in Part 7)
CREATE TABLE IF NOT EXISTS analytics.mv_refresh_status (
    mv_name           TEXT PRIMARY KEY,
    is_stale          BOOLEAN     NOT NULL DEFAULT FALSE,
    last_refreshed    TIMESTAMPTZ,
    last_data_change  TIMESTAMPTZ
);

INSERT INTO analytics.mv_refresh_status (mv_name, is_stale, last_refreshed)
VALUES ('mv_country_year_loss',      FALSE, now()),
       ('mv_commodity_loss_summary', FALSE, now()),
       ('mv_activity_loss_summary',  FALSE, now())
ON CONFLICT (mv_name) DO NOTHING;


/* =====================================================================
   PART 6 - TRIGGERS  (only where there is a clear reason)
   ===================================================================== */

-- 6.1 AUDIT: keep the history of changed / deleted observations.
--     (INSERTs are not logged - the row itself is the record - which keeps bulk loads cheap.)
CREATE TABLE IF NOT EXISTS food_waste.observation_audit (
    audit_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id   INTEGER     NOT NULL,          -- no FK on purpose: must survive deletes
    operation   CHAR(1)     NOT NULL CHECK (operation IN ('U', 'D')),
    old_row     JSONB       NOT NULL,
    new_row     JSONB,
    changed_by  TEXT        NOT NULL DEFAULT current_user,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_obs_audit_record
    ON food_waste.observation_audit (record_id, changed_at DESC);

CREATE OR REPLACE FUNCTION food_waste.trg_observation_audit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO food_waste.observation_audit (record_id, operation, old_row, new_row)
        VALUES (OLD.record_id, 'U', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSE  -- DELETE
        INSERT INTO food_waste.observation_audit (record_id, operation, old_row, new_row)
        VALUES (OLD.record_id, 'D', to_jsonb(OLD), NULL);
        RETURN OLD;
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_obs_audit_update ON food_waste.observation;
CREATE TRIGGER trg_obs_audit_update
    AFTER UPDATE ON food_waste.observation
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)          -- ignore "no-op" updates
    EXECUTE FUNCTION food_waste.trg_observation_audit();

DROP TRIGGER IF EXISTS trg_obs_audit_delete ON food_waste.observation;
CREATE TRIGGER trg_obs_audit_delete
    AFTER DELETE ON food_waste.observation
    FOR EACH ROW
    EXECUTE FUNCTION food_waste.trg_observation_audit();

-- 6.2 VALIDATION: a rule a CHECK constraint cannot express (depends on today's date).
CREATE OR REPLACE FUNCTION food_waste.trg_observation_validate()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.year < 1900 OR NEW.year > EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER THEN
        RAISE EXCEPTION 'Invalid year % for record_id % (allowed: 1900..%)',
            NEW.year, NEW.record_id, EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_obs_validate ON food_waste.observation;
CREATE TRIGGER trg_obs_validate
    BEFORE INSERT OR UPDATE OF year ON food_waste.observation
    FOR EACH ROW
    EXECUTE FUNCTION food_waste.trg_observation_validate();

-- 6.3 STALE FLAG: materialized views do not refresh themselves, so any data change
--     marks them stale. STATEMENT-level = fires once per statement, not per row.
CREATE OR REPLACE FUNCTION analytics.trg_mark_mv_stale()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE analytics.mv_refresh_status
    SET is_stale = TRUE,
        last_data_change = now();
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_obs_mark_stale ON food_waste.observation;
CREATE TRIGGER trg_obs_mark_stale
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON food_waste.observation
    FOR EACH STATEMENT
    EXECUTE FUNCTION analytics.trg_mark_mv_stale();

DROP TRIGGER IF EXISTS trg_obsact_mark_stale ON food_waste.observation_activity;
CREATE TRIGGER trg_obsact_mark_stale
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON food_waste.observation_activity
    FOR EACH STATEMENT
    EXECUTE FUNCTION analytics.trg_mark_mv_stale();


/* =====================================================================
   PART 7 - PROCEDURES  (CALL ...; they can orchestrate multi-step work)
   ===================================================================== */

-- 7.1 Refresh only what is stale (or everything with p_force => TRUE).
--     CONCURRENTLY = readers are never blocked while it refreshes.
CREATE OR REPLACE PROCEDURE analytics.sp_refresh_analytics(p_force BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT mv_name
             FROM analytics.mv_refresh_status
             WHERE is_stale OR p_force
             ORDER BY mv_name
    LOOP
        EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.%I', r.mv_name);
        UPDATE analytics.mv_refresh_status
        SET is_stale = FALSE, last_refreshed = now()
        WHERE mv_name = r.mv_name;
        RAISE NOTICE 'Refreshed %', r.mv_name;
    END LOOP;
END;
$$;

-- 7.2 Optimized ETL: staging -> core tables, in one atomic call.
--     Differences from the original script:
--       * source lookup uses the hashed key (hash join / index) instead of
--         IS NOT DISTINCT FROM (which forces a nested loop over the whole table);
--       * DISTINCT ON (record_id) so duplicate ids in the CSV cannot crash
--         "ON CONFLICT DO UPDATE command cannot affect row a second time";
--       * the upsert skips rows that did not change (less WAL, no useless audit rows);
--       * row counts are reported, and the MVs are refreshed at the end.
CREATE OR REPLACE PROCEDURE staging.sp_load_food_waste(p_refresh_analytics BOOLEAN DEFAULT TRUE)
LANGUAGE plpgsql
AS $$
DECLARE
    n BIGINT;
BEGIN
    INSERT INTO food_waste.country (m49_code, country_name)
    SELECT DISTINCT ON (TRIM(m49_code)) TRIM(m49_code), TRIM(country)
    FROM staging.stg_food_waste
    WHERE TRIM(m49_code) <> '' AND TRIM(country) <> ''
    ORDER BY TRIM(m49_code), TRIM(country)
    ON CONFLICT (m49_code) DO UPDATE SET country_name = EXCLUDED.country_name
    WHERE food_waste.country.country_name IS DISTINCT FROM EXCLUDED.country_name;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'country: % rows inserted/updated', n;

    INSERT INTO food_waste.commodity (cpc_code, commodity_name)
    SELECT DISTINCT ON (TRIM(cpc_code)) TRIM(cpc_code), TRIM(commodity)
    FROM staging.stg_food_waste
    WHERE TRIM(cpc_code) <> '' AND TRIM(commodity) <> ''
    ORDER BY TRIM(cpc_code), TRIM(commodity)
    ON CONFLICT (cpc_code) DO UPDATE SET commodity_name = EXCLUDED.commodity_name
    WHERE food_waste.commodity.commodity_name IS DISTINCT FROM EXCLUDED.commodity_name;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'commodity: % rows inserted/updated', n;

    INSERT INTO food_waste.method (method_name)
    SELECT DISTINCT TRIM(method_data_collection)
    FROM staging.stg_food_waste
    WHERE TRIM(method_data_collection) <> ''
    ON CONFLICT (method_name) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'method: % rows inserted', n;

    INSERT INTO food_waste.activity (activity_name)
    SELECT DISTINCT TRIM(activity)
    FROM staging.stg_activity_long
    WHERE TRIM(activity) <> ''
    ON CONFLICT (activity_name) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'activity: % rows inserted', n;

    -- unique index uq_source_key makes "ON CONFLICT DO NOTHING" a safe de-duplication
    INSERT INTO food_waste.source (reference, url)
    SELECT DISTINCT NULLIF(TRIM(reference), ''), NULLIF(TRIM(url), '')
    FROM staging.stg_food_waste
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'source: % rows inserted', n;

    INSERT INTO food_waste.observation
           (record_id, year, loss_percentage, cpc_code, method_id, m49_code, source_id)
    SELECT DISTINCT ON (s.record_id::INTEGER)
           s.record_id::INTEGER,
           s.year::INTEGER,
           s.loss_percentage::NUMERIC(8,3),
           TRIM(s.cpc_code),
           m.method_id,
           TRIM(s.m49_code),
           src.source_id
    FROM staging.stg_food_waste s
    JOIN food_waste.country   c  ON c.m49_code = TRIM(s.m49_code)
    JOIN food_waste.commodity cm ON cm.cpc_code = TRIM(s.cpc_code)
    LEFT JOIN food_waste.method m
           ON m.method_name = NULLIF(TRIM(s.method_data_collection), '')
    JOIN food_waste.source src
           ON food_waste.fn_source_key(src.reference, src.url)
            = food_waste.fn_source_key(NULLIF(TRIM(s.reference), ''), NULLIF(TRIM(s.url), ''))
    WHERE TRIM(s.record_id) <> ''
      AND TRIM(s.year) <> ''
      AND TRIM(s.loss_percentage) <> ''
    ORDER BY s.record_id::INTEGER
    ON CONFLICT (record_id) DO UPDATE SET
        year            = EXCLUDED.year,
        loss_percentage = EXCLUDED.loss_percentage,
        cpc_code        = EXCLUDED.cpc_code,
        method_id       = EXCLUDED.method_id,
        m49_code        = EXCLUDED.m49_code,
        source_id       = EXCLUDED.source_id
    WHERE (food_waste.observation.year, food_waste.observation.loss_percentage,
           food_waste.observation.cpc_code, food_waste.observation.method_id,
           food_waste.observation.m49_code, food_waste.observation.source_id)
          IS DISTINCT FROM
          (EXCLUDED.year, EXCLUDED.loss_percentage, EXCLUDED.cpc_code,
           EXCLUDED.method_id, EXCLUDED.m49_code, EXCLUDED.source_id);
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'observation: % rows inserted/updated', n;

    INSERT INTO food_waste.observation_activity (record_id, activity_id)
    SELECT DISTINCT al.record_id::INTEGER, a.activity_id
    FROM staging.stg_activity_long al
    JOIN food_waste.activity    a ON a.activity_name = TRIM(al.activity)
    JOIN food_waste.observation o ON o.record_id     = al.record_id::INTEGER
    WHERE TRIM(al.record_id) <> '' AND TRIM(al.activity) <> ''
    ON CONFLICT (record_id, activity_id) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;  RAISE NOTICE 'observation_activity: % rows inserted', n;

    -- refresh planner statistics after a (possibly) large load
    ANALYZE food_waste.observation;
    ANALYZE food_waste.observation_activity;

    IF p_refresh_analytics THEN
        CALL analytics.sp_refresh_analytics();
    END IF;
END;
$$;
-- Usage:   CALL staging.sp_load_food_waste();


/* =====================================================================
   PART 8 - OPTIMIZED QUERIES: AFTER (compare with the Part 1 baselines)
   ===================================================================== */

-- O1: same query as B1 - now an Index Only Scan on idx_obs_country_year.
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.year, ROUND(AVG(o.loss_percentage), 3) AS avg_loss
FROM food_waste.observation o
JOIN food_waste.country c ON c.m49_code = o.m49_code
WHERE c.country_name = 'Egypt'
GROUP BY o.year
ORDER BY o.year;

--     ...or skip the aggregation completely with the materialized view / function:
EXPLAIN (ANALYZE, BUFFERS)
SELECT year, avg_loss FROM analytics.mv_country_year_loss
WHERE country_name = 'Egypt' ORDER BY year;

SELECT * FROM analytics.fn_country_loss_trend('Egypt', 2000, 2010);

-- O2: sargable rewrite of B2 (plain range on the raw column -> idx_obs_year).
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), AVG(loss_percentage)
FROM food_waste.observation
WHERE year BETWEEN 2010 AND 2019;

-- O3: window function instead of a correlated subquery (ONE pass instead of N passes).
EXPLAIN (ANALYZE, BUFFERS)
SELECT record_id, cpc_code, loss_percentage
FROM (
    SELECT o.record_id, o.cpc_code, o.loss_percentage,
           AVG(o.loss_percentage) OVER (PARTITION BY o.cpc_code) AS commodity_avg
    FROM food_waste.observation o
) t
WHERE loss_percentage > commodity_avg;

-- O4: substring search on long text. On a table this small PostgreSQL correctly prefers a
--     Seq Scan (reading 40 pages is cheaper than using any index). To PROVE the trigram
--     index works, disable seq scans for one test (never do this in production):
SET enable_seqscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT source_id, reference
FROM food_waste.source
WHERE reference ILIKE '%no. 1777 on%';
RESET enable_seqscan;

-- O5: partial index in action (only the worst records are indexed)
EXPLAIN (ANALYZE, BUFFERS)
SELECT record_id, year, loss_percentage
FROM food_waste.observation
WHERE loss_percentage >= 30
ORDER BY loss_percentage DESC
LIMIT 20;

-- O6: the view hides the joins and the derived activity_count
SELECT record_id, country_name, commodity_name, year, loss_percentage,
       loss_category, activity_count, activities
FROM analytics.v_observation_detail
WHERE country_name = 'Egypt' AND year = 2015
ORDER BY loss_percentage DESC
LIMIT 10;


/* =====================================================================
   PART 9 - ADVANCED QUERY PATTERNS
   ===================================================================== */

-- 9.1 Rank commodities inside each country, keep the worst 3 (window function)
SELECT country_name, commodity_name, avg_loss, rnk
FROM (
    SELECT c.country_name, cm.commodity_name,
           ROUND(AVG(o.loss_percentage), 3) AS avg_loss,
           RANK() OVER (PARTITION BY c.m49_code ORDER BY AVG(o.loss_percentage) DESC) AS rnk
    FROM food_waste.observation o
    JOIN food_waste.country   c  ON c.m49_code = o.m49_code
    JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
    WHERE c.country_name = 'Egypt'
    GROUP BY c.m49_code, c.country_name, cm.commodity_name
) t
WHERE rnk <= 3
ORDER BY country_name, rnk;

-- 9.2 Year-over-year change per commodity (LAG)
SELECT cm.commodity_name, o.year,
       ROUND(AVG(o.loss_percentage), 3) AS avg_loss,
       ROUND(AVG(o.loss_percentage)
             - LAG(AVG(o.loss_percentage)) OVER (PARTITION BY cm.cpc_code ORDER BY o.year), 3) AS yoy_change
FROM food_waste.observation o
JOIN food_waste.commodity cm ON cm.cpc_code = o.cpc_code
WHERE cm.commodity_name = 'Wheat'
GROUP BY cm.cpc_code, cm.commodity_name, o.year
ORDER BY o.year;

-- 9.3 Subtotals and grand total in one pass (ROLLUP)
SELECT COALESCE(a.activity_name, '** ALL ACTIVITIES **') AS activity,
       COALESCE(o.year::TEXT,    '** ALL YEARS **')      AS year,
       COUNT(*)                                          AS n_obs,
       ROUND(AVG(o.loss_percentage), 2)                  AS avg_loss
FROM food_waste.observation o
JOIN food_waste.observation_activity oa ON oa.record_id = o.record_id
JOIN food_waste.activity a              ON a.activity_id = oa.activity_id
WHERE o.year BETWEEN 2018 AND 2020
GROUP BY ROLLUP (a.activity_name, o.year)
ORDER BY a.activity_name NULLS LAST, o.year NULLS LAST;

-- 9.4 Latest value per country/commodity through the view (DISTINCT ON inside)
SELECT * FROM analytics.v_latest_observation
WHERE country_name = 'Egypt'
ORDER BY year DESC, commodity_name
LIMIT 10;

-- 9.5 Ready-made reports from the materialized views / functions
SELECT * FROM analytics.mv_commodity_loss_summary ORDER BY rank_by_avg_loss LIMIT 5;
SELECT * FROM analytics.mv_activity_loss_summary  ORDER BY avg_loss DESC;
SELECT * FROM analytics.fn_top_commodities_by_loss(2015, 5);


/* =====================================================================
   PART 10 - INDEX HEALTH CHECK  (run after the system has been used a while)
   idx_scan = 0 for a long time  ->  index is never used  ->  candidate to drop.
   ===================================================================== */
SELECT s.relname                                AS table_name,
       s.indexrelname                           AS index_name,
       s.idx_scan                               AS times_used,
       pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size
FROM pg_stat_user_indexes s
WHERE s.schemaname IN ('food_waste', 'analytics')
ORDER BY pg_relation_size(s.indexrelid) DESC;

SELECT * FROM analytics.mv_refresh_status ORDER BY mv_name;


/* =====================================================================
   PART 11 - DEMO (copy the lines between the markers and run them in one go).
   Everything is rolled back, so data stays exactly as it was.
   ---------------------------------------------------------------------
-- >>> START
BEGIN;

-- (a) audit trigger + stale flag: change one record
UPDATE food_waste.observation
SET loss_percentage = ROUND(loss_percentage / 2, 3) + 0.001
WHERE record_id = (SELECT MIN(record_id) FROM food_waste.observation);

SELECT record_id, operation,
       old_row->>'loss_percentage' AS old_loss,
       new_row->>'loss_percentage' AS new_loss,
       changed_by, changed_at
FROM food_waste.observation_audit ORDER BY audit_id DESC LIMIT 3;

SELECT mv_name, is_stale FROM analytics.mv_refresh_status ORDER BY mv_name;   -- all TRUE now

-- (b) procedure: refreshes only what is stale
CALL analytics.sp_refresh_analytics();
SELECT mv_name, is_stale FROM analytics.mv_refresh_status ORDER BY mv_name;   -- all FALSE now

-- (c) validation trigger blocks an impossible year
DO $demo$
BEGIN
    UPDATE food_waste.observation SET year = 2099
    WHERE record_id = (SELECT MIN(record_id) FROM food_waste.observation);
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'Blocked by trigger: %', SQLERRM;
END
$demo$;

ROLLBACK;
-- <<< END
   ===================================================================== */
