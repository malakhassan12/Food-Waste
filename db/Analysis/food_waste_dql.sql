-- ============================================================================
-- food_waste_dql.sql  -  Data analysis queries (DQL), Step 8
--
-- Answers the project's analytical questions:
--   1. Which crops have the highest recorded loss?
--   2. How does loss change over years and across regions?
--   3. Where in the supply chain is loss recorded, and what are the root causes?
--
-- Prerequisites (run once, in this order):
--   1. db/food_waste_queries.sql            (schema + ETL)
--   2. db/analysis/00_setup_reference_and_raw.sql  (lookups, raw staging, views)
--
-- Conventions used throughout:
--   * Loss values are percentages (0-100), as stored in observation.loss_percentage.
--   * Medians are used instead of means for ranking. The loss distribution is
--     skewed (max 65%, most records are 1-5%), so a mean is pulled by outliers.
--   * Crops, countries and continents come from v_observation_analysis, which
--     only includes the 142 countries in food_waste.country_continent. Global
--     aggregate rows (World, Africa, Asia, ...) are not in that list, so they
--     are never counted as countries.
--   * Thresholds (minimum records, minimum countries, reliable years) are written
--     inline so each rule is visible. Change them in the query where they appear.
--   * Method groups: 'Modelled estimate' (62% of records, see 0.2) is reported
--     separately because it systematically reports lower loss than measured or
--     published data. Rankings are shown both with and without it.
--   * Records can belong to several supply chain stages. Stage totals are per
--     stage and must NOT be summed across stages.
--
-- Limitations to keep in mind when reading results:
--   * Year coverage collapses after 2021 (see 0.3). Trend queries use only
--     years with at least 30 reporting countries.
--   * Country coverage varies by year, so year-to-year changes can reflect which
--     countries reported. Section 2.3 and 2.4 compare the same countries across
--     periods to avoid this.
--   * A high loss in one record does not mean that crop is the most lost crop.
--     Rankings aggregate over many records, with minimum-size rules.
-- ============================================================================


-- ============================================================================
-- 0. DATA COVERAGE AND GUARDRAILS
-- ============================================================================

-- 0.1 Overall coverage
SELECT
    COUNT(*)                     AS records,
    COUNT(DISTINCT country_name) AS countries,
    COUNT(DISTINCT crop)         AS crops,
    MIN(year)                    AS first_year,
    MAX(year)                    AS last_year
FROM food_waste.v_observation_analysis;


-- 0.2 Method groups: how many records, and how their loss levels differ.
--     Shows why rankings are checked with and without modelled estimates.
WITH total AS (
    SELECT COUNT(*) AS total_records FROM food_waste.v_observation_analysis
),
base AS (
    SELECT method_group, loss_percentage FROM food_waste.v_observation_analysis
),
ranked AS (
    SELECT method_group, loss_percentage,
           ROW_NUMBER() OVER (PARTITION BY method_group ORDER BY loss_percentage) AS rn,
           COUNT(*)     OVER (PARTITION BY method_group)                          AS n
    FROM base
),
med AS (
    SELECT method_group, AVG(loss_percentage) AS median_loss
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY method_group
),
st AS (
    SELECT method_group,
           COUNT(*)                         AS records,
           ROUND(AVG(loss_percentage), 2)   AS mean_loss
    FROM base
    GROUP BY method_group
)
SELECT
    st.method_group,
    st.records,
    ROUND(100.0 * st.records / t.total_records, 1) AS share_of_records_pct,
    ROUND(med.median_loss, 2)                      AS median_loss_pct,
    st.mean_loss                                   AS mean_loss_pct
FROM st
JOIN med ON med.method_group = st.method_group
CROSS JOIN total t
ORDER BY st.records DESC;


-- 0.3 Year coverage. A year is "reliable" when at least 30 countries reported.
--     Only reliable years are used for trend analysis in Section 2.
SELECT
    year,
    COUNT(*)                     AS records,
    COUNT(DISTINCT country_name) AS countries,
    CASE WHEN COUNT(DISTINCT country_name) >= 30
         THEN 'Yes' ELSE 'No' END AS reliable_year
FROM food_waste.v_observation_analysis
GROUP BY year
ORDER BY year;


-- ============================================================================
-- 1. CROPS WITH THE HIGHEST RECORDED LOSS
-- ============================================================================

-- 1.1 Crop ranking by median loss, all methods.
--     Rule: at least 30 records from at least 5 countries.
WITH crop_size AS (
    SELECT
        crop,
        COUNT(*)                         AS records,
        COUNT(DISTINCT country_name)     AS countries,
        COUNT(DISTINCT year)             AS years,
        ROUND(AVG(loss_percentage), 2)   AS mean_loss_pct,
        MAX(loss_percentage)             AS max_loss_pct
    FROM food_waste.v_observation_analysis
    GROUP BY crop
    HAVING COUNT(*) >= 30 AND COUNT(DISTINCT country_name) >= 5
),
ranked AS (
    SELECT
        v.crop,
        v.loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY v.crop ORDER BY v.loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY v.crop)                            AS n
    FROM food_waste.v_observation_analysis v
    JOIN crop_size s ON s.crop = v.crop
),
crop_median AS (
    SELECT crop, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY crop
)
SELECT
    ROW_NUMBER() OVER (ORDER BY m.median_loss_pct DESC, s.records DESC) AS crop_rank,
    s.crop,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    s.mean_loss_pct,
    s.max_loss_pct,
    s.records,
    s.countries,
    s.years
FROM crop_size s
JOIN crop_median m ON m.crop = s.crop
ORDER BY m.median_loss_pct DESC, s.records DESC
LIMIT 15;


-- 1.2 Same ranking using measured, surveyed or published data only
--     (modelled estimates and expert opinion removed). If a crop stays near the
--     top here, its high loss is not just an artefact of the modelling method.
WITH crop_size AS (
    SELECT
        crop,
        COUNT(*)                         AS records,
        COUNT(DISTINCT country_name)     AS countries,
        ROUND(AVG(loss_percentage), 2)   AS mean_loss_pct
    FROM food_waste.v_observation_analysis
    WHERE method_group = 'Measured, surveyed or published'
    GROUP BY crop
    HAVING COUNT(*) >= 30 AND COUNT(DISTINCT country_name) >= 5
),
ranked AS (
    SELECT
        v.crop,
        v.loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY v.crop ORDER BY v.loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY v.crop)                            AS n
    FROM food_waste.v_observation_analysis v
    JOIN crop_size s ON s.crop = v.crop
    WHERE v.method_group = 'Measured, surveyed or published'
),
crop_median AS (
    SELECT crop, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY crop
)
SELECT
    ROW_NUMBER() OVER (ORDER BY m.median_loss_pct DESC, s.records DESC) AS crop_rank,
    s.crop,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    s.mean_loss_pct,
    s.records,
    s.countries
FROM crop_size s
JOIN crop_median m ON m.crop = s.crop
ORDER BY m.median_loss_pct DESC, s.records DESC
LIMIT 15;


-- 1.3 Highest single records. For data review only, NOT a crop ranking:
--     one record is one country-crop-year measurement, and it may be a
--     single study or a modelled estimate.
SELECT
    crop,
    country_name,
    year,
    loss_percentage,
    method,
    source_reference
FROM food_waste.v_observation_analysis
ORDER BY loss_percentage DESC, record_id
LIMIT 10;


-- ============================================================================
-- 2. CHANGE OVER YEARS AND ACROSS REGIONS
--    "Regions" are the UN M49 continental groups (Africa, Americas, Asia, Europe,
--    Oceania). The database has no sub-national region field: the raw
--    region column is blank for about 94% of rows and is not comparable.
-- ============================================================================

-- 2.1 Yearly loss profile for reliable years, with change from the previous
--     reliable year. Composition (which countries reported) can change between
--     years, so 2.3 and 2.4 are the like-for-like comparisons.
WITH reliable_years AS (
    SELECT year
    FROM food_waste.v_observation_analysis
    GROUP BY year
    HAVING COUNT(DISTINCT country_name) >= 30
),
yr_base AS (
    SELECT v.year, v.loss_percentage
    FROM food_waste.v_observation_analysis v
    JOIN reliable_years r ON r.year = v.year
),
ranked AS (
    SELECT
        year,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY year)                          AS n
    FROM yr_base
),
yr_median AS (
    SELECT year, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY year
),
yr_stats AS (
    SELECT year, COUNT(*) AS records, ROUND(AVG(loss_percentage), 2) AS mean_loss_pct
    FROM yr_base
    GROUP BY year
)
SELECT
    s.year,
    s.records,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    s.mean_loss_pct,
    ROUND(m.median_loss_pct - LAG(m.median_loss_pct) OVER (ORDER BY s.year), 2)
        AS change_vs_prev_year_pts
FROM yr_stats s
JOIN yr_median m ON m.year = s.year
ORDER BY s.year;


-- 2.2 Median loss by continent and reliable year.
--     Only continent-years with at least 20 records are shown.
WITH reliable_years AS (
    SELECT year
    FROM food_waste.v_observation_analysis
    GROUP BY year
    HAVING COUNT(DISTINCT country_name) >= 30
),
base AS (
    SELECT v.continent, v.year, v.country_name, v.loss_percentage
    FROM food_waste.v_observation_analysis v
    JOIN reliable_years r ON r.year = v.year
),
ranked AS (
    SELECT
        continent,
        year,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY continent, year ORDER BY loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY continent, year)                          AS n
    FROM base
),
med AS (
    SELECT continent, year, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY continent, year
),
cnt AS (
    SELECT
        continent,
        year,
        COUNT(*)                     AS records,
        COUNT(DISTINCT country_name) AS countries
    FROM base
    GROUP BY continent, year
)
SELECT
    c.continent,
    c.year,
    c.records,
    c.countries,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct
FROM cnt c
JOIN med m ON m.continent = c.continent AND m.year = c.year
WHERE c.records >= 20
ORDER BY c.continent, c.year;


-- 2.3 Like-for-like change by crop, 2000-2010 vs 2011-2021.
--     For each crop and country, the average loss in each period is calculated,
--     and only crop-country pairs present in BOTH periods are compared. This
--     removes the effect of countries entering or leaving the dataset.
--     Rule: at least 5 paired countries per crop.
WITH periods AS (
    SELECT
        crop,
        country_name,
        continent,
        loss_percentage,
        CASE
            WHEN year BETWEEN 2000 AND 2010 THEN 'early'
            WHEN year BETWEEN 2011 AND 2021 THEN 'late'
        END AS period
    FROM food_waste.v_observation_analysis
    WHERE year BETWEEN 2000 AND 2021
),
pair AS (
    SELECT
        crop,
        country_name,
        continent,
        AVG(CASE WHEN period = 'early' THEN loss_percentage END) AS early_loss,
        AVG(CASE WHEN period = 'late'  THEN loss_percentage END) AS late_loss
    FROM periods
    WHERE period IS NOT NULL
    GROUP BY crop, country_name, continent
),
paired AS (
    SELECT * FROM pair
    WHERE early_loss IS NOT NULL AND late_loss IS NOT NULL
)
SELECT
    crop,
    COUNT(*)                                 AS paired_countries,
    ROUND(AVG(early_loss), 2)                AS early_avg_loss_pct,
    ROUND(AVG(late_loss), 2)                 AS late_avg_loss_pct,
    ROUND(AVG(late_loss - early_loss), 2)    AS avg_change_pts
FROM paired
GROUP BY crop
HAVING COUNT(*) >= 5
ORDER BY ABS(AVG(late_loss - early_loss)) DESC, crop
LIMIT 15;


-- 2.4 Like-for-like change by continent, same method as 2.3.
WITH periods AS (
    SELECT
        crop,
        country_name,
        continent,
        loss_percentage,
        CASE
            WHEN year BETWEEN 2000 AND 2010 THEN 'early'
            WHEN year BETWEEN 2011 AND 2021 THEN 'late'
        END AS period
    FROM food_waste.v_observation_analysis
    WHERE year BETWEEN 2000 AND 2021
),
pair AS (
    SELECT
        crop,
        country_name,
        continent,
        AVG(CASE WHEN period = 'early' THEN loss_percentage END) AS early_loss,
        AVG(CASE WHEN period = 'late'  THEN loss_percentage END) AS late_loss
    FROM periods
    WHERE period IS NOT NULL
    GROUP BY crop, country_name, continent
),
paired AS (
    SELECT * FROM pair
    WHERE early_loss IS NOT NULL AND late_loss IS NOT NULL
)
SELECT
    continent,
    COUNT(*)                                 AS paired_crop_country_pairs,
    COUNT(DISTINCT country_name)             AS paired_countries,
    ROUND(AVG(early_loss), 2)                AS early_avg_loss_pct,
    ROUND(AVG(late_loss), 2)                 AS late_avg_loss_pct,
    ROUND(AVG(late_loss - early_loss), 2)    AS avg_change_pts
FROM paired
GROUP BY continent
ORDER BY continent;


-- ============================================================================
-- 3. LOSS BY SUPPLY CHAIN STAGE
--    Stage groups come from food_waste.activity_stage_map (52 split activity
--    labels rolled up into 7 stage groups). "Whole supply chain" and "Unknown"
--    are excluded because they are not stages.
-- ============================================================================

-- 3.1 Median loss by stage group, all records.
--     A record in two stage groups counts once in each group.
WITH stage_obs AS (
    SELECT DISTINCT record_id, stage_group, loss_percentage, crop, country_name
    FROM food_waste.v_stage_observation
),
ranked AS (
    SELECT
        stage_group,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY stage_group ORDER BY loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY stage_group)                          AS n
    FROM stage_obs
),
med AS (
    SELECT stage_group, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY stage_group
),
st AS (
    SELECT
        stage_group,
        COUNT(*)                         AS records,
        COUNT(DISTINCT country_name)     AS countries,
        COUNT(DISTINCT crop)             AS crops,
        ROUND(AVG(loss_percentage), 2)   AS mean_loss_pct,
        MAX(loss_percentage)             AS max_loss_pct
    FROM stage_obs
    GROUP BY stage_group
)
SELECT
    st.stage_group,
    st.records,
    st.countries,
    st.crops,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    st.mean_loss_pct,
    st.max_loss_pct
FROM st
JOIN med m ON m.stage_group = st.stage_group
ORDER BY median_loss_pct DESC;


-- 3.2 Sensitivity: the same stage comparison for records tagged with exactly
--     one activity. These records have an unambiguous stage, so the result does
--     not depend on how loss was split between stages.
WITH stage_obs AS (
    SELECT DISTINCT record_id, stage_group, loss_percentage, crop, country_name
    FROM food_waste.v_stage_observation
    WHERE stage_count = 1
),
ranked AS (
    SELECT
        stage_group,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY stage_group ORDER BY loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY stage_group)                          AS n
    FROM stage_obs
),
med AS (
    SELECT stage_group, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY stage_group
),
st AS (
    SELECT
        stage_group,
        COUNT(*)                         AS records,
        COUNT(DISTINCT country_name)     AS countries,
        ROUND(AVG(loss_percentage), 2)   AS mean_loss_pct
    FROM stage_obs
    GROUP BY stage_group
)
SELECT
    st.stage_group,
    st.records,
    st.countries,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    st.mean_loss_pct
FROM st
JOIN med m ON m.stage_group = st.stage_group
ORDER BY median_loss_pct DESC;


-- 3.3 Individual split activities with the highest median loss.
--     Rule: at least 30 records. Uses the original activity labels, so it
--     shows detail that the stage groups in 3.1 hide.
WITH act_obs AS (
    SELECT activity_name, stage_group, loss_percentage, record_id
    FROM food_waste.v_stage_observation
),
act_size AS (
    SELECT
        activity_name,
        stage_group,
        COUNT(*)                       AS records,
        ROUND(AVG(loss_percentage), 2) AS mean_loss_pct
    FROM act_obs
    GROUP BY activity_name, stage_group
    HAVING COUNT(*) >= 30
),
ranked AS (
    SELECT
        o.activity_name,
        o.loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY o.activity_name ORDER BY o.loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY o.activity_name)                             AS n
    FROM act_obs o
    JOIN act_size s ON s.activity_name = o.activity_name
),
med AS (
    SELECT activity_name, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY activity_name
)
SELECT
    s.activity_name,
    s.stage_group,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct,
    s.mean_loss_pct,
    s.records
FROM act_size s
JOIN med m ON m.activity_name = s.activity_name
ORDER BY m.median_loss_pct DESC, s.records DESC
LIMIT 15;


-- ============================================================================
-- 4. ROOT CAUSES OF LOSS
--    cause_of_loss is only in the raw CSV (about 3.5% of rows have a text).
--    Causes are grouped by keyword rules, checked in priority order (first match
--    wins), and defined in v_cause_of_loss. Rows that match no rule are kept as
--    "Not a specific cause". Results are row-level from the raw file, not
--    linked to food_waste.observation.
-- ============================================================================

-- 4.1 How much of the data has a recorded cause
WITH country_rows AS (
    SELECT COUNT(*) AS total_rows
    FROM food_waste.stg_raw_food_waste r
    JOIN food_waste.country_continent cc
      ON cc.m49_code = TRIM(r.m49_code)
),
cause_rows AS (
    SELECT COUNT(*) AS rows_with_cause
    FROM food_waste.v_cause_of_loss
)
SELECT
    t.total_rows,
    c.rows_with_cause,
    ROUND(100.0 * c.rows_with_cause / t.total_rows, 1) AS pct_rows_with_cause
FROM country_rows t
CROSS JOIN cause_rows c;


-- 4.2 Root-cause categories: how often each is recorded and how much loss
--     it is associated with (median).
WITH base AS (
    SELECT cause_category, crop, country_name, loss_percentage
    FROM food_waste.v_cause_of_loss
),
total AS (
    SELECT COUNT(*) AS total_rows FROM base
),
ranked AS (
    SELECT
        cause_category,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY cause_category ORDER BY loss_percentage) AS rn,
        COUNT(*)     OVER (PARTITION BY cause_category)                          AS n
    FROM base
),
med AS (
    SELECT cause_category, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY cause_category
),
st AS (
    SELECT
        cause_category,
        COUNT(*)                         AS rows_n,
        COUNT(DISTINCT country_name)     AS countries,
        COUNT(DISTINCT crop)             AS crops,
        ROUND(AVG(loss_percentage), 2)   AS mean_loss_pct
    FROM base
    GROUP BY cause_category
)
SELECT
    st.cause_category,
    st.rows_n                                    AS raw_rows,
    ROUND(100.0 * st.rows_n / t.total_rows, 1)   AS share_of_cause_rows_pct,
    ROUND(m.median_loss_pct, 2)                  AS median_loss_pct,
    st.mean_loss_pct,
    st.countries,
    st.crops
FROM st
JOIN med m ON m.cause_category = st.cause_category
CROSS JOIN total t
ORDER BY st.rows_n DESC;


-- 4.3 Root causes by supply chain stage (raw food_supply_stage column).
--     Shows which causes are recorded at which stage. Only combinations with
--     at least 5 rows are shown. "Not a specific cause" is excluded.
WITH base AS (
    SELECT food_supply_stage, cause_category, loss_percentage
    FROM food_waste.v_cause_of_loss
    WHERE food_supply_stage IS NOT NULL
      AND food_supply_stage <> 'Whole supply chain'
      AND cause_category <> 'Not a specific cause'
),
ranked AS (
    SELECT
        food_supply_stage,
        cause_category,
        loss_percentage,
        ROW_NUMBER() OVER (PARTITION BY food_supply_stage, cause_category
                           ORDER BY loss_percentage)                            AS rn,
        COUNT(*)     OVER (PARTITION BY food_supply_stage, cause_category)      AS n
    FROM base
),
med AS (
    SELECT food_supply_stage, cause_category, AVG(loss_percentage) AS median_loss_pct
    FROM ranked
    WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
    GROUP BY food_supply_stage, cause_category
),
st AS (
    SELECT
        food_supply_stage,
        cause_category,
        COUNT(*) AS rows_n
    FROM base
    GROUP BY food_supply_stage, cause_category
    HAVING COUNT(*) >= 5
)
SELECT
    st.food_supply_stage,
    st.cause_category,
    st.rows_n AS raw_rows,
    ROUND(m.median_loss_pct, 2) AS median_loss_pct
FROM st
JOIN med m
  ON m.food_supply_stage = st.food_supply_stage
 AND m.cause_category    = st.cause_category
ORDER BY st.food_supply_stage, st.rows_n DESC, st.cause_category;


-- 4.4 Rule review: the distinct cause texts behind each category, most frequent
--     first. Use this to check the keyword rules in v_cause_of_loss and to find
--     texts that should be re-classified.
SELECT
    SUBSTR(cause_text, 1, 120) AS cause_text_preview,
    cause_category,
    COUNT(*) AS raw_rows
FROM food_waste.v_cause_of_loss
GROUP BY SUBSTR(cause_text, 1, 120), cause_category
ORDER BY raw_rows DESC, cause_text_preview
LIMIT 40;
