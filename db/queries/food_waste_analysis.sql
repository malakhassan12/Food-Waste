-- ============================================================================
-- food_waste_analysis.sql  -  Food loss analysis for the final dashboard
--
-- ONE file with everything the analysis needs:
--   PART A  setup      lookup tables (continent, supply chain stage) + analysis views
--   PART B  dashboard  v_dash_* views that a BI tool can read directly
--   PART C  queries    every analytical question, one named block per question
--
-- Works on the tables created by food_waste_queries.sql (public schema):
--   country, commodity, method, activity, source, observation, observation_activity
--
-- Run it as a script (psql, pgAdmin, DBeaver) or let the notebook
-- Step_5_Data_Analysis_Dashboard.ipynb run it block by block.
-- Safe to re-run: views and lookup tables are dropped and rebuilt.
--
-- Block format (read by the notebook):
--   -- name:     unique id of the block
--   -- page:     dashboard page the result belongs to
--   -- title:    short title of the chart or table
--   -- question: the business question the block answers
--   -- visual:   suggested chart type
--
-- Rules used everywhere
--   * Loss is a percentage (0-100) from observation.loss_percentage.
--   * Medians rank things, not means: the distribution is skewed, so a few
--     very high records would pull a mean up.
--   * Minimum sizes are written inside each query so the rule is visible.
--   * A year is reliable when at least 30 countries reported (see yearly views).
--   * Modelled estimates are reported separately from measured data.
--   * A record can belong to several supply chain stages: stage totals are per
--     stage and must not be added together.
--   * cause_of_loss is not part of this cleaned dataset, so root causes are not
--     analysed here.
-- ============================================================================

-- name: setup
-- page: 0 Setup
-- title: Lookup tables and analysis views
-- question: Prepare continent and stage lookups plus the views used by every query.
-- visual: none

-- ---------------------------------------------------------------------------
-- A1. Drop old views (dependent views first) and lookup tables
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_dash_method CASCADE;
DROP VIEW IF EXISTS v_dash_stage CASCADE;
DROP VIEW IF EXISTS v_dash_continent CASCADE;
DROP VIEW IF EXISTS v_dash_country CASCADE;
DROP VIEW IF EXISTS v_dash_crops CASCADE;
DROP VIEW IF EXISTS v_dash_yearly CASCADE;
DROP VIEW IF EXISTS v_dash_kpis CASCADE;
DROP VIEW IF EXISTS v_stage CASCADE;
DROP VIEW IF EXISTS v_activity_obs CASCADE;
DROP VIEW IF EXISTS v_obs CASCADE;
DROP TABLE IF EXISTS activity_stage_map CASCADE;
DROP TABLE IF EXISTS country_continent CASCADE;

-- ---------------------------------------------------------------------------
-- A2. Country -> continent (UN M49 continental groups)
--     The database has no region column. Only countries present in the
--     country table are kept. Codes are matched without leading zeros.
-- ---------------------------------------------------------------------------
CREATE TABLE country_continent (
    m49_code     VARCHAR(3)   PRIMARY KEY,
    country_name VARCHAR(150) NOT NULL,
    continent    VARCHAR(30)  NOT NULL
);

INSERT INTO country_continent (m49_code, country_name, continent)
SELECT c.m49_code, c.country_name, v.continent
FROM (VALUES
    ('12', 'Africa'),  -- Algeria
    ('24', 'Africa'),  -- Angola
    ('204', 'Africa'),  -- Benin
    ('72', 'Africa'),  -- Botswana
    ('854', 'Africa'),  -- Burkina Faso
    ('108', 'Africa'),  -- Burundi
    ('120', 'Africa'),  -- Cameroon
    ('148', 'Africa'),  -- Chad
    ('180', 'Africa'),  -- DR Congo
    ('818', 'Africa'),  -- Egypt
    ('232', 'Africa'),  -- Eritrea
    ('748', 'Africa'),  -- Eswatini
    ('231', 'Africa'),  -- Ethiopia
    ('266', 'Africa'),  -- Gabon
    ('270', 'Africa'),  -- Gambia
    ('288', 'Africa'),  -- Ghana
    ('324', 'Africa'),  -- Guinea
    ('624', 'Africa'),  -- Guinea-Bissau
    ('384', 'Africa'),  -- Ivory Coast
    ('404', 'Africa'),  -- Kenya
    ('426', 'Africa'),  -- Lesotho
    ('430', 'Africa'),  -- Liberia
    ('450', 'Africa'),  -- Madagascar
    ('454', 'Africa'),  -- Malawi
    ('466', 'Africa'),  -- Mali
    ('478', 'Africa'),  -- Mauritania
    ('480', 'Africa'),  -- Mauritius
    ('504', 'Africa'),  -- Morocco
    ('508', 'Africa'),  -- Mozambique
    ('516', 'Africa'),  -- Namibia
    ('562', 'Africa'),  -- Niger
    ('566', 'Africa'),  -- Nigeria
    ('646', 'Africa'),  -- Rwanda
    ('686', 'Africa'),  -- Senegal
    ('694', 'Africa'),  -- Sierra Leone
    ('706', 'Africa'),  -- Somalia
    ('710', 'Africa'),  -- South Africa
    ('728', 'Africa'),  -- South Sudan
    ('729', 'Africa'),  -- Sudan
    ('834', 'Africa'),  -- Tanzania
    ('768', 'Africa'),  -- Togo
    ('788', 'Africa'),  -- Tunisia
    ('800', 'Africa'),  -- Uganda
    ('894', 'Africa'),  -- Zambia
    ('716', 'Africa'),  -- Zimbabwe
    ('32', 'Americas'),  -- Argentina
    ('68', 'Americas'),  -- Bolivia
    ('76', 'Americas'),  -- Brazil
    ('124', 'Americas'),  -- Canada
    ('152', 'Americas'),  -- Chile
    ('170', 'Americas'),  -- Colombia
    ('188', 'Americas'),  -- Costa Rica
    ('192', 'Americas'),  -- Cuba
    ('218', 'Americas'),  -- Ecuador
    ('222', 'Americas'),  -- El Salvador
    ('320', 'Americas'),  -- Guatemala
    ('328', 'Americas'),  -- Guyana
    ('332', 'Americas'),  -- Haiti
    ('340', 'Americas'),  -- Honduras
    ('388', 'Americas'),  -- Jamaica
    ('484', 'Americas'),  -- Mexico
    ('558', 'Americas'),  -- Nicaragua
    ('591', 'Americas'),  -- Panama
    ('600', 'Americas'),  -- Paraguay
    ('604', 'Americas'),  -- Peru
    ('659', 'Americas'),  -- Saint Kitts and Nevis
    ('662', 'Americas'),  -- Saint Lucia
    ('780', 'Americas'),  -- Trinidad and Tobago
    ('840', 'Americas'),  -- USA
    ('862', 'Americas'),  -- Venezuela
    ('4', 'Asia'),  -- Afghanistan
    ('51', 'Asia'),  -- Armenia
    ('31', 'Asia'),  -- Azerbaijan
    ('48', 'Asia'),  -- Bahrain
    ('50', 'Asia'),  -- Bangladesh
    ('116', 'Asia'),  -- Cambodia
    ('156', 'Asia'),  -- China
    ('268', 'Asia'),  -- Georgia
    ('356', 'Asia'),  -- India
    ('360', 'Asia'),  -- Indonesia
    ('364', 'Asia'),  -- Iran
    ('392', 'Asia'),  -- Japan
    ('400', 'Asia'),  -- Jordan
    ('398', 'Asia'),  -- Kazakhstan
    ('417', 'Asia'),  -- Kyrgyzstan
    ('418', 'Asia'),  -- Laos
    ('422', 'Asia'),  -- Lebanon
    ('458', 'Asia'),  -- Malaysia
    ('496', 'Asia'),  -- Mongolia
    ('104', 'Asia'),  -- Myanmar
    ('524', 'Asia'),  -- Nepal
    ('408', 'Asia'),  -- North Korea
    ('512', 'Asia'),  -- Oman
    ('586', 'Asia'),  -- Pakistan
    ('275', 'Asia'),  -- Palestine
    ('608', 'Asia'),  -- Philippines
    ('682', 'Asia'),  -- Saudi Arabia
    ('144', 'Asia'),  -- Sri Lanka
    ('760', 'Asia'),  -- Syrian Arab Republic
    ('158', 'Asia'),  -- Taiwan
    ('762', 'Asia'),  -- Tajikistan
    ('764', 'Asia'),  -- Thailand
    ('626', 'Asia'),  -- Timor-Leste
    ('792', 'Asia'),  -- Turkey
    ('860', 'Asia'),  -- Uzbekistan
    ('704', 'Asia'),  -- Viet Nam
    ('410', 'Asia'),  -- South Korea
    ('8', 'Europe'),  -- Albania
    ('40', 'Europe'),  -- Austria
    ('112', 'Europe'),  -- Belarus
    ('56', 'Europe'),  -- Belgium
    ('191', 'Europe'),  -- Croatia
    ('208', 'Europe'),  -- Denmark
    ('233', 'Europe'),  -- Estonia
    ('246', 'Europe'),  -- Finland
    ('250', 'Europe'),  -- France
    ('276', 'Europe'),  -- Germany
    ('300', 'Europe'),  -- Greece
    ('348', 'Europe'),  -- Hungary
    ('372', 'Europe'),  -- Ireland
    ('380', 'Europe'),  -- Italy
    ('428', 'Europe'),  -- Latvia
    ('440', 'Europe'),  -- Lithuania
    ('442', 'Europe'),  -- Luxembourg
    ('528', 'Europe'),  -- Netherlands
    ('807', 'Europe'),  -- North Macedonia
    ('578', 'Europe'),  -- Norway
    ('616', 'Europe'),  -- Poland
    ('620', 'Europe'),  -- Portugal
    ('642', 'Europe'),  -- Romania
    ('643', 'Europe'),  -- Russian Federation
    ('688', 'Europe'),  -- Serbia
    ('703', 'Europe'),  -- Slovakia
    ('724', 'Europe'),  -- Spain
    ('752', 'Europe'),  -- Sweden
    ('756', 'Europe'),  -- Switzerland
    ('804', 'Europe'),  -- Ukraine
    ('826', 'Europe'),  -- UK
    ('36', 'Oceania'),  -- Australia
    ('242', 'Oceania'),  -- Fiji
    ('540', 'Oceania'),  -- New Caledonia
    ('554', 'Oceania')  -- New Zealand
) AS v(m49_code, continent)
JOIN country c
  ON LTRIM(c.m49_code, '0') = v.m49_code;

-- ---------------------------------------------------------------------------
-- A3. Activity -> supply chain stage group
--     The database has 52 split activity labels. They are rolled up into stage
--     groups. A NULL stage_group is not a real stage (Whole supply chain is a
--     multi-stage aggregate, Unknown has no stage information).
-- ---------------------------------------------------------------------------
CREATE TABLE activity_stage_map (
    activity_name VARCHAR(250) PRIMARY KEY,
    stage_group   VARCHAR(40)
);

INSERT INTO activity_stage_map (activity_name, stage_group)
SELECT a.activity_name, v.stage_group
FROM (VALUES
    ('Farm', 'Farm & harvest'),
    ('Field', 'Farm & harvest'),
    ('Harvesting', 'Farm & harvest'),
    ('Threshing', 'Farm & harvest'),
    ('Shelling', 'Farm & harvest'),
    ('Winnowing', 'Farm & harvest'),
    ('Husking', 'Farm & harvest'),
    ('Picking', 'Farm & harvest'),
    ('Lifting', 'Farm & harvest'),
    ('Drying', 'Post-harvest handling'),
    ('Cleaning', 'Post-harvest handling'),
    ('Sorting', 'Post-harvest handling'),
    ('Grading', 'Post-harvest handling'),
    ('Packaging', 'Post-harvest handling'),
    ('Bagging', 'Post-harvest handling'),
    ('Handling', 'Post-harvest handling'),
    ('Curing', 'Post-harvest handling'),
    ('Ripening', 'Post-harvest handling'),
    ('Washing', 'Post-harvest handling'),
    ('Dewatering', 'Post-harvest handling'),
    ('Sifting', 'Post-harvest handling'),
    ('Polishing', 'Post-harvest handling'),
    ('Stacking', 'Post-harvest handling'),
    ('Layering', 'Post-harvest handling'),
    ('Bundling', 'Post-harvest handling'),
    ('Piling', 'Post-harvest handling'),
    ('Bulking', 'Post-harvest handling'),
    ('Assembling', 'Post-harvest handling'),
    ('Storage', 'Storage'),
    ('Preservation', 'Storage'),
    ('Transportation', 'Transport & distribution'),
    ('Collection', 'Transport & distribution'),
    ('Distribution', 'Transport & distribution'),
    ('Loading', 'Transport & distribution'),
    ('Unloading', 'Transport & distribution'),
    ('Exporting', 'Transport & distribution'),
    ('Processing', 'Processing'),
    ('Milling', 'Processing'),
    ('Manufacturing', 'Processing'),
    ('Parboiling', 'Processing'),
    ('Peeling', 'Processing'),
    ('Roasting', 'Processing'),
    ('Blanching', 'Processing'),
    ('Freezing', 'Processing'),
    ('Grating', 'Processing'),
    ('Wholesale', 'Market & retail'),
    ('Retailing', 'Market & retail'),
    ('Marketing', 'Market & retail'),
    ('Trading', 'Market & retail'),
    ('Consumption', 'Consumption'),
    ('Whole supply chain', NULL),
    ('Unknown', NULL)
) AS v(activity_name, stage_group)
JOIN activity a
  ON a.activity_name = v.activity_name;

-- ---------------------------------------------------------------------------
-- A4. v_obs: one row per observation with crop, country, continent, method
--     group, source and number of activities.
--     product_group is derived from the CPC code prefix (check it with the
--     check_product_groups query).
--     time_period is used for like-for-like comparisons.
-- ---------------------------------------------------------------------------
CREATE VIEW v_obs AS
WITH base AS (
    SELECT
        o.record_id,
        o.year,
        o.loss_percentage,
        o.m49_code,
        o.method_id,
        o.source_id,
        c.cpc_code,
        c.commodity_name,
        SPLIT_PART(c.cpc_code, '.', 1) AS cpc_int
    FROM observation o
    JOIN commodity c
      ON c.cpc_code = o.cpc_code
),
norm AS (
    SELECT
        b.*,
        CASE WHEN LENGTH(b.cpc_int) < 4
             THEN LPAD(b.cpc_int, 4, '0')
             ELSE b.cpc_int
        END AS cpc_norm
    FROM base b
)
SELECT
    n.record_id,
    n.year,
    n.loss_percentage,
    n.cpc_code,
    n.commodity_name AS commodity,
    CASE
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '011' THEN 'Cereals'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '012' THEN 'Vegetables'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '013' THEN 'Fruits and nuts'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '014' THEN 'Oilseeds'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '015' THEN 'Roots and tubers'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '016' THEN 'Spice and stimulant crops'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '017' THEN 'Pulses'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '018' THEN 'Sugar crops'
        WHEN SUBSTR(n.cpc_norm, 1, 3) = '019' THEN 'Other crops'
        WHEN SUBSTR(n.cpc_norm, 1, 2) = '02' THEN 'Live animals and animal products'
        WHEN SUBSTR(n.cpc_norm, 1, 2) = '21' THEN 'Meat, fish, fruit and vegetable products'
        WHEN SUBSTR(n.cpc_norm, 1, 2) = '22' THEN 'Dairy and egg products'
        WHEN SUBSTR(n.cpc_norm, 1, 2) = '23' THEN 'Grain mill and other food products'
        WHEN SUBSTR(n.cpc_norm, 1, 2) = '24' THEN 'Beverages'
        ELSE 'Other'
    END AS product_group,
    n.m49_code,
    cc.country_name AS country,
    cc.continent,
    COALESCE(m.method_name, 'Not specified') AS method_name,
    CASE
        WHEN m.method_name = 'Modelled Estimates' THEN 'Modelled estimate'
        WHEN m.method_name IS NULL
          OR m.method_name IN ('Expert Opinion', 'No Data Collection Specified')
            THEN 'Expert opinion or unspecified'
        ELSE 'Measured, surveyed or published'
    END AS method_group,
    s.reference AS source_reference,
    COALESCE(ac.n_activities, 0) AS activity_count,
    CASE
        WHEN n.year BETWEEN 2000 AND 2010 THEN '2000-2010'
        WHEN n.year BETWEEN 2011 AND 2021 THEN '2011-2021'
    END AS time_period
FROM norm n
JOIN country_continent cc
  ON cc.m49_code = n.m49_code
LEFT JOIN method m
  ON m.method_id = n.method_id
LEFT JOIN source s
  ON s.source_id = n.source_id
LEFT JOIN (
    SELECT record_id, COUNT(*) AS n_activities
    FROM observation_activity
    GROUP BY record_id
) ac
  ON ac.record_id = n.record_id;

-- ---------------------------------------------------------------------------
-- A5. v_activity_obs: one row per observation x original activity label
-- ---------------------------------------------------------------------------
CREATE VIEW v_activity_obs AS
SELECT
    v.record_id,
    v.year,
    v.loss_percentage,
    v.commodity,
    v.product_group,
    v.country,
    v.continent,
    v.method_group,
    v.activity_count,
    a.activity_name,
    sm.stage_group
FROM v_obs v
JOIN observation_activity oa
  ON oa.record_id = v.record_id
JOIN activity a
  ON a.activity_id = oa.activity_id
LEFT JOIN activity_stage_map sm
  ON sm.activity_name = a.activity_name;

-- ---------------------------------------------------------------------------
-- A6. v_stage: one row per observation x stage group (real stages only).
--     A record tagged with two activities of the same group appears once.
-- ---------------------------------------------------------------------------
CREATE VIEW v_stage AS
SELECT DISTINCT
    record_id,
    year,
    loss_percentage,
    commodity,
    product_group,
    country,
    continent,
    method_group,
    activity_count,
    stage_group
FROM v_activity_obs
WHERE stage_group IS NOT NULL;

-- ---------------------------------------------------------------------------
-- PART B. Dashboard views (one per core dashboard dataset)
-- ---------------------------------------------------------------------------

-- B1. Headline numbers (single row)
CREATE VIEW v_dash_kpis AS
SELECT
    COUNT(*)                               AS total_records,
    COUNT(DISTINCT country)                AS countries,
    COUNT(DISTINCT commodity)              AS commodities,
    COUNT(DISTINCT source_reference)       AS sources,
    MIN(year)                              AS first_year,
    MAX(year)                              AS last_year,
    ROUND(AVG(loss_percentage), 2)         AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct,
    MAX(loss_percentage)                   AS max_loss_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE method_group = 'Modelled estimate') / COUNT(*), 1)
                                           AS pct_modelled_records
FROM v_obs;

-- B2. Yearly profile. A year is reliable when at least 30 countries reported.
CREATE VIEW v_dash_yearly AS
SELECT
    year,
    COUNT(*)                       AS records,
    COUNT(DISTINCT country)        AS countries,
    COUNT(DISTINCT commodity)      AS commodities,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.25) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p25_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.75) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p75_loss_pct,
    (COUNT(DISTINCT country) >= 30) AS is_reliable_year
FROM v_obs
GROUP BY year;

-- B3. Crop profile. meets_min_size = at least 30 records from 5 countries.
CREATE VIEW v_dash_crops AS
SELECT
    commodity,
    product_group,
    COUNT(*)                       AS records,
    COUNT(DISTINCT country)        AS countries,
    MIN(year)                      AS first_year,
    MAX(year)                      AS last_year,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct,
    (COUNT(*) >= 30 AND COUNT(DISTINCT country) >= 5) AS meets_min_size
FROM v_obs
GROUP BY commodity, product_group;

-- B4. Country profile. meets_min_size = at least 30 records.
CREATE VIEW v_dash_country AS
SELECT
    country,
    m49_code,
    continent,
    COUNT(*)                       AS records,
    COUNT(DISTINCT commodity)      AS crops,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    (COUNT(*) >= 30)               AS meets_min_size
FROM v_obs
GROUP BY country, m49_code, continent;

-- B5. Continent profile
CREATE VIEW v_dash_continent AS
SELECT
    continent,
    COUNT(*)                       AS records,
    COUNT(DISTINCT country)        AS countries,
    COUNT(DISTINCT commodity)      AS crops,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct
FROM v_obs
GROUP BY continent;

-- B6. Supply chain stage profile (a record counts once in each of its stages)
CREATE VIEW v_dash_stage AS
SELECT
    stage_group,
    COUNT(*)                       AS records,
    COUNT(DISTINCT country)        AS countries,
    COUNT(DISTINCT commodity)      AS crops,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct
FROM v_stage
GROUP BY stage_group;

-- B7. Data collection method profile
CREATE VIEW v_dash_method AS
SELECT
    method_name,
    method_group,
    COUNT(*)                       AS records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_obs
GROUP BY method_name, method_group;

-- ============================================================================
-- name: kpis
-- page: 1 Overview
-- title: Headline numbers
-- question: How big is the dataset and what is the typical recorded loss?
-- visual: KPI cards
SELECT * FROM v_dash_kpis;

-- ============================================================================
-- name: loss_distribution
-- page: 1 Overview
-- title: Distribution of recorded loss
-- question: How are loss values spread: are most records small, and how many are extreme?
-- visual: Column chart
WITH banded AS (
    SELECT
        CASE
            WHEN loss_percentage < 1  THEN '1. Under 1'
            WHEN loss_percentage < 5  THEN '2. 1 to 5'
            WHEN loss_percentage < 10 THEN '3. 5 to 10'
            WHEN loss_percentage < 20 THEN '4. 10 to 20'
            WHEN loss_percentage < 40 THEN '5. 20 to 40'
            ELSE '6. 40 and above'
        END AS loss_band
    FROM v_obs
)
SELECT
    loss_band,
    COUNT(*) AS records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct
FROM banded
GROUP BY loss_band
ORDER BY loss_band;

-- ============================================================================
-- name: yearly_coverage
-- page: 1 Overview
-- title: Records and countries per year
-- question: In which years is the data rich enough to trust, and where does coverage collapse?
-- visual: Column chart + line (countries)
SELECT * FROM v_dash_yearly ORDER BY year;

-- ============================================================================
-- name: method_summary
-- page: 1 Overview
-- title: Data collection methods
-- question: How was the data collected, and does the method change the loss level?
-- visual: Bar chart
SELECT * FROM v_dash_method ORDER BY records DESC;

-- ============================================================================
-- name: top_crops
-- page: 2 Crops
-- title: Crops with the highest median loss
-- question: Which crops lose the most? (at least 30 records from 5 countries)
-- visual: Horizontal bar chart
SELECT
    RANK() OVER (ORDER BY median_loss_pct DESC) AS crop_rank,
    commodity,
    product_group,
    records,
    countries,
    avg_loss_pct,
    median_loss_pct,
    p90_loss_pct
FROM v_dash_crops
WHERE meets_min_size
ORDER BY crop_rank, commodity
LIMIT 20;

-- ============================================================================
-- name: lowest_loss_crops
-- page: 2 Crops
-- title: Crops with the lowest median loss
-- question: Which well-covered crops lose the least?
-- visual: Horizontal bar chart
SELECT
    RANK() OVER (ORDER BY median_loss_pct ASC) AS crop_rank,
    commodity,
    product_group,
    records,
    countries,
    median_loss_pct
FROM v_dash_crops
WHERE meets_min_size
ORDER BY crop_rank, commodity
LIMIT 15;

-- ============================================================================
-- name: top_crops_measured
-- page: 2 Crops
-- title: Crop ranking using measured data only
-- question: Do the top crops stay on top when modelled estimates and expert opinion are removed?
-- visual: Bar chart with rank shift
WITH measured AS (
    SELECT
        commodity,
        COUNT(*)                AS records,
        COUNT(DISTINCT country) AS countries,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
    FROM v_obs
    WHERE method_group = 'Measured, surveyed or published'
    GROUP BY commodity
    HAVING COUNT(*) >= 20 AND COUNT(DISTINCT country) >= 5
),
all_methods AS (
    SELECT
        commodity,
        RANK() OVER (ORDER BY median_loss_pct DESC) AS rank_all_methods
    FROM v_dash_crops
    WHERE meets_min_size
)
SELECT
    RANK() OVER (ORDER BY m.median_loss_pct DESC) AS rank_measured,
    m.commodity,
    m.records,
    m.countries,
    m.median_loss_pct,
    a.rank_all_methods
FROM measured m
LEFT JOIN all_methods a
  ON a.commodity = m.commodity
ORDER BY rank_measured, m.commodity
LIMIT 20;

-- ============================================================================
-- name: product_group_summary
-- page: 2 Crops
-- title: Loss by product group
-- question: Which product groups (cereals, vegetables, fruits, ...) lose the most?
-- visual: Bar chart
SELECT
    product_group,
    COUNT(DISTINCT commodity) AS crops,
    COUNT(*)                  AS records,
    COUNT(DISTINCT country)   AS countries,
    ROUND(AVG(loss_percentage), 2) AS avg_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct
FROM v_obs
GROUP BY product_group
HAVING COUNT(*) >= 30
ORDER BY median_loss_pct DESC;

-- ============================================================================
-- name: crop_variability
-- page: 2 Crops
-- title: Crops with the most uncertain loss
-- question: For which crops does recorded loss vary the most between records?
-- visual: Range bars (p10 to p90)
SELECT
    commodity,
    COUNT(*)                AS records,
    COUNT(DISTINCT country) AS countries,
    ROUND((percentile_cont(0.1) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p10_loss_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS p90_loss_pct,
    ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY loss_percentage))::numeric
        - (percentile_cont(0.1) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS spread_pp
FROM v_obs
GROUP BY commodity
HAVING COUNT(*) >= 30 AND COUNT(DISTINCT country) >= 5
ORDER BY spread_pp DESC
LIMIT 15;

-- ============================================================================
-- name: modelled_vs_measured_gap
-- page: 2 Crops
-- title: Modelled estimates versus measured data by crop
-- question: For which crops do modelled estimates disagree most with measured data?
-- visual: Dumbbell or grouped bars
WITH per_crop AS (
    SELECT
        commodity,
        COUNT(*) FILTER (WHERE method_group = 'Modelled estimate') AS modelled_records,
        COUNT(*) FILTER (WHERE method_group = 'Measured, surveyed or published') AS measured_records,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage)
               FILTER (WHERE method_group = 'Modelled estimate'))::numeric, 2) AS modelled_median_pct,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage)
               FILTER (WHERE method_group = 'Measured, surveyed or published'))::numeric, 2) AS measured_median_pct
    FROM v_obs
    GROUP BY commodity
)
SELECT
    commodity,
    modelled_records,
    measured_records,
    modelled_median_pct,
    measured_median_pct,
    ROUND(measured_median_pct - modelled_median_pct, 2) AS gap_pp
FROM per_crop
WHERE modelled_records >= 10 AND measured_records >= 10
ORDER BY gap_pp DESC
LIMIT 15;

-- ============================================================================
-- name: yearly_trend
-- page: 3 Trends
-- title: Median loss by reliable year
-- question: How does recorded loss change over the years? (years with 30+ reporting countries)
-- visual: Line chart with p25-p75 band
WITH reliable AS (
    SELECT * FROM v_dash_yearly WHERE is_reliable_year
)
SELECT
    year,
    records,
    countries,
    p25_loss_pct,
    median_loss_pct,
    p75_loss_pct,
    avg_loss_pct,
    ROUND(median_loss_pct - LAG(median_loss_pct) OVER (ORDER BY year), 2) AS change_vs_previous_reliable_year
FROM reliable
ORDER BY year;

-- ============================================================================
-- name: yearly_by_continent
-- page: 3 Trends
-- title: Median loss by continent and year
-- question: Do continents follow the same trend over time? (reliable years, 20+ records)
-- visual: Multi-line chart
WITH reliable AS (
    SELECT year FROM v_dash_yearly WHERE is_reliable_year
)
SELECT
    v.continent,
    v.year,
    COUNT(*)                AS records,
    COUNT(DISTINCT v.country) AS countries,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_obs v
JOIN reliable r
  ON r.year = v.year
GROUP BY v.continent, v.year
HAVING COUNT(*) >= 20
ORDER BY v.continent, v.year;

-- ============================================================================
-- name: like_for_like_crops
-- page: 3 Trends
-- title: Change by crop, 2000-2010 versus 2011-2021
-- question: Which crops improved or got worse when the same countries are compared across periods?
-- visual: Diverging bar chart
WITH pairs AS (
    SELECT
        commodity,
        country,
        AVG(loss_percentage) FILTER (WHERE time_period = '2000-2010') AS early_avg,
        AVG(loss_percentage) FILTER (WHERE time_period = '2011-2021') AS late_avg
    FROM v_obs
    WHERE time_period IS NOT NULL
    GROUP BY commodity, country
    HAVING COUNT(*) FILTER (WHERE time_period = '2000-2010') > 0
       AND COUNT(*) FILTER (WHERE time_period = '2011-2021') > 0
)
SELECT
    commodity,
    COUNT(*)                         AS paired_countries,
    ROUND(AVG(early_avg), 2)         AS avg_2000_2010,
    ROUND(AVG(late_avg), 2)          AS avg_2011_2021,
    ROUND(AVG(late_avg) - AVG(early_avg), 2) AS change_pp,
    ROUND(100.0 * (AVG(late_avg) - AVG(early_avg)) / NULLIF(AVG(early_avg), 0), 1) AS change_pct
FROM pairs
GROUP BY commodity
HAVING COUNT(*) >= 5
ORDER BY change_pp;

-- ============================================================================
-- name: like_for_like_continents
-- page: 3 Trends
-- title: Change by continent, 2000-2010 versus 2011-2021
-- question: Which continents improved or got worse on the same crop-country pairs?
-- visual: Diverging bar chart
WITH pairs AS (
    SELECT
        continent,
        commodity,
        country,
        AVG(loss_percentage) FILTER (WHERE time_period = '2000-2010') AS early_avg,
        AVG(loss_percentage) FILTER (WHERE time_period = '2011-2021') AS late_avg
    FROM v_obs
    WHERE time_period IS NOT NULL
    GROUP BY continent, commodity, country
    HAVING COUNT(*) FILTER (WHERE time_period = '2000-2010') > 0
       AND COUNT(*) FILTER (WHERE time_period = '2011-2021') > 0
)
SELECT
    continent,
    COUNT(*)                         AS paired_crop_country,
    ROUND(AVG(early_avg), 2)         AS avg_2000_2010,
    ROUND(AVG(late_avg), 2)          AS avg_2011_2021,
    ROUND(AVG(late_avg) - AVG(early_avg), 2) AS change_pp
FROM pairs
GROUP BY continent
HAVING COUNT(*) >= 10
ORDER BY change_pp;

-- ============================================================================
-- name: like_for_like_countries
-- page: 3 Trends
-- title: Change by country, 2000-2010 versus 2011-2021
-- question: Which countries improved or got worse on the same crops across periods?
-- visual: Diverging bar chart
WITH pairs AS (
    SELECT
        country,
        commodity,
        AVG(loss_percentage) FILTER (WHERE time_period = '2000-2010') AS early_avg,
        AVG(loss_percentage) FILTER (WHERE time_period = '2011-2021') AS late_avg
    FROM v_obs
    WHERE time_period IS NOT NULL
    GROUP BY country, commodity
    HAVING COUNT(*) FILTER (WHERE time_period = '2000-2010') > 0
       AND COUNT(*) FILTER (WHERE time_period = '2011-2021') > 0
)
SELECT
    country,
    COUNT(*)                         AS paired_crops,
    ROUND(AVG(early_avg), 2)         AS avg_2000_2010,
    ROUND(AVG(late_avg), 2)          AS avg_2011_2021,
    ROUND(AVG(late_avg) - AVG(early_avg), 2) AS change_pp
FROM pairs
GROUP BY country
HAVING COUNT(*) >= 5
ORDER BY change_pp;

-- ============================================================================
-- name: continent_summary
-- page: 4 Geography
-- title: Loss by continent
-- question: Which continents record the highest loss, and how much data backs each one?
-- visual: Bar chart
SELECT * FROM v_dash_continent ORDER BY median_loss_pct DESC;

-- ============================================================================
-- name: country_ranking
-- page: 4 Geography
-- title: Countries ranked by median loss
-- question: Which countries record the highest and lowest loss? (at least 30 records)
-- visual: Map or ranked bar chart
SELECT
    RANK() OVER (ORDER BY median_loss_pct DESC) AS country_rank,
    country,
    m49_code,
    continent,
    records,
    crops,
    avg_loss_pct,
    median_loss_pct
FROM v_dash_country
WHERE meets_min_size
ORDER BY country_rank, country;

-- ============================================================================
-- name: country_crop_hotspots
-- page: 4 Geography
-- title: Country and crop hotspots
-- question: Which country-crop pairs show the highest loss? (at least 5 records)
-- visual: Ranked table
SELECT
    country,
    continent,
    commodity,
    COUNT(*)                       AS records,
    MIN(year)                      AS first_year,
    MAX(year)                      AS last_year,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_obs
GROUP BY country, continent, commodity
HAVING COUNT(*) >= 5
ORDER BY median_loss_pct DESC, records DESC
LIMIT 25;

-- ============================================================================
-- name: top_crops_per_continent
-- page: 4 Geography
-- title: Top 3 crops in each continent
-- question: Which crops lose the most in each continent? (15+ records from 3+ countries)
-- visual: Small multiples or table
WITH per_continent AS (
    SELECT
        continent,
        commodity,
        COUNT(*)                AS records,
        COUNT(DISTINCT country) AS countries,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
    FROM v_obs
    GROUP BY continent, commodity
    HAVING COUNT(*) >= 15 AND COUNT(DISTINCT country) >= 3
),
ranked AS (
    SELECT
        p.*,
        RANK() OVER (PARTITION BY continent ORDER BY median_loss_pct DESC) AS rk
    FROM per_continent p
)
SELECT
    continent,
    rk AS crop_rank,
    commodity,
    records,
    countries,
    median_loss_pct
FROM ranked
WHERE rk <= 3
ORDER BY continent, rk, commodity;

-- ============================================================================
-- name: stage_summary
-- page: 5 Supply chain
-- title: Loss by supply chain stage
-- question: At which stage of the supply chain is the highest loss recorded?
-- visual: Bar chart
SELECT * FROM v_dash_stage ORDER BY median_loss_pct DESC;

-- ============================================================================
-- name: stage_single_activity
-- page: 5 Supply chain
-- title: Stage check on single-activity records
-- question: Is the stage ranking the same when only records with exactly one activity are used?
-- visual: Paired bars
WITH single AS (
    SELECT
        stage_group,
        COUNT(*) AS records_single_activity,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
    FROM v_stage
    WHERE activity_count = 1
    GROUP BY stage_group
)
SELECT
    s.stage_group,
    s.records_single_activity,
    s.median_loss_pct  AS median_single_activity,
    d.median_loss_pct  AS median_all_records,
    ROUND(s.median_loss_pct - d.median_loss_pct, 2) AS difference_pp
FROM single s
JOIN v_dash_stage d
  ON d.stage_group = s.stage_group
ORDER BY s.median_loss_pct DESC;

-- ============================================================================
-- name: top_activities
-- page: 5 Supply chain
-- title: Individual activities with the highest median loss
-- question: Which specific activities (drying, threshing, storage, ...) lose the most? (30+ records)
-- visual: Horizontal bar chart
SELECT
    RANK() OVER (ORDER BY median_loss_pct DESC) AS activity_rank,
    activity_name,
    stage_group,
    records,
    countries,
    median_loss_pct
FROM (
    SELECT
        activity_name,
        stage_group,
        COUNT(*)                AS records,
        COUNT(DISTINCT country) AS countries,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
    FROM v_activity_obs
    WHERE stage_group IS NOT NULL
    GROUP BY activity_name, stage_group
    HAVING COUNT(*) >= 30
) t
ORDER BY activity_rank, activity_name
LIMIT 15;

-- ============================================================================
-- name: stage_by_product_group
-- page: 5 Supply chain
-- title: Stage by product group
-- question: Where in the supply chain does each product group lose the most? (20+ records per cell)
-- visual: Heatmap
SELECT
    product_group,
    stage_group,
    COUNT(*) AS records,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_stage
GROUP BY product_group, stage_group
HAVING COUNT(*) >= 20
ORDER BY product_group, stage_group;

-- ============================================================================
-- name: stage_by_continent
-- page: 5 Supply chain
-- title: Stage by continent
-- question: Does the weakest stage differ between continents? (20+ records per cell)
-- visual: Heatmap
SELECT
    continent,
    stage_group,
    COUNT(*) AS records,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_stage
GROUP BY continent, stage_group
HAVING COUNT(*) >= 20
ORDER BY continent, stage_group;

-- ============================================================================
-- name: crop_stage_hotspots
-- page: 5 Supply chain
-- title: Crop and stage hotspots
-- question: Which crop-stage pairs show the highest loss? (15+ records from 3+ countries)
-- visual: Ranked table
SELECT
    commodity,
    stage_group,
    COUNT(*)                AS records,
    COUNT(DISTINCT country) AS countries,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_stage
GROUP BY commodity, stage_group
HAVING COUNT(*) >= 15 AND COUNT(DISTINCT country) >= 3
ORDER BY median_loss_pct DESC, records DESC
LIMIT 25;

-- ============================================================================
-- name: activity_count_profile
-- page: 5 Supply chain
-- title: How many activities does a record cover?
-- question: Are records tied to one stage, or do many span several activities?
-- visual: Column chart
SELECT
    CASE
        WHEN activity_count = 0 THEN '0 No activity'
        WHEN activity_count = 1 THEN '1 One activity'
        WHEN activity_count = 2 THEN '2 Two activities'
        ELSE '3 Three or more'
    END AS activity_profile,
    COUNT(*) AS records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY loss_percentage))::numeric, 2) AS median_loss_pct
FROM v_obs
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- name: stage_information_quality
-- page: 5 Supply chain
-- title: Records with a specific stage
-- question: How many records carry a real stage, and how many say Whole supply chain or Unknown?
-- visual: KPI cards or stacked bar
SELECT
    (SELECT COUNT(*) FROM v_obs) AS total_records,
    COUNT(DISTINCT record_id) FILTER (WHERE stage_group IS NOT NULL) AS records_with_specific_stage,
    COUNT(DISTINCT record_id) FILTER (WHERE activity_name = 'Whole supply chain') AS records_whole_supply_chain,
    COUNT(DISTINCT record_id) FILTER (WHERE activity_name = 'Unknown') AS records_unknown_stage
FROM v_activity_obs;

-- ============================================================================
-- name: method_mix_by_year
-- page: 6 Data reliability
-- title: Method mix by year
-- question: Is the share of modelled estimates growing over time? (reliable years)
-- visual: Stacked area or 100 percent columns
WITH reliable AS (
    SELECT year FROM v_dash_yearly WHERE is_reliable_year
)
SELECT
    v.year,
    COUNT(*) AS records,
    ROUND(100.0 * COUNT(*) FILTER (WHERE v.method_group = 'Modelled estimate') / COUNT(*), 1) AS pct_modelled,
    ROUND(100.0 * COUNT(*) FILTER (WHERE v.method_group = 'Measured, surveyed or published') / COUNT(*), 1) AS pct_measured,
    ROUND(100.0 * COUNT(*) FILTER (WHERE v.method_group = 'Expert opinion or unspecified') / COUNT(*), 1) AS pct_expert_or_unspecified
FROM v_obs v
JOIN reliable r
  ON r.year = v.year
GROUP BY v.year
ORDER BY v.year;

-- ============================================================================
-- name: source_concentration
-- page: 6 Data reliability
-- title: Most used sources
-- question: How concentrated is the data: do a few sources supply most of the records?
-- visual: Pareto chart
WITH per_source AS (
    SELECT
        COALESCE(source_reference, '(no reference)') AS source_reference,
        COUNT(*) AS records
    FROM v_obs
    GROUP BY COALESCE(source_reference, '(no reference)')
),
ranked AS (
    SELECT
        source_reference,
        records,
        ROW_NUMBER() OVER (ORDER BY records DESC, source_reference) AS source_rank
    FROM per_source
)
SELECT
    source_rank,
    SUBSTR(source_reference, 1, 110) AS source_reference_short,
    records,
    ROUND(100.0 * records / SUM(records) OVER (), 1) AS share_pct,
    ROUND(100.0 * SUM(records) OVER (ORDER BY source_rank) / SUM(records) OVER (), 1) AS cumulative_share_pct
FROM ranked
ORDER BY source_rank
LIMIT 15;

-- ============================================================================
-- name: outlier_records
-- page: 6 Data reliability
-- title: Highest single records
-- question: Which individual records have the highest loss, for data review? (not a ranking of crops)
-- visual: Table
SELECT
    record_id,
    year,
    country,
    commodity,
    loss_percentage,
    method_name,
    activity_count,
    SUBSTR(COALESCE(source_reference, '(no reference)'), 1, 90) AS source_reference_short
FROM v_obs
ORDER BY loss_percentage DESC, record_id
LIMIT 20;

-- ============================================================================
-- name: data_quality_checks
-- page: 6 Data reliability
-- title: Analysis data checks
-- question: Is anything dropped or unmapped before the analysis starts?
-- visual: Table
SELECT 'Countries without a continent (dropped from analysis)' AS check_name, COUNT(*) AS n
FROM country c
LEFT JOIN country_continent cc ON cc.m49_code = c.m49_code
WHERE cc.m49_code IS NULL
UNION ALL
SELECT 'Activities without a mapping row', COUNT(*)
FROM activity a
LEFT JOIN activity_stage_map sm ON sm.activity_name = a.activity_name
WHERE sm.activity_name IS NULL
UNION ALL
SELECT 'Observations missing from the analysis view', (SELECT COUNT(*) FROM observation) - (SELECT COUNT(*) FROM v_obs)
UNION ALL
SELECT 'Observations with no method', COUNT(*) FROM observation WHERE method_id IS NULL
UNION ALL
SELECT 'Records in product group Other', COUNT(*) FROM v_obs WHERE product_group = 'Other'
UNION ALL
SELECT 'Records outside both comparison periods', COUNT(*) FROM v_obs WHERE time_period IS NULL;

-- ============================================================================
-- name: check_product_groups
-- page: 6 Data reliability
-- title: Product group mapping check
-- question: Does the CPC-based product grouping look right?
-- visual: Table
SELECT
    product_group,
    COUNT(DISTINCT commodity) AS crops,
    COUNT(*)                  AS records,
    MIN(commodity)            AS first_crop_alphabetically,
    MAX(commodity)            AS last_crop_alphabetically
FROM v_obs
GROUP BY product_group
ORDER BY crops DESC;