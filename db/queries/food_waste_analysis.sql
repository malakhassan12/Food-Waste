-- ============================================================================
-- food_waste_analysis.sql - Lookup tables and shared analysis views
--
-- The Spark job now owns analytical output generation. This file keeps the
-- PostgreSQL setup block that creates country/stage lookup tables and shared
-- observation/dashboard views for any SQL or BI consumers that still need them.
--
-- Run notebook 05 first to create the lookup tables and views in PostgreSQL.
-- Spark consumes observations from food-waste-raw and reads the lookup tables directly through JDBC.
-- The Kafka consumer stores Spark's Silver, Gold, and Dashboard outputs in PostgreSQL.

-- Rules used by the retained shared views
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

SET search_path TO food_waste, public;

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
