-- ============================================================================
-- 00_setup_reference_and_raw.sql
-- Run ONCE after food_waste_queries.sql (the schema/ETL script).
-- Creates the lookup tables used by food_waste_dql.sql and a raw staging table
-- that keeps the original CSV (including cause_of_loss, which the cleaned
-- observation table does not carry).
--
-- Run from the repository root:
--   psql -h localhost -p 5433 -U airflow -d food_waste_db -f db/analysis/00_setup_reference_and_raw.sql
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. Country -> continent (UN M49 continental grouping)
--    Needed because the database has no region column. Only the 142 countries
--    present in food_waste.country are mapped. M49 aggregate rows (World,
--    Africa, Asia, ...) are intentionally NOT mapped, so joins exclude them.
--    Source: UN Statistics Division, Standard country or area codes (M49).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS food_waste.country_continent (
    m49_code    VARCHAR(3)  PRIMARY KEY REFERENCES food_waste.country(m49_code),
    country_name VARCHAR(150) NOT NULL,
    continent   VARCHAR(30) NOT NULL
);

INSERT INTO food_waste.country_continent (m49_code, country_name, continent)
VALUES
    ('12', 'Algeria', 'Africa'),
    ('24', 'Angola', 'Africa'),
    ('204', 'Benin', 'Africa'),
    ('72', 'Botswana', 'Africa'),
    ('854', 'Burkina Faso', 'Africa'),
    ('108', 'Burundi', 'Africa'),
    ('120', 'Cameroon', 'Africa'),
    ('148', 'Chad', 'Africa'),
    ('180', 'DR Congo', 'Africa'),
    ('818', 'Egypt', 'Africa'),
    ('232', 'Eritrea', 'Africa'),
    ('748', 'Eswatini', 'Africa'),
    ('231', 'Ethiopia', 'Africa'),
    ('266', 'Gabon', 'Africa'),
    ('270', 'Gambia', 'Africa'),
    ('288', 'Ghana', 'Africa'),
    ('324', 'Guinea', 'Africa'),
    ('624', 'Guinea-Bissau', 'Africa'),
    ('384', 'Ivory Coast', 'Africa'),
    ('404', 'Kenya', 'Africa'),
    ('426', 'Lesotho', 'Africa'),
    ('430', 'Liberia', 'Africa'),
    ('450', 'Madagascar', 'Africa'),
    ('454', 'Malawi', 'Africa'),
    ('466', 'Mali', 'Africa'),
    ('478', 'Mauritania', 'Africa'),
    ('480', 'Mauritius', 'Africa'),
    ('504', 'Morocco', 'Africa'),
    ('508', 'Mozambique', 'Africa'),
    ('516', 'Namibia', 'Africa'),
    ('562', 'Niger', 'Africa'),
    ('566', 'Nigeria', 'Africa'),
    ('646', 'Rwanda', 'Africa'),
    ('686', 'Senegal', 'Africa'),
    ('694', 'Sierra Leone', 'Africa'),
    ('706', 'Somalia', 'Africa'),
    ('710', 'South Africa', 'Africa'),
    ('728', 'South Sudan', 'Africa'),
    ('729', 'Sudan', 'Africa'),
    ('834', 'Tanzania', 'Africa'),
    ('768', 'Togo', 'Africa'),
    ('788', 'Tunisia', 'Africa'),
    ('800', 'Uganda', 'Africa'),
    ('894', 'Zambia', 'Africa'),
    ('716', 'Zimbabwe', 'Africa'),
    ('32', 'Argentina', 'Americas'),
    ('68', 'Bolivia', 'Americas'),
    ('76', 'Brazil', 'Americas'),
    ('124', 'Canada', 'Americas'),
    ('152', 'Chile', 'Americas'),
    ('170', 'Colombia', 'Americas'),
    ('188', 'Costa Rica', 'Americas'),
    ('192', 'Cuba', 'Americas'),
    ('218', 'Ecuador', 'Americas'),
    ('222', 'El Salvador', 'Americas'),
    ('320', 'Guatemala', 'Americas'),
    ('328', 'Guyana', 'Americas'),
    ('332', 'Haiti', 'Americas'),
    ('340', 'Honduras', 'Americas'),
    ('388', 'Jamaica', 'Americas'),
    ('484', 'Mexico', 'Americas'),
    ('558', 'Nicaragua', 'Americas'),
    ('591', 'Panama', 'Americas'),
    ('600', 'Paraguay', 'Americas'),
    ('604', 'Peru', 'Americas'),
    ('659', 'Saint Kitts and Nevis', 'Americas'),
    ('662', 'Saint Lucia', 'Americas'),
    ('780', 'Trinidad and Tobago', 'Americas'),
    ('840', 'USA', 'Americas'),
    ('862', 'Venezuela', 'Americas'),
    ('4', 'Afghanistan', 'Asia'),
    ('51', 'Armenia', 'Asia'),
    ('31', 'Azerbaijan', 'Asia'),
    ('48', 'Bahrain', 'Asia'),
    ('50', 'Bangladesh', 'Asia'),
    ('116', 'Cambodia', 'Asia'),
    ('156', 'China', 'Asia'),
    ('268', 'Georgia', 'Asia'),
    ('356', 'India', 'Asia'),
    ('360', 'Indonesia', 'Asia'),
    ('364', 'Iran', 'Asia'),
    ('392', 'Japan', 'Asia'),
    ('400', 'Jordan', 'Asia'),
    ('398', 'Kazakhstan', 'Asia'),
    ('417', 'Kyrgyzstan', 'Asia'),
    ('418', 'Laos', 'Asia'),
    ('422', 'Lebanon', 'Asia'),
    ('458', 'Malaysia', 'Asia'),
    ('496', 'Mongolia', 'Asia'),
    ('104', 'Myanmar', 'Asia'),
    ('524', 'Nepal', 'Asia'),
    ('408', 'North Korea', 'Asia'),
    ('512', 'Oman', 'Asia'),
    ('586', 'Pakistan', 'Asia'),
    ('275', 'Palestine', 'Asia'),
    ('608', 'Philippines', 'Asia'),
    ('682', 'Saudi Arabia', 'Asia'),
    ('144', 'Sri Lanka', 'Asia'),
    ('760', 'Syrian Arab Republic', 'Asia'),
    ('158', 'Taiwan', 'Asia'),
    ('762', 'Tajikistan', 'Asia'),
    ('764', 'Thailand', 'Asia'),
    ('626', 'Timor-Leste', 'Asia'),
    ('792', 'Turkey', 'Asia'),
    ('860', 'Uzbekistan', 'Asia'),
    ('704', 'Viet Nam', 'Asia'),
    ('410', 'South Korea', 'Asia'),
    ('8', 'Albania', 'Europe'),
    ('40', 'Austria', 'Europe'),
    ('112', 'Belarus', 'Europe'),
    ('56', 'Belgium', 'Europe'),
    ('191', 'Croatia', 'Europe'),
    ('208', 'Denmark', 'Europe'),
    ('233', 'Estonia', 'Europe'),
    ('246', 'Finland', 'Europe'),
    ('250', 'France', 'Europe'),
    ('276', 'Germany', 'Europe'),
    ('300', 'Greece', 'Europe'),
    ('348', 'Hungary', 'Europe'),
    ('372', 'Ireland', 'Europe'),
    ('380', 'Italy', 'Europe'),
    ('428', 'Latvia', 'Europe'),
    ('440', 'Lithuania', 'Europe'),
    ('442', 'Luxembourg', 'Europe'),
    ('528', 'Netherlands', 'Europe'),
    ('807', 'North Macedonia', 'Europe'),
    ('578', 'Norway', 'Europe'),
    ('616', 'Poland', 'Europe'),
    ('620', 'Portugal', 'Europe'),
    ('642', 'Romania', 'Europe'),
    ('643', 'Russian Federation', 'Europe'),
    ('688', 'Serbia', 'Europe'),
    ('703', 'Slovakia', 'Europe'),
    ('724', 'Spain', 'Europe'),
    ('752', 'Sweden', 'Europe'),
    ('756', 'Switzerland', 'Europe'),
    ('804', 'Ukraine', 'Europe'),
    ('826', 'UK', 'Europe'),
    ('36', 'Australia', 'Oceania'),
    ('242', 'Fiji', 'Oceania'),
    ('540', 'New Caledonia', 'Oceania'),
    ('554', 'New Zealand', 'Oceania')
ON CONFLICT (m49_code)
DO UPDATE SET country_name = EXCLUDED.country_name,
              continent    = EXCLUDED.continent;

-- ----------------------------------------------------------------------------
-- 2. Activity -> supply chain stage group
--    The database stores 52 split activity labels (food_waste.activity). They
--    are rolled up here into stage groups that follow the project's supply
--    chain vocabulary. NULL stage_group = not a real stage ("Whole supply
--    chain" is a multi-stage aggregate, "Unknown" has no stage information).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS food_waste.activity_stage_map (
    activity_name VARCHAR(250) PRIMARY KEY REFERENCES food_waste.activity(activity_name),
    stage_group   VARCHAR(40)
);

INSERT INTO food_waste.activity_stage_map (activity_name, stage_group)
VALUES
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
ON CONFLICT (activity_name)
DO UPDATE SET stage_group = EXCLUDED.stage_group;

-- ----------------------------------------------------------------------------
-- 3. Raw staging table (original CSV, every column kept as text)
--    Used only for root-cause analysis: cause_of_loss is not carried into the
--    cleaned observation table. Rows are the raw grain (31,465 rows, before
--    the cleaning step removed 301 rows), so they are not a 1:1 match of
--    food_waste.observation and are analysed on their own.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS food_waste.stg_raw_food_waste (
    m49_code TEXT,
    country TEXT,
    region TEXT,
    cpc_code TEXT,
    commodity TEXT,
    year TEXT,
    loss_percentage TEXT,
    loss_percentage_original TEXT,
    loss_quantity TEXT,
    activity TEXT,
    food_supply_stage TEXT,
    treatment TEXT,
    cause_of_loss TEXT,
    sample_size TEXT,
    method_data_collection TEXT,
    reference TEXT,
    url TEXT,
    notes TEXT
);

TRUNCATE food_waste.stg_raw_food_waste;

-- Load the raw CSV (psql meta-command; must be run from the repository root)
\copy food_waste.stg_raw_food_waste FROM 'data-set/World-food-waste.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')


-- ----------------------------------------------------------------------------
-- 4. Analysis views (shared by food_waste_dql.sql so each query stays short)
-- ----------------------------------------------------------------------------

-- One row per observation, with crop, country, continent, method group, source.
DROP VIEW IF EXISTS food_waste.v_observation_analysis;
CREATE VIEW food_waste.v_observation_analysis AS
SELECT
    o.record_id,
    o.year,
    o.loss_percentage,
    c.cpc_code,
    c.commodity_name AS crop,
    cc.country_name,
    cc.continent,
    COALESCE(m.method_name, 'Not specified') AS method,
    CASE
        WHEN m.method_name = 'Modelled Estimates' THEN 'Modelled estimate'
        WHEN m.method_name IN ('Expert Opinion', 'No Data Collection Specified')
          OR m.method_name IS NULL THEN 'Expert opinion or unspecified'
        ELSE 'Measured, surveyed or published'
    END AS method_group,
    src.reference AS source_reference,
    (SELECT COUNT(*)
       FROM food_waste.observation_activity oa
      WHERE oa.record_id = o.record_id) AS stage_count
FROM food_waste.observation o
JOIN food_waste.commodity c
  ON c.cpc_code = o.cpc_code
JOIN food_waste.country_continent cc
  ON cc.m49_code = o.m49_code
LEFT JOIN food_waste.method m
  ON m.method_id = o.method_id
LEFT JOIN food_waste.source src
  ON src.source_id = o.source_id;

-- One row per observation x supply chain stage group (stage groups only).
-- A record tagged with two activities in the same group appears once.
DROP VIEW IF EXISTS food_waste.v_stage_observation;
CREATE VIEW food_waste.v_stage_observation AS
SELECT DISTINCT
    va.record_id,
    va.year,
    va.loss_percentage,
    va.crop,
    va.country_name,
    va.continent,
    va.method_group,
    va.stage_count,
    a.activity_name,
    sm.stage_group
FROM food_waste.v_observation_analysis va
JOIN food_waste.observation_activity oa
  ON oa.record_id = va.record_id
JOIN food_waste.activity a
  ON a.activity_id = oa.activity_id
JOIN food_waste.activity_stage_map sm
  ON sm.activity_name = a.activity_name
WHERE sm.stage_group IS NOT NULL;

-- Root-cause rows from the raw CSV (cause_of_loss is not in the cleaned table).
-- Raw grain: one row per raw CSV row that has a cause text, countries only.
DROP VIEW IF EXISTS food_waste.v_cause_of_loss;
CREATE VIEW food_waste.v_cause_of_loss AS
SELECT
    x.country_name,
    x.continent,
    x.crop,
    x.year,
    x.loss_percentage,
    x.food_supply_stage,
    x.method,
    x.cause_text,
    CASE
        WHEN x.cause_lc LIKE '%insect%' OR x.cause_lc LIKE '%pest%' OR x.cause_lc LIKE '%rodent%'
          OR x.cause_lc LIKE '% rat %' OR x.cause_lc LIKE '% rat,%' OR x.cause_lc LIKE '% rats%'
          OR x.cause_lc LIKE '%weevil%' OR x.cause_lc LIKE '%borer%' OR x.cause_lc LIKE '%bruchid%'
          OR x.cause_lc LIKE '%bird%' OR x.cause_lc LIKE '%animal%' OR x.cause_lc LIKE '%wildlife%'
          OR x.cause_lc LIKE '%predator%' OR x.cause_lc LIKE '%fruit fl%' OR x.cause_lc LIKE '%locust%'
            THEN 'Pests and animals'
        WHEN x.cause_lc LIKE '%decay%' OR x.cause_lc LIKE '% rot%' OR x.cause_lc LIKE 'rot%'
          OR x.cause_lc LIKE '%rot/%' OR x.cause_lc LIKE '%rott%' OR x.cause_lc LIKE '%mould%'
          OR x.cause_lc LIKE '%mold%' OR x.cause_lc LIKE '%fung%' OR x.cause_lc LIKE '%bacter%'
          OR x.cause_lc LIKE '%disease%' OR x.cause_lc LIKE '%mycotox%' OR x.cause_lc LIKE '%spoil%'
          OR x.cause_lc LIKE '%ergot%'
            THEN 'Disease and spoilage'
        WHEN x.cause_lc LIKE '%mechanic%' OR x.cause_lc LIKE '%physical%' OR x.cause_lc LIKE '%damage%'
          OR x.cause_lc LIKE '%bruis%' OR x.cause_lc LIKE '%crush%' OR x.cause_lc LIKE '%spill%'
          OR x.cause_lc LIKE '%breakage%' OR x.cause_lc LIKE '%tramp%' OR x.cause_lc LIKE '%dropped%'
            THEN 'Mechanical and physical damage'
        WHEN x.cause_lc LIKE '%harvest%' OR x.cause_lc LIKE '%handling%' OR x.cause_lc LIKE '%transport%'
          OR x.cause_lc LIKE '%thresh%' OR x.cause_lc LIKE '%shell%' OR x.cause_lc LIKE '%winnow%'
          OR x.cause_lc LIKE '%loading%' OR x.cause_lc LIKE '%packag%' OR x.cause_lc LIKE '%sorting%'
          OR x.cause_lc LIKE '%grading%' OR x.cause_lc LIKE '%trim%' OR x.cause_lc LIKE '%dry%'
          OR x.cause_lc LIKE '%cool%'
            THEN 'Post-harvest handling and transport'
        WHEN x.cause_lc LIKE '%storage%' OR x.cause_lc LIKE '%store%' OR x.cause_lc LIKE '%cold%'
            THEN 'Storage conditions'
        WHEN x.cause_lc LIKE '%weather%' OR x.cause_lc LIKE '%temperature%' OR x.cause_lc LIKE '%heat%'
          OR x.cause_lc LIKE '% hot%' OR x.cause_lc LIKE '%humid%' OR x.cause_lc LIKE '%moisture%'
          OR x.cause_lc LIKE '%climat%' OR x.cause_lc LIKE '%rain%' OR x.cause_lc LIKE '%dampness%'
          OR x.cause_lc LIKE '%drought%'
            THEN 'Climate and environment'
        WHEN x.cause_lc LIKE '%market%' OR x.cause_lc LIKE '%demand%' OR x.cause_lc LIKE '%supply%'
          OR x.cause_lc LIKE '%price%' OR x.cause_lc LIKE '%quality%' OR x.cause_lc LIKE '%cosmetic%'
          OR x.cause_lc LIKE '%reject%' OR x.cause_lc LIKE '%surplus%' OR x.cause_lc LIKE '%sale%'
          OR x.cause_lc LIKE '%appearance%' OR x.cause_lc LIKE '%size%' OR x.cause_lc LIKE '%export%'
          OR x.cause_lc LIKE '%unmarketable%'
            THEN 'Market, quality and demand'
        ELSE 'Not a specific cause'
    END AS cause_category
FROM (
    SELECT
        cc.country_name,
        cc.continent,
        TRIM(r.commodity) AS crop,
        CAST(TRIM(r.year) AS INTEGER) AS year,
        CAST(TRIM(r.loss_percentage) AS NUMERIC(8,3)) AS loss_percentage,
        NULLIF(TRIM(r.food_supply_stage), '') AS food_supply_stage,
        NULLIF(TRIM(r.method_data_collection), '') AS method,
        TRIM(r.cause_of_loss) AS cause_text,
        LOWER(TRIM(r.cause_of_loss)) AS cause_lc
    FROM food_waste.stg_raw_food_waste r
    JOIN food_waste.country_continent cc
      ON cc.m49_code = TRIM(r.m49_code)
    WHERE TRIM(r.cause_of_loss) <> ''
) x;

COMMIT;

-- Sanity checks
SELECT 'country_continent' AS table_name, COUNT(*) AS row_count FROM food_waste.country_continent
UNION ALL SELECT 'activity_stage_map', COUNT(*) FROM food_waste.activity_stage_map
UNION ALL SELECT 'stg_raw_food_waste', COUNT(*) FROM food_waste.stg_raw_food_waste;

-- Every country in the observation table should have a continent
SELECT COUNT(*) AS countries_without_continent
FROM food_waste.country c
LEFT JOIN food_waste.country_continent cc ON cc.m49_code = c.m49_code
WHERE cc.m49_code IS NULL;

-- Analysis views build without errors (should return the same row counts)
SELECT 'v_observation_analysis' AS view_name, COUNT(*) AS row_count FROM food_waste.v_observation_analysis
UNION ALL SELECT 'v_stage_observation', COUNT(*) FROM food_waste.v_stage_observation
UNION ALL SELECT 'v_cause_of_loss', COUNT(*) FROM food_waste.v_cause_of_loss;

-- Every activity label should be mapped (expect 0 unmapped)
SELECT COUNT(*) AS unmapped_activities
FROM food_waste.activity a
LEFT JOIN food_waste.activity_stage_map m ON m.activity_name = a.activity_name
WHERE m.activity_name IS NULL;
