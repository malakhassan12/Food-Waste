"""
food_waste_processing_main.py  -  Spark job for the Food Waste project

    cleaned CSV  ->  SILVER (typed + validated + enriched)  ->  GOLD (dashboard tables)

Put this file in:  waste-food-project/spark/jobs/food_waste_processing_main.py
(inside the Spark container it is /opt/spark/jobs, and ./data is /opt/data)

Run from the project root:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark/jobs/food_waste_processing_main.py

Input files (inside ./data, seen by Spark as /opt/data):
    World-food-waste_cleaned_final.csv       one row per record
    World-food-waste_activity_long.csv       record_id, activity (one row per activity)
    lookup/country_continent.csv             m49_code, country_name, continent
    lookup/activity_stage_map.csv            activity_name, stage_group

Output (inside ./data/processed_by_spark):
    silver/observation, silver/observation_activity     parquet
    gold/<table>/part-*.csv                             one folder per dashboard table
    dashboard/food_waste_dashboard.csv                  ONE flat file for the dashboard
    (optional) PostgreSQL tables gold_<table>           when WRITE_TO_POSTGRES=1
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ----------------------------------------------------------------------------
# Config (paths are inside the Spark container)
# ----------------------------------------------------------------------------
DATA = os.getenv("DATA_DIR", "/opt/data")
FOOD_CSV = f"{DATA}/World-food-waste_cleaned_final.csv"
ACT_CSV = f"{DATA}/World-food-waste_activity_long.csv"
CONT_CSV = f"{DATA}/lookup/country_continent.csv"
STAGE_CSV = f"{DATA}/lookup/activity_stage_map.csv"
OUT = f"{DATA}/processed_by_spark"

WRITE_TO_POSTGRES = os.getenv("WRITE_TO_POSTGRES", "0") == "1"
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/food_waste_db")
PG_PROPS = {
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASSWORD", "airflow"),
    "driver": "org.postgresql.Driver",
}

MIN_COUNTRIES_RELIABLE_YEAR = 30

spark = SparkSession.builder.appName("food-waste-processing").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")  # tiny data: default 200 is wasteful
spark.sparkContext.setLogLevel("WARN")


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def read_csv(path):
    """Read every column as string: keeps cpc_code like 0142 and m49_code like 004."""
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def clean_strings(df):
    """Trim every column and turn empty strings into NULL."""
    for c in df.columns:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))
    return df


def pct(p, name):
    """Exact percentile of loss_percentage, rounded to 2 decimals."""
    return F.round(F.expr(f"percentile(loss_percentage, {p})"), 2).alias(name)


def core_stats():
    """The same measures for every grouped table (median ranks things, mean does not)."""
    return [
        F.count("*").alias("records"),
        F.countDistinct("country").alias("countries"),
        F.round(F.avg("loss_percentage"), 2).alias("avg_loss_pct"),
        pct(0.5, "median_loss_pct"),
        pct(0.9, "p90_loss_pct"),
    ]


def save(df, name):
    """Write one gold table as CSV (and optionally to PostgreSQL)."""
    df = df.cache()
    n = df.count()
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/gold/{name}")
    msg = f"   gold/{name:<28} {n:>6} rows"
    if WRITE_TO_POSTGRES:
        df.write.jdbc(url=PG_URL, table=f"gold_{name}", mode="overwrite", properties=PG_PROPS)
        msg += "  -> PostgreSQL"
    print(msg)
    df.unpersist()


def write_single_csv(df, name):
    """Also write ONE file with a fixed name (easier to load in a BI tool than part files)."""
    tmp = f"{OUT}/dashboard/_{name}_tmp"
    final = f"{OUT}/dashboard/{name}.csv"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    try:
        HPath = spark._jvm.org.apache.hadoop.fs.Path
        fs = HPath(tmp).getFileSystem(spark._jsc.hadoopConfiguration())
        part = [st.getPath() for st in fs.listStatus(HPath(tmp)) if st.getPath().getName().startswith("part-")][0]
        fs.delete(HPath(final), False)
        fs.rename(part, HPath(final))
        fs.delete(HPath(tmp), True)
        print(f"   dashboard/{name}.csv written")
    except Exception as err:  # the part file in tmp is still a valid CSV
        print(f"   WARNING: could not build the single file ({err}); use the part file in {tmp}")


# ============================================================================
# KAFKA + POSTGRESQL FUNCTIONS
# ============================================================================


# 1. READ DATA FROM KAFKA
def read_from_kafka(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    )


# 2. SEND DATA TO KAFKA
def send_to_kafka(df, topic):
    (
        df.selectExpr(
            "CAST(record_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )


# 3. READ DATA FROM POSTGRESQL
def read_from_db(table):
    return spark.read.jdbc(
        url=PG_URL,
        table=table,
        properties=PG_PROPS
    )


# 4. SEND DATA TO POSTGRESQL
def send_to_db(df, table):
    (
        df.write
        .jdbc(
            url=PG_URL,
            table=table,
            mode="append",
            properties=PG_PROPS
        )
    )


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

# # Read reference data from PostgreSQL
# country_lookup = read_from_db("food_waste.country_lookup")

# # Read incoming records from Kafka
# kafka_data = read_from_kafka("food-waste-cleaned")

# # After processing, send results to Kafka and PostgreSQL
# # Use these inside foreachBatch for streaming data:
# #
# # def process_batch(batch_df, batch_id):
# #     processed_df = your_processing_logic(batch_df)
# #     send_to_kafka(processed_df, "food-waste-processed")
# #     send_to_db(processed_df, "analytics.food_loss_observations")



# ----------------------------------------------------------------------------
# 1. READ + TYPE + VALIDATE
# ----------------------------------------------------------------------------
print("[1/5] Reading and validating...")
raw = clean_strings(read_csv(FOOD_CSV))
n_raw = raw.count()

typed = (
    raw.withColumn("record_id", F.col("record_id").cast("int"))
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("loss_percentage", F.col("loss_percentage").cast("double"))
    .withColumn("activity_count", F.col("activity_count").cast("int"))
)

# coalesce(..., False): a NULL test result must count as invalid, not vanish from both sides
is_valid = F.coalesce(
    F.col("record_id").isNotNull()
    & F.col("year").isNotNull()
    & F.col("m49_code").isNotNull()
    & F.col("cpc_code").isNotNull()
    & F.col("loss_percentage").between(0, 100),
    F.lit(False),
)


rejected = typed.filter(~is_valid)
valid = typed.filter(is_valid)

n_rejected = rejected.count()

print(f"   rows read: {n_raw}   rejected: {n_rejected}")
if n_rejected:
    rejected.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/rejected")

# groupBy keeps NULL keys in Spark, but explicit labels are clearer on a dashboard
valid = (
    valid.withColumn(
        "method_data_collection",
        F.coalesce(F.col("method_data_collection"), F.lit("No Data Collection Specified")),
    )
    .withColumn("activity", F.coalesce(F.col("activity"), F.lit("Unknown")))
)

# ----------------------------------------------------------------------------
# 2. SILVER: enrich with the columns the dashboard questions need
# ----------------------------------------------------------------------------
print("[2/5] Building silver...")
continent_lookup = clean_strings(read_csv(CONT_CSV)).select(
    F.regexp_replace("m49_code", r"^0+", "").alias("m49_key"), F.col("continent")
)

cpc_int = F.split(F.col("cpc_code"), r"\.").getItem(0)
cpc_norm = F.when(F.length(cpc_int) < 4, F.lpad(cpc_int, 4, "0")).otherwise(cpc_int)
c3 = F.substring(cpc_norm, 1, 3)
c2 = F.substring(cpc_norm, 1, 2)

enriched = (
    valid.withColumn("m49_key", F.regexp_replace("m49_code", r"^0+", ""))
    .join(F.broadcast(continent_lookup), "m49_key", "left")
    # product group from the CPC prefix (same rules as food_waste_analysis.sql)
    .withColumn(
        "product_group",
        F.when(c3 == "011", "Cereals")
        .when(c3 == "012", "Vegetables")
        .when(c3 == "013", "Fruits and nuts")
        .when(c3 == "014", "Oilseeds")
        .when(c3 == "015", "Roots and tubers")
        .when(c3 == "016", "Spice and stimulant crops")
        .when(c3 == "017", "Pulses")
        .when(c3 == "018", "Sugar crops")
        .when(c3 == "019", "Other crops")
        .when(c2 == "02", "Live animals and animal products")
        .when(c2 == "21", "Meat, fish, fruit and vegetable products")
        .when(c2 == "22", "Dairy and egg products")
        .when(c2 == "23", "Grain mill and other food products")
        .when(c2 == "24", "Beverages")
        .otherwise("Other"),
    )
    # modelled values must never be mixed silently with measured ones
    .withColumn(
        "method_group",
        F.when(F.col("method_data_collection") == "Modelled Estimates", "Modelled estimate")
        .when(
            F.col("method_data_collection").isin("Expert Opinion", "No Data Collection Specified"),
            "Expert opinion or unspecified",
        )
        .otherwise("Measured, surveyed or published"),
    )
    .withColumn("is_modelled", F.col("method_group") == "Modelled estimate")
    # like-for-like comparison windows
    .withColumn(
        "time_period",
        F.when(F.col("year").between(2000, 2010), "2000-2010").when(
            F.col("year").between(2011, 2021), "2011-2021"
        ),
    )
    .withColumn(
        "loss_band",
        F.when(F.col("loss_percentage") < 1, "1. Under 1")
        .when(F.col("loss_percentage") < 5, "2. 1 to 5")
        .when(F.col("loss_percentage") < 10, "3. 5 to 10")
        .when(F.col("loss_percentage") < 20, "4. 10 to 20")
        .when(F.col("loss_percentage") < 40, "5. 20 to 40")
        .otherwise("6. 40 and above"),
    )
)

n_unmapped = enriched.filter(F.col("continent").isNull()).count()
if n_unmapped:
    print(f"   WARNING: {n_unmapped} rows have no continent and are dropped from the analysis")

silver = enriched.filter(F.col("continent").isNotNull()).select(
    "record_id", "m49_code", "country", "continent",
    "cpc_code", "commodity", "product_group",
    "year", "time_period", "loss_percentage", "loss_band",
    "activity", "activity_count",
    "method_data_collection", "method_group", "is_modelled",
    "reference",
).cache()
n_silver = silver.count()
assert n_silver == n_raw - n_rejected - n_unmapped, "rows were lost while building silver"
silver.write.mode("overwrite").parquet(f"{OUT}/silver/observation")
print(f"   silver/observation: {n_silver} rows")

# one row per record x activity, with the supply chain stage
stage_map = clean_strings(read_csv(STAGE_CSV))
activities = (
    clean_strings(read_csv(ACT_CSV))
    .withColumn("record_id", F.col("record_id").cast("int"))
    .join(F.broadcast(stage_map), F.col("activity") == F.col("activity_name"), "left")
    .drop("activity_name")
)
silver_act = activities.join(
    silver.select(
        "record_id", "country", "continent", "commodity", "product_group",
        "year", "loss_percentage", "method_group", "activity_count",
    ),
    "record_id",
    "inner",
).cache()
unmapped_act = (
    silver_act.filter(F.col("stage_group").isNull())
    .filter(~F.col("activity").isin("Whole supply chain", "Unknown"))
    .select("activity").distinct().count()
)
if unmapped_act:
    print(f"   WARNING: {unmapped_act} activity labels have no stage mapping")
silver_act.write.mode("overwrite").parquet(f"{OUT}/silver/observation_activity")
print(f"   silver/observation_activity: {silver_act.count()} rows")

# a record counts once per stage (two activities of one stage must not double count)
stage_obs = silver_act.filter(F.col("stage_group").isNotNull()).dropDuplicates(
    ["record_id", "stage_group"]
)

# ----------------------------------------------------------------------------
# 3. GOLD: one table per dashboard question
# ----------------------------------------------------------------------------
print("[3/5] Building gold tables...")

# --- Overview: how big is the data, what is the typical loss? ---------------
kpis = silver.agg(
    F.count("*").alias("total_records"),
    F.countDistinct("country").alias("countries"),
    F.countDistinct("commodity").alias("commodities"),
    F.countDistinct("reference").alias("sources"),
    F.min("year").alias("first_year"),
    F.max("year").alias("last_year"),
    F.round(F.avg("loss_percentage"), 2).alias("avg_loss_pct"),
    pct(0.5, "median_loss_pct"),
    pct(0.9, "p90_loss_pct"),
    F.max("loss_percentage").alias("max_loss_pct"),
    F.round(100 * F.avg(F.col("is_modelled").cast("int")), 1).alias("pct_modelled_records"),
)
save(kpis, "kpis")

loss_distribution = (
    silver.groupBy("loss_band")
    .agg(F.count("*").alias("records"))
    .withColumn("share_pct", F.round(100 * F.col("records") / n_silver, 1))
    .orderBy("loss_band")
)
save(loss_distribution, "loss_distribution")

# --- Trends: how does loss change over time? --------------------------------
yearly = (
    silver.groupBy("year")
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("country").alias("countries"),
        F.countDistinct("commodity").alias("commodities"),
        F.round(F.avg("loss_percentage"), 2).alias("avg_loss_pct"),
        pct(0.25, "p25_loss_pct"),
        pct(0.5, "median_loss_pct"),
        pct(0.75, "p75_loss_pct"),
    )
    .withColumn("is_reliable_year", F.col("countries") >= MIN_COUNTRIES_RELIABLE_YEAR)
    .orderBy("year")
    .cache()
)
save(yearly, "yearly")

w_year = Window.orderBy("year")
yearly_trend = yearly.filter("is_reliable_year").withColumn(
    "change_vs_previous_reliable_year",
    F.round(F.col("median_loss_pct") - F.lag("median_loss_pct").over(w_year), 2),
)
save(yearly_trend, "yearly_trend")

reliable_years = yearly.filter("is_reliable_year").select("year")

continent_year = (
    silver.join(F.broadcast(reliable_years), "year", "left_semi")
    .groupBy("continent", "year")
    .agg(*core_stats())
    .filter(F.col("records") >= 20)
    .orderBy("continent", "year")
)
save(continent_year, "continent_year")

method_year = (
    silver.join(F.broadcast(reliable_years), "year", "left_semi")
    .groupBy("year")
    .agg(
        F.count("*").alias("records"),
        F.round(100 * F.avg((F.col("method_group") == "Modelled estimate").cast("int")), 1).alias("pct_modelled"),
        F.round(100 * F.avg((F.col("method_group") == "Measured, surveyed or published").cast("int")), 1).alias("pct_measured"),
        F.round(100 * F.avg((F.col("method_group") == "Expert opinion or unspecified").cast("int")), 1).alias("pct_expert_or_unspecified"),
    )
    .orderBy("year")
)
save(method_year, "method_year")


def like_for_like(level, min_pairs):
    """Compare 2000-2010 with 2011-2021 on the SAME country + crop pairs only."""
    pairs = (
        silver.filter(F.col("time_period").isNotNull())
        .groupBy("continent", "country", "commodity")
        .agg(
            F.avg(F.when(F.col("time_period") == "2000-2010", F.col("loss_percentage"))).alias("early_avg"),
            F.avg(F.when(F.col("time_period") == "2011-2021", F.col("loss_percentage"))).alias("late_avg"),
        )
        .filter(F.col("early_avg").isNotNull() & F.col("late_avg").isNotNull())
    )
    return (
        pairs.groupBy(level)
        .agg(F.count("*").alias("paired"), F.avg("early_avg").alias("e"), F.avg("late_avg").alias("l"))
        .filter(F.col("paired") >= min_pairs)
        .select(
            level,
            "paired",
            F.round("e", 2).alias("avg_2000_2010"),
            F.round("l", 2).alias("avg_2011_2021"),
            F.round(F.col("l") - F.col("e"), 2).alias("change_pp"),
            F.round(F.when(F.col("e") != 0, 100 * (F.col("l") - F.col("e")) / F.col("e")), 1).alias("change_pct"),
        )
        .orderBy("change_pp")
    )


save(like_for_like("commodity", 5), "like_for_like_crops")
save(like_for_like("continent", 10), "like_for_like_continents")
save(like_for_like("country", 5), "like_for_like_countries")

# --- Crops: which crops lose the most? --------------------------------------
crops = (
    silver.groupBy("commodity", "product_group")
    .agg(*core_stats(), F.min("year").alias("first_year"), F.max("year").alias("last_year"))
    .withColumn("meets_min_size", (F.col("records") >= 30) & (F.col("countries") >= 5))
)
w_crop = Window.partitionBy("meets_min_size").orderBy(F.desc("median_loss_pct"))
crops = crops.withColumn("crop_rank", F.when(F.col("meets_min_size"), F.rank().over(w_crop)))
save(crops.orderBy(F.desc("meets_min_size"), "crop_rank"), "crops")

product_group = (
    silver.groupBy("product_group")
    .agg(F.countDistinct("commodity").alias("crops"), *core_stats())
    .filter(F.col("records") >= 30)
    .orderBy(F.desc("median_loss_pct"))
)
save(product_group, "product_group")

# --- Geography: where is loss highest? --------------------------------------
country = (
    silver.groupBy("country", "m49_code", "continent")
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("commodity").alias("crops"),
        F.round(F.avg("loss_percentage"), 2).alias("avg_loss_pct"),
        pct(0.5, "median_loss_pct"),
    )
    .withColumn("meets_min_size", F.col("records") >= 30)
)
w_country = Window.partitionBy("meets_min_size").orderBy(F.desc("median_loss_pct"))
country = country.withColumn("country_rank", F.when(F.col("meets_min_size"), F.rank().over(w_country)))
save(country.orderBy(F.desc("meets_min_size"), "country_rank"), "country")

continent = (
    silver.groupBy("continent")
    .agg(F.countDistinct("commodity").alias("crops"), *core_stats())
    .orderBy(F.desc("median_loss_pct"))
)
save(continent, "continent")

country_crop_hotspots = (
    silver.groupBy("country", "continent", "commodity")
    .agg(F.count("*").alias("records"), pct(0.5, "median_loss_pct"))
    .filter(F.col("records") >= 5)
    .orderBy(F.desc("median_loss_pct"), F.desc("records"))
    .limit(25)
)
save(country_crop_hotspots, "country_crop_hotspots")

# --- Supply chain: at which stage is loss highest? --------------------------
stage = (
    stage_obs.groupBy("stage_group")
    .agg(F.countDistinct("commodity").alias("crops"), *core_stats())
    .orderBy(F.desc("median_loss_pct"))
)
save(stage, "stage")

stage_product_group = (
    stage_obs.groupBy("product_group", "stage_group")
    .agg(F.count("*").alias("records"), pct(0.5, "median_loss_pct"))
    .filter(F.col("records") >= 20)
    .orderBy("product_group", "stage_group")
)
save(stage_product_group, "stage_product_group")

stage_continent = (
    stage_obs.groupBy("continent", "stage_group")
    .agg(F.count("*").alias("records"), pct(0.5, "median_loss_pct"))
    .filter(F.col("records") >= 20)
    .orderBy("continent", "stage_group")
)
save(stage_continent, "stage_continent")

top_activities = (
    silver_act.filter(F.col("stage_group").isNotNull())
    .groupBy("activity", "stage_group")
    .agg(F.count("*").alias("records"), F.countDistinct("country").alias("countries"), pct(0.5, "median_loss_pct"))
    .filter(F.col("records") >= 30)
    .orderBy(F.desc("median_loss_pct"))
    .limit(15)
)
save(top_activities, "top_activities")

# --- Data reliability: how much can we trust the numbers? -------------------
method = (
    silver.groupBy("method_data_collection", "method_group")
    .agg(
        F.count("*").alias("records"),
        F.round(F.avg("loss_percentage"), 2).alias("avg_loss_pct"),
        pct(0.5, "median_loss_pct"),
    )
    .withColumn("share_pct", F.round(100 * F.col("records") / n_silver, 1))
    .orderBy(F.desc("records"))
)
save(method, "method")

# --- Detail layer: your grouping (country + crop + year + activity + method) -
# One row = country + commodity + year + activity + method. reference / url are not needed.
group_keys = [
    "m49_code", "country", "cpc_code", "commodity",
    "year", "activity", "method_data_collection",
]
# these columns depend on the keys above, so they do not split any group
derived_keys = ["continent", "product_group", "method_group", "time_period"]

grouped = (
    silver.groupBy(*group_keys, *derived_keys)
    .agg(
        F.round(F.avg("loss_percentage"), 2).alias("loss_percentage"),
        F.round(F.expr("percentile(loss_percentage, 0.5)"), 2).alias("median_loss"),
        F.round(F.min("loss_percentage"), 2).alias("min_loss"),
        F.round(F.max("loss_percentage"), 2).alias("max_loss"),
        F.round(F.sum("loss_percentage"), 4).alias("loss_sum"),  # sum of losses: weighted average = SUM(loss_sum) / SUM(count)
        F.count("*").alias("count"),  # how many rows were merged
    )
    .orderBy(*group_keys)
    .cache()
)

# sanity checks (same ideas as the pandas version)
assert grouped.agg(F.sum("count")).first()[0] == n_silver, "some rows were lost while grouping"
assert grouped.groupBy(*group_keys).count().filter(F.col("count") > 1).count() == 0, "group key is not unique"
n_merged = grouped.filter(F.col("count") > 1).count()
print(f"   grouped: rows before {n_silver}, after {grouped.count()}, merged groups (count > 1): {n_merged}")
save(grouped, "grouped")

# --- Dashboard layer: ONE flat table, every column ready for slicers and charts ---
# Same grain as `grouped` (country + crop + year + activity + method), plus the
# dimensions the dashboard filters on. No joins are needed in the BI tool.
NO_STAGE = "Whole supply chain or unknown"
stage_lookup = stage_map.select(
    F.col("activity_name").alias("_act"), F.col("stage_group").alias("_stage")
)

dashboard = (
    grouped.join(F.broadcast(stage_lookup), F.col("activity") == F.col("_act"), "left")
    .withColumn(
        "stage_group",
        F.when(F.col("_stage").isNotNull(), F.col("_stage"))
        .when(F.col("activity").isin("Whole supply chain", "Unknown"), F.lit(NO_STAGE))
        .otherwise(F.lit("Unmapped activity")),
    )
    .drop("_act", "_stage")
    .join(F.broadcast(yearly.select("year", "is_reliable_year")), "year", "left")
    .withColumn("decade", F.concat((F.floor(F.col("year") / 10) * 10).cast("string"), F.lit("s")))
    .withColumn("is_modelled", F.col("method_group") == "Modelled estimate")
    .withColumn("is_merged_group", F.col("count") > 1)
    .withColumn(
        "loss_band",
        F.when(F.col("loss_percentage") < 1, "1. Under 1")
        .when(F.col("loss_percentage") < 5, "2. 1 to 5")
        .when(F.col("loss_percentage") < 10, "3. 5 to 10")
        .when(F.col("loss_percentage") < 20, "4. 10 to 20")
        .when(F.col("loss_percentage") < 40, "5. 20 to 40")
        .otherwise("6. 40 and above"),
    )
    .select(
        "country", "continent", "m49_code",
        "commodity", "product_group", "cpc_code",
        "year", "decade", "time_period", "is_reliable_year",
        "activity", "stage_group",
        "method_data_collection", "method_group", "is_modelled",
        "loss_percentage", "median_loss", "min_loss", "max_loss", "loss_sum", "count",
        "loss_band", "is_merged_group",
    )
    .orderBy("country", "commodity", "year", "activity")
    .cache()
)

assert dashboard.count() == grouped.count(), "the stage / year joins changed the number of rows"
assert dashboard.agg(F.sum("count")).first()[0] == n_silver, "some rows were lost in the dashboard table"
n_no_stage = dashboard.filter(F.col("stage_group") == "Unmapped activity").count()
if n_no_stage:
    print(f"   WARNING: {n_no_stage} dashboard rows have an activity that is not in activity_stage_map.csv")
save(dashboard, "dashboard_grouped")
write_single_csv(dashboard, "food_waste_dashboard")

# ----------------------------------------------------------------------------
# --- Additional analytical outputs ------------------------------------------
# 1. Top crops using measured observations only.
measured_data = silver.filter(F.col("method_group") == "Measured, surveyed or published")
top_crops_measured = (measured_data.groupBy("commodity", "product_group").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), pct(0.5, "median_loss"), pct(0.9, "p90_loss"), F.countDistinct("country").alias("countries"), F.min("year").alias("first_year"), F.max("year").alias("last_year")).orderBy(F.desc("median_loss"), F.desc("records")))
save(top_crops_measured, "top_crops_measured")

# 2. Crop loss variability.
crop_variability = (silver.groupBy("commodity", "product_group").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), F.round(F.stddev("loss_percentage"), 2).alias("stddev_loss"), F.round(F.min("loss_percentage"), 2).alias("min_loss"), F.round(F.max("loss_percentage"), 2).alias("max_loss"), pct(0.5, "median_loss"), F.round(F.expr("percentile(loss_percentage, 0.75) - percentile(loss_percentage, 0.25)"), 2).alias("iqr_loss"), F.countDistinct("country").alias("countries")).withColumn("loss_range", F.round(F.col("max_loss") - F.col("min_loss"), 2)).orderBy(F.desc("stddev_loss")))
save(crop_variability, "crop_variability")

# 3. Modelled versus measured loss gap.
method_comparison = silver.withColumn("comparison_method", F.when(F.col("method_group") == "Measured, surveyed or published", "Measured").when(F.col("method_group") == "Modelled estimate", "Modelled")).filter(F.col("comparison_method").isNotNull())
modelled_vs_measured_gap = (method_comparison.groupBy("commodity", "product_group", "comparison_method").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), pct(0.5, "median_loss"), F.countDistinct("country").alias("countries")).groupBy("commodity", "product_group").pivot("comparison_method", ["Measured", "Modelled"]).agg(F.first("records").alias("records"), F.first("avg_loss").alias("avg_loss"), F.first("median_loss").alias("median_loss"), F.first("countries").alias("countries")).withColumn("avg_loss_gap", F.round(F.col("Modelled_avg_loss") - F.col("Measured_avg_loss"), 2)).withColumn("median_loss_gap", F.round(F.col("Modelled_median_loss") - F.col("Measured_median_loss"), 2)).orderBy(F.desc(F.abs(F.col("avg_loss_gap")))))
save(modelled_vs_measured_gap, "modelled_vs_measured_gap")

# 4. Top five crops per continent with at least five records.
continent_crop_stats = silver.groupBy("continent", "commodity", "product_group").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), pct(0.5, "median_loss"), F.countDistinct("country").alias("countries")).filter(F.col("records") >= 5)
continent_crop_window = Window.partitionBy("continent").orderBy(F.desc("median_loss"), F.desc("records"))
top_crops_per_continent = continent_crop_stats.withColumn("crop_rank", F.dense_rank().over(continent_crop_window)).filter(F.col("crop_rank") <= 5).orderBy("continent", "crop_rank")
save(top_crops_per_continent, "top_crops_per_continent")

# 5. Stages associated with exactly one activity.
stage_single_activity = (
    silver_act
    .filter(
        F.col("stage_group").isNotNull() &
        F.col("activity").isNotNull()
    )
    .groupBy("stage_group")
    .agg(
        F.countDistinct("activity").alias("activity_count"),

        F.concat_ws(
            ", ",
            F.sort_array(F.collect_set("activity"))
        ).alias("activities"),

        F.countDistinct("record_id").alias("records")
    )
    .filter(F.col("activity_count") == 1)
    .orderBy(F.desc("records"))
)

save(stage_single_activity, "stage_single_activity")

# 6. Crop and supply-chain stage hotspots.
crop_stage_hotspots = (silver.join(silver_act.select("record_id", "stage_group").filter(F.col("stage_group").isNotNull()).distinct(), "record_id", "inner").groupBy("commodity", "product_group", "stage_group").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), pct(0.5, "median_loss"), pct(0.9, "p90_loss"), F.countDistinct("country").alias("countries")).filter(F.col("records") >= 5).orderBy(F.desc("median_loss"), F.desc("records")))
save(crop_stage_hotspots, "crop_stage_hotspots")

# 7. Observation profile by distinct activity count.
activity_counts = silver_act.filter(F.col("activity").isNotNull()).groupBy("record_id").agg(F.countDistinct("activity").alias("activity_count"))
activity_count_profile = (silver.select("record_id").distinct().join(activity_counts, "record_id", "left").fillna({"activity_count": 0}).groupBy("activity_count").agg(F.count("*").alias("observations")).withColumn("percentage", F.round(100 * F.col("observations") / F.sum("observations").over(Window.partitionBy()), 2)).orderBy("activity_count"))
save(activity_count_profile, "activity_count_profile")

# 8. Top five crops per country with at least five records.
country_crop_stats = silver.groupBy("country", "m49_code", "continent", "commodity", "product_group").agg(F.count("*").alias("records"), F.round(F.avg("loss_percentage"), 2).alias("avg_loss"), pct(0.5, "median_loss"), pct(0.9, "p90_loss"), F.min("year").alias("first_year"), F.max("year").alias("last_year")).filter(F.col("records") >= 5)
country_crop_window = Window.partitionBy("country").orderBy(F.desc("median_loss"), F.desc("records"))
top_crops_per_country = country_crop_stats.withColumn("crop_rank", F.dense_rank().over(country_crop_window)).filter(F.col("crop_rank") <= 5).orderBy("country", "crop_rank")
save(top_crops_per_country, "top_crops_per_country")
print("[4/5] Output folders:", f"{OUT}/gold", "and", f"{OUT}/dashboard")
print("[5/5] Done.")
spark.stop()