"""
Trusted Feature Engineering pipeline for Home Credit (AWS Glue / PySpark).

This script consolidates EDA findings from:
- EDA_train_payments.ipynb
- EDA-pos_cash-credit_card.ipynb
- EDA_bureau_bureau_balance.ipynb

Output datasets are written to trusted/ and refined/ in Parquet format.
"""

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, NumericType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer


# =========================
# Global configuration
# =========================
BUCKET_NAME = "hcdr"
RAW_PATH = "s3://{}/raw/".format(BUCKET_NAME)
REFINED_PATH = "s3://{}/refined/".format(BUCKET_NAME)
# Compatibilidad por nombre; no se utiliza trusted para escritura.
TRUSTED_PATH = REFINED_PATH
GLUE_RAW_DB = "raw_db"
GLUE_TRUSTED_DB = "trusted_db"

# Ordered lookup candidates (first hit wins)
GLUE_TRUSTED_DB_CANDIDATES = [GLUE_TRUSTED_DB, "hcdr_trusted"]
GLUE_RAW_DB_CANDIDATES = [GLUE_RAW_DB]

# Keep False in Glue environments where catalog should be source-of-truth.
ENABLE_S3_FALLBACK = False

TABLES = [
    "application_train",
    "bureau",
    "bureau_balance",
    "previous_application",
    "pos_cash_balance",
    "credit_card_balance",
    "installments_payments",
]

DATASETS = {}
DATASET_PATHS = {}


# =========================
# Spark / Glue initialization
# =========================
try:
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    print("GlueContext inicializado.")
except Exception:
    spark = (
        SparkSession.builder
        .appName("Trusted_FeatureEngineering_HomeCredit_Glue")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "240")
        .enableHiveSupport()
        .getOrCreate()
    )
    print("SparkSession fallback inicializada.")

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "240")


# =========================
# Helper functions
# =========================
def sql(query, rows=20, truncate=False):
    df = spark.sql(query)
    df.show(rows, truncate=truncate)
    return df


def guardar_spark_en_trusted(df, nombre):
    out = REFINED_PATH + nombre.strip("/") + "/"
    if "/trusted/" in out:
        raise ValueError("Bloqueado: salida a trusted detectada: {}".format(out))
    df.write.mode("overwrite").parquet(out)
    print("Guardado refined (redirigido desde trusted):", out)
    DATASETS[nombre] = df
    DATASET_PATHS[nombre] = out
    return df


def guardar_spark_en_refined(df, nombre):
    out = REFINED_PATH + nombre.strip("/") + "/"
    df.write.mode("overwrite").parquet(out)
    print("Guardado refined:", out)
    DATASETS[nombre] = df
    DATASET_PATHS[nombre] = out
    return df


def normalizar_columnas(df):
    new_cols = [c.lower().strip() for c in df.columns]
    return df.toDF(*new_cols)


def table_exists(database, table):
    try:
        return spark.catalog.tableExists("{}.{}".format(database, table))
    except Exception:
        return False


def first_existing_table(table_name, db_candidates):
    for db in db_candidates:
        if table_exists(db, table_name):
            return db
    return None


def cargar_tabla_o_path(nombre_tabla, path_fallback):
    trusted_db = first_existing_table(nombre_tabla, GLUE_TRUSTED_DB_CANDIDATES)
    if trusted_db is not None:
        print("Leyendo {} desde Glue trusted ({}).".format(nombre_tabla, trusted_db))
        return normalizar_columnas(spark.table("{}.{}".format(trusted_db, nombre_tabla)))

    raw_db = first_existing_table(nombre_tabla, GLUE_RAW_DB_CANDIDATES)
    if raw_db is not None:
        print("Leyendo {} desde Glue raw ({}).".format(nombre_tabla, raw_db))
        return normalizar_columnas(spark.table("{}.{}".format(raw_db, nombre_tabla)))

    if not ENABLE_S3_FALLBACK:
        raise ValueError(
            "Tabla {} no encontrada en Glue y ENABLE_S3_FALLBACK=False.".format(nombre_tabla)
        )

    path = path_fallback
    print("Tabla {} no existe en Glue. Fallback S3: {}".format(nombre_tabla, path))
    try:
        df = spark.read.parquet(path)
    except Exception:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    return normalizar_columnas(df)


def get_col(df, col_name, default_value=None, cast_type=None):
    if col_name in df.columns:
        c = F.col(col_name)
    else:
        c = F.lit(default_value)
    if cast_type is not None:
        c = c.cast(cast_type)
    return c


def add_missing_columns(df, required_cols, default_value=None):
    out = df
    for c in required_cols:
        if c not in out.columns:
            out = out.withColumn(c, F.lit(default_value))
    return out


def warn_missing_columns(df, required_cols, table_name):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print("WARNING [{}] columnas faltantes: {}".format(table_name, ", ".join(missing)))


def null_summary(df, top_n=50):
    exprs = []
    for c in df.columns:
        exprs.append(
            F.struct(
                F.lit(c).alias("columna"),
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).cast("double").alias("nulls"),
                F.count(F.lit(1)).cast("double").alias("total")
            ).alias(c)
        )
    stacked = df.select(*exprs)
    exploded = stacked.select(F.explode(F.array(*[F.col(c) for c in df.columns])).alias("x"))
    out = (
        exploded
        .select(
            F.col("x.columna").alias("columna"),
            F.col("x.nulls").alias("nulls"),
            F.col("x.total").alias("total"),
            F.round(F.col("x.nulls") * 100.0 / F.col("x.total"), 4).alias("pct_nulls")
        )
        .orderBy(F.desc("pct_nulls"), F.desc("nulls"))
        .limit(top_n)
    )
    return out


# =========================
# Load base tables
# =========================
dfs = {}
for t in TABLES:
    fallback = RAW_PATH + t + "/"
    try:
        df_t = cargar_tabla_o_path(t, fallback)
        df_t.createOrReplaceTempView(t)
        dfs[t] = df_t
        print("{} -> rows: {:,} | cols: {}".format(t, df_t.count(), len(df_t.columns)))
    except Exception as e:
        print("WARNING: No se pudo cargar tabla {}. Error: {}".format(t, str(e)))


app = dfs.get("application_train")
bureau = dfs.get("bureau")
bureau_balance = dfs.get("bureau_balance")
prev = dfs.get("previous_application")
pos = dfs.get("pos_cash_balance")
cc = dfs.get("credit_card_balance")
inst = dfs.get("installments_payments")


# =========================
# Application base cleaning
# =========================
if app is None:
    raise ValueError("application_train es obligatoria para crear el dataset maestro.")

app = normalizar_columnas(app)
app = add_missing_columns(
    app,
    [
        "sk_id_curr", "target", "days_birth", "days_employed", "amt_credit",
        "amt_income_total", "amt_annuity", "amt_goods_price", "cnt_fam_members", "code_gender"
    ],
    None
)

# Remove badly read headers and invalid target values
app = app.withColumn("target_int", F.col("target").cast("int"))
app = app.filter(F.col("sk_id_curr").cast("string") != F.lit("SK_ID_CURR"))
app = app.filter(F.col("target_int").isin([0, 1]))

app = (
    app
    .withColumn("target", F.col("target_int").cast("int"))
    .drop("target_int")
    .withColumn("age_years", F.abs(F.col("days_birth")) / F.lit(365.0))
    .withColumn("flag_days_employed_anomaly", F.when(F.col("days_employed") == 365243, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("years_employed", F.when(F.col("days_employed") == 365243, None).otherwise(F.abs(F.col("days_employed")) / F.lit(365.0)))
    .withColumn(
        "credit_income_ratio",
        F.when(F.col("amt_income_total") > 0, F.col("amt_credit") / F.col("amt_income_total")).otherwise(None)
    )
    .withColumn(
        "annuity_income_ratio",
        F.when(F.col("amt_income_total") > 0, F.col("amt_annuity") / F.col("amt_income_total")).otherwise(None)
    )
    .withColumn(
        "goods_credit_ratio",
        F.when(F.col("amt_credit") > 0, F.col("amt_goods_price") / F.col("amt_credit")).otherwise(None)
    )
    .withColumn(
        "income_per_family_member",
        F.when(F.col("cnt_fam_members") > 0, F.col("amt_income_total") / F.col("cnt_fam_members")).otherwise(None)
    )
    .withColumn(
        "code_gender",
        F.when(F.upper(F.col("code_gender")) == F.lit("XNA"), F.lit("Unknown"))
        .otherwise(F.coalesce(F.col("code_gender"), F.lit("Unknown")))
    )
)

# Fill null categorical values with Unknown
string_cols = [f.name for f in app.schema.fields if isinstance(f.dataType, StringType)]
if string_cols:
    app = app.fillna("Unknown", subset=string_cols)

# Median imputations for key numeric columns + missing flags
num_cols_critical = [c for c in ["amt_income_total", "amt_credit", "amt_annuity", "amt_goods_price", "cnt_fam_members"] if c in app.columns]
for c in num_cols_critical:
    med = app.approxQuantile(c, [0.5], 0.01)[0]
    if med is not None:
        app = app.withColumn("flag_missing_{}".format(c), F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        app = app.fillna({c: float(med)})

app_base_clean = app
app_base_clean.createOrReplaceTempView("application_base_clean")
guardar_spark_en_trusted(app_base_clean, "application_base_clean")


# =========================
# Bureau aggregation
# =========================
if bureau is not None:
    bureau = normalizar_columnas(bureau)
    bureau = add_missing_columns(
        bureau,
        [
            "sk_id_curr", "credit_active", "days_enddate_fact", "days_credit_enddate", "days_credit",
            "amt_credit_sum", "amt_credit_sum_debt", "amt_credit_sum_overdue", "credit_day_overdue"
        ],
        None
    )
    bureau = bureau.withColumn("credit_active_norm", F.lower(F.coalesce(F.col("credit_active"), F.lit("unknown"))))
    bureau = bureau.withColumn("is_recent_1y", F.when((F.col("days_credit") >= -365) & (F.col("days_credit") <= 0), 1).otherwise(0))
    bureau = bureau.withColumn("missing_days_credit_enddate", F.when(F.col("days_credit_enddate").isNull(), 1).otherwise(0))
    bureau = bureau.withColumn("expected_enddate_past", F.when(F.col("days_credit_enddate") < 0, 1).otherwise(0))
    bureau = bureau.withColumn("expected_enddate_future", F.when(F.col("days_credit_enddate") >= 0, 1).otherwise(0))
    bureau = bureau.withColumn(
        "active_with_enddate_fact",
        F.when((F.col("credit_active_norm") == "active") & F.col("days_enddate_fact").isNotNull(), 1).otherwise(0)
    )
    bureau = bureau.withColumn(
        "active_expected_enddate_past",
        F.when((F.col("credit_active_norm") == "active") & (F.col("days_credit_enddate") < 0), 1).otherwise(0)
    )

    bureau_agg = (
        bureau.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("bureau_num_records"),
            F.sum(F.when(F.col("credit_active_norm") == "active", 1).otherwise(0)).alias("bureau_active_credits"),
            F.sum(F.when(F.col("credit_active_norm") == "closed", 1).otherwise(0)).alias("bureau_closed_credits"),
            F.sum(F.when(F.col("credit_active_norm").isin("bad debt", "bad_debt"), 1).otherwise(0)).alias("bureau_bad_debt_credits"),
            F.round(F.sum(F.coalesce(F.col("amt_credit_sum"), F.lit(0.0))), 4).alias("bureau_total_credit_sum"),
            F.round(F.avg(F.col("amt_credit_sum")), 4).alias("bureau_avg_credit_sum"),
            F.round(F.sum(F.coalesce(F.col("amt_credit_sum_debt"), F.lit(0.0))), 4).alias("bureau_total_debt"),
            F.round(F.sum(F.coalesce(F.col("amt_credit_sum_overdue"), F.lit(0.0))), 4).alias("bureau_total_overdue"),
            F.max(F.coalesce(F.col("credit_day_overdue"), F.lit(0))).alias("bureau_max_days_overdue"),
            F.round(F.avg(F.col("days_credit")), 4).alias("bureau_avg_days_credit"),
            F.sum(F.col("is_recent_1y")).alias("bureau_recent_credits_1y"),
            F.round(F.avg(F.col("is_recent_1y")), 6).alias("bureau_pct_recent_credits_1y"),
            F.sum(F.col("missing_days_credit_enddate")).alias("bureau_missing_days_credit_enddate"),
            F.round(F.avg(F.col("missing_days_credit_enddate")), 6).alias("bureau_pct_missing_days_credit_enddate"),
            F.sum(F.col("expected_enddate_past")).alias("bureau_expected_enddate_past"),
            F.sum(F.col("expected_enddate_future")).alias("bureau_expected_enddate_future"),
            F.max(F.col("active_with_enddate_fact")).alias("flag_active_with_enddate_fact"),
            F.round(F.avg(F.col("active_with_enddate_fact")), 6).alias("pct_active_with_enddate_fact"),
            F.sum(F.col("active_expected_enddate_past")).alias("active_expected_enddate_past_count"),
        )
    )
else:
    print("WARNING: bureau no disponible; se crea agg vacía.")
    bureau_agg = app_base_clean.select("sk_id_curr").distinct()

bureau_agg.createOrReplaceTempView("bureau_agg")
guardar_spark_en_trusted(bureau_agg, "bureau_agg")


# =========================
# Bureau balance aggregation
# =========================
if bureau_balance is not None and bureau is not None:
    bb = normalizar_columnas(bureau_balance)
    bb = add_missing_columns(bb, ["sk_id_bureau", "months_balance", "status"], None)

    b_keys = bureau.select("sk_id_bureau", "sk_id_curr")
    bb_join = bb.join(b_keys, on="sk_id_bureau", how="inner")
    bb_join = bb_join.withColumn("status_norm", F.upper(F.coalesce(F.col("status"), F.lit("X"))))
    bb_join = bb_join.withColumn(
        "status_numeric",
        F.when(F.col("status_norm").isin("0", "1", "2", "3", "4", "5"), F.col("status_norm").cast("int")).otherwise(None)
    )
    bb_join = bb_join.withColumn("is_dpd", F.when(F.col("status_norm").isin("1", "2", "3", "4", "5"), 1).otherwise(0))
    bb_join = bb_join.withColumn("is_closed", F.when(F.col("status_norm") == "C", 1).otherwise(0))
    bb_join = bb_join.withColumn("is_x", F.when(F.col("status_norm") == "X", 1).otherwise(0))

    bb_agg = (
        bb_join.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("bb_total_snapshots"),
            F.countDistinct("months_balance").alias("bb_num_months"),
            F.sum(F.col("is_closed")).alias("bb_num_closed"),
            F.sum(F.when(F.col("status_norm") == "0", 1).otherwise(0)).alias("bb_num_status_0"),
            F.sum(F.col("is_dpd")).alias("bb_num_status_1_5"),
            F.sum(F.when(F.col("status_norm") == "C", 1).otherwise(0)).alias("bb_num_status_c"),
            F.sum(F.when(F.col("status_norm") == "X", 1).otherwise(0)).alias("bb_num_status_x"),
            F.round(F.avg(F.col("is_closed")), 6).alias("bb_pct_closed"),
            F.round(F.avg(F.col("is_dpd")), 6).alias("bb_pct_dpd"),
            F.max(F.col("status_numeric")).alias("bb_max_status_numeric"),
            F.max(F.col("months_balance")).alias("bb_recent_month"),
            F.min(F.col("months_balance")).alias("bb_oldest_month"),
        )
    )
else:
    print("WARNING: bureau_balance o bureau no disponibles; se crea agg vacía.")
    bb_agg = app_base_clean.select("sk_id_curr").distinct()

bb_agg.createOrReplaceTempView("bureau_balance_agg")
guardar_spark_en_trusted(bb_agg, "bureau_balance_agg")


# =========================
# Previous application aggregation
# =========================
if prev is not None:
    prev = normalizar_columnas(prev)
    prev = add_missing_columns(
        prev,
        [
            "sk_id_curr", "name_contract_status", "amt_application", "amt_credit", "amt_annuity",
            "days_decision", "cnt_payment"
        ],
        None
    )
    prev = prev.withColumn("status_norm", F.upper(F.coalesce(F.col("name_contract_status"), F.lit("UNKNOWN"))))
    prev = prev.withColumn(
        "credit_application_ratio",
        F.when(F.col("amt_application") > 0, F.col("amt_credit") / F.col("amt_application")).otherwise(None)
    )

    prev_agg = (
        prev.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("prev_num_applications"),
            F.sum(F.when(F.col("status_norm") == "APPROVED", 1).otherwise(0)).alias("prev_approved_count"),
            F.sum(F.when(F.col("status_norm") == "REFUSED", 1).otherwise(0)).alias("prev_refused_count"),
            F.sum(F.when(F.col("status_norm") == "CANCELED", 1).otherwise(0)).alias("prev_canceled_count"),
            F.sum(F.when(F.col("status_norm") == "UNUSED OFFER", 1).otherwise(0)).alias("prev_unused_offer_count"),
            F.round(F.avg(F.col("amt_application")), 4).alias("prev_avg_amt_application"),
            F.round(F.avg(F.col("amt_credit")), 4).alias("prev_avg_amt_credit"),
            F.round(F.avg(F.col("amt_annuity")), 4).alias("prev_avg_amt_annuity"),
            F.round(F.sum(F.coalesce(F.col("amt_credit"), F.lit(0.0))), 4).alias("prev_total_amt_credit"),
            F.round(F.avg(F.col("days_decision")), 4).alias("prev_avg_days_decision"),
            F.min(F.col("days_decision")).alias("prev_min_days_decision"),
            F.round(F.avg(F.col("credit_application_ratio")), 6).alias("prev_credit_application_ratio"),
            F.round(F.avg(F.col("cnt_payment")), 4).alias("prev_avg_cnt_payment"),
        )
    )

    w_prev_last = Window.partitionBy("sk_id_curr").orderBy(F.col("days_decision").desc_nulls_last())
    prev_last = (
        prev.select("sk_id_curr", "status_norm", "days_decision")
        .withColumn("rn", F.row_number().over(w_prev_last))
        .filter(F.col("rn") == 1)
        .select(F.col("sk_id_curr"), F.col("status_norm").alias("prev_last_status"))
    )
    prev_agg = prev_agg.join(prev_last, on="sk_id_curr", how="left")
else:
    print("WARNING: previous_application no disponible; se crea agg vacía.")
    prev_agg = app_base_clean.select("sk_id_curr").distinct()

prev_agg.createOrReplaceTempView("previous_application_agg")
guardar_spark_en_trusted(prev_agg, "previous_application_agg")


# =========================
# Installments aggregation
# =========================
if inst is not None:
    inst = normalizar_columnas(inst)
    inst = add_missing_columns(
        inst,
        ["sk_id_curr", "days_entry_payment", "days_instalment", "amt_payment", "amt_instalment"],
        None
    )
    inst = (
        inst
        .withColumn("payment_delay", F.col("days_entry_payment") - F.col("days_instalment"))
        .withColumn("payment_ratio", F.when(F.col("amt_instalment") > 0, F.col("amt_payment") / F.col("amt_instalment")).otherwise(None))
        .withColumn("flag_late_payment", F.when((F.col("payment_delay") > 0) & F.col("payment_delay").isNotNull(), 1).otherwise(0))
        .withColumn("flag_underpayment", F.when((F.col("payment_ratio") < 1) & F.col("payment_ratio").isNotNull(), 1).otherwise(0))
    )

    inst_agg = (
        inst.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("inst_num_payments"),
            F.round(F.avg(F.col("payment_delay")), 4).alias("inst_avg_payment_delay"),
            F.max(F.col("payment_delay")).alias("inst_max_payment_delay"),
            F.sum(F.col("flag_late_payment")).alias("inst_late_payment_count"),
            F.round(F.avg(F.col("flag_late_payment")), 6).alias("inst_pct_late_payments"),
            F.round(F.avg(F.col("payment_ratio")), 6).alias("inst_avg_payment_ratio"),
            F.round(F.min(F.col("payment_ratio")), 6).alias("inst_min_payment_ratio"),
            F.sum(F.col("flag_underpayment")).alias("inst_underpayment_count"),
            F.round(F.avg(F.col("flag_underpayment")), 6).alias("inst_pct_underpayment"),
            F.round(F.sum(F.coalesce(F.col("amt_instalment"), F.lit(0.0))), 4).alias("inst_total_instalment"),
            F.round(F.sum(F.coalesce(F.col("amt_payment"), F.lit(0.0))), 4).alias("inst_total_payment"),
        )
    )
else:
    print("WARNING: installments_payments no disponible; se crea agg vacía.")
    inst_agg = app_base_clean.select("sk_id_curr").distinct()

inst_agg.createOrReplaceTempView("installments_agg")
guardar_spark_en_trusted(inst_agg, "installments_agg")


# =========================
# POS CASH aggregation
# =========================
if pos is not None:
    pos = normalizar_columnas(pos)
    pos = add_missing_columns(
        pos,
        ["sk_id_curr", "months_balance", "sk_dpd", "sk_dpd_def", "name_contract_status", "cnt_instalment", "cnt_instalment_future"],
        None
    )
    pos = pos.withColumn("status_norm", F.upper(F.coalesce(F.col("name_contract_status"), F.lit("UNKNOWN"))))
    pos_agg = (
        pos.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("pos_num_records"),
            F.round(F.avg(F.col("months_balance")), 4).alias("pos_avg_months_balance"),
            F.max(F.col("sk_dpd")).alias("pos_max_sk_dpd"),
            F.round(F.avg(F.col("sk_dpd")), 4).alias("pos_avg_sk_dpd"),
            F.max(F.col("sk_dpd_def")).alias("pos_max_sk_dpd_def"),
            F.round(F.avg(F.col("sk_dpd_def")), 4).alias("pos_avg_sk_dpd_def"),
            F.sum(F.when(F.col("status_norm") == "COMPLETED", 1).otherwise(0)).alias("pos_completed_count"),
            F.sum(F.when(F.col("status_norm") == "ACTIVE", 1).otherwise(0)).alias("pos_active_count"),
            F.round(F.avg(F.when(F.col("status_norm") == "COMPLETED", 1).otherwise(0)), 6).alias("pos_pct_completed"),
            F.round(F.avg(F.col("cnt_instalment")), 4).alias("pos_avg_cnt_instalment"),
            F.round(F.avg(F.col("cnt_instalment_future")), 4).alias("pos_avg_cnt_instalment_future"),
        )
    )
else:
    print("WARNING: pos_cash_balance no disponible; se crea agg vacía.")
    pos_agg = app_base_clean.select("sk_id_curr").distinct()

pos_agg.createOrReplaceTempView("pos_cash_agg")
guardar_spark_en_trusted(pos_agg, "pos_cash_agg")


# =========================
# Credit card aggregation
# =========================
if cc is not None:
    cc = normalizar_columnas(cc)
    cc = add_missing_columns(
        cc,
        [
            "sk_id_curr", "amt_balance", "amt_credit_limit_actual", "amt_drawings_current",
            "amt_payment_total_current", "sk_dpd", "sk_dpd_def", "cnt_drawings_current"
        ],
        None
    )
    cc = cc.withColumn("limit_missing_flag", F.when((F.col("amt_credit_limit_actual").isNull()) | (F.col("amt_credit_limit_actual") <= 0), 1).otherwise(0))
    cc = cc.withColumn(
        "cc_util_ratio_row",
        F.when(F.col("amt_credit_limit_actual") > 0, F.col("amt_balance") / F.col("amt_credit_limit_actual")).otherwise(None)
    )

    cc_agg = (
        cc.groupBy("sk_id_curr").agg(
            F.count(F.lit(1)).alias("cc_num_records"),
            F.round(F.avg(F.col("amt_balance")), 4).alias("cc_avg_balance"),
            F.round(F.max(F.col("amt_balance")), 4).alias("cc_max_balance"),
            F.round(F.avg(F.col("amt_credit_limit_actual")), 4).alias("cc_avg_credit_limit"),
            F.round(F.max(F.col("amt_credit_limit_actual")), 4).alias("cc_max_credit_limit"),
            F.round(F.avg(F.col("amt_drawings_current")), 4).alias("cc_avg_drawings"),
            F.round(F.sum(F.coalesce(F.col("amt_drawings_current"), F.lit(0.0))), 4).alias("cc_total_drawings"),
            F.round(F.avg(F.col("amt_payment_total_current")), 4).alias("cc_avg_payment_total"),
            F.round(F.sum(F.coalesce(F.col("amt_payment_total_current"), F.lit(0.0))), 4).alias("cc_total_payment"),
            F.max(F.col("sk_dpd")).alias("cc_max_dpd"),
            F.round(F.avg(F.col("sk_dpd")), 4).alias("cc_avg_dpd"),
            F.max(F.col("sk_dpd_def")).alias("cc_max_dpd_def"),
            F.round(F.avg(F.col("sk_dpd_def")), 4).alias("cc_avg_dpd_def"),
            F.round(F.avg(F.col("cnt_drawings_current")), 4).alias("cc_avg_cnt_drawings"),
            F.round(F.avg(F.col("cc_util_ratio_row")), 6).alias("cc_utilization_ratio"),
            F.sum(F.col("limit_missing_flag")).alias("cc_missing_limit_count"),
        )
    )
else:
    print("WARNING: credit_card_balance no disponible; se crea agg vacía.")
    cc_agg = app_base_clean.select("sk_id_curr").distinct()

cc_agg.createOrReplaceTempView("credit_card_agg")
guardar_spark_en_trusted(cc_agg, "credit_card_agg")


# =========================
# Master dataset
# =========================
master = app_base_clean.alias("a")

join_targets = [
    ("bureau_agg", bureau_agg),
    ("bureau_balance_agg", bb_agg),
    ("previous_application_agg", prev_agg),
    ("installments_agg", inst_agg),
    ("pos_cash_agg", pos_agg),
    ("credit_card_agg", cc_agg),
]

for name, dfi in join_targets:
    cols = [c for c in dfi.columns if c != "target"]
    dfi = dfi.select(*cols).dropDuplicates(["sk_id_curr"])
    master = master.join(dfi.alias(name), on="sk_id_curr", how="left")

master = normalizar_columnas(master)
master.createOrReplaceTempView("master_dataset")
guardar_spark_en_trusted(master, "master_dataset")

# Validation checks
app_rows = app_base_clean.count()
master_rows = master.count()
dup_clients = master.groupBy("sk_id_curr").count().filter(F.col("count") > 1).count()
print("Validacion rows app:", app_rows)
print("Validacion rows master:", master_rows)
print("Clientes duplicados en master:", dup_clients)


# =========================
# ML-ready dataset (SparkML)
# =========================
id_cols = [c for c in ["sk_id_curr", "sk_id_prev", "sk_id_bureau"] if c in master.columns]
label_col = "target"

cat_cols = [
    f.name
    for f in master.schema.fields
    if isinstance(f.dataType, StringType) and f.name not in id_cols + [label_col]
]
num_cols = [
    f.name
    for f in master.schema.fields
    if isinstance(f.dataType, NumericType) and f.name not in id_cols + [label_col]
]

master_ml = master
if cat_cols:
    master_ml = master_ml.fillna("Unknown", subset=cat_cols)

imputer_cols_in = []
for c in num_cols:
    if c in master_ml.columns:
        imputer_cols_in.append(c)

stages = []
if cat_cols:
    indexed_cols = [c + "_idx" for c in cat_cols]
    ohe_cols = [c + "_ohe" for c in cat_cols]
    indexers = [StringIndexer(inputCol=c, outputCol=idx, handleInvalid="keep") for c, idx in zip(cat_cols, indexed_cols)]
    encoder = OneHotEncoder(inputCols=indexed_cols, outputCols=ohe_cols, handleInvalid="keep")
    stages.extend(indexers)
    stages.append(encoder)
else:
    indexed_cols = []
    ohe_cols = []

if imputer_cols_in:
    imputer_out = [c + "_imp" for c in imputer_cols_in]
    imputer = Imputer(strategy="median", inputCols=imputer_cols_in, outputCols=imputer_out)
    stages.append(imputer)
else:
    imputer_out = []

assembler_inputs = imputer_out + ohe_cols
if not assembler_inputs:
    # If no features remain, keep a constant vector
    master_ml = master_ml.withColumn("const_feature", F.lit(1.0))
    assembler_inputs = ["const_feature"]

assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="keep")
stages.append(assembler)

pipeline = Pipeline(stages=stages)
pipeline_model = pipeline.fit(master_ml)
master_ml_ready = pipeline_model.transform(master_ml)
master_ml_ready = (
    master_ml_ready
    .withColumn("label", F.col("target").cast("double"))
    .select("sk_id_curr", "label", "features")
)

master_ml_ready.createOrReplaceTempView("master_dataset_ml_ready")
guardar_spark_en_trusted(master_ml_ready, "master_dataset_ml_ready")


# =========================
# Final validations
# =========================
validation_rows = []
for name, dfv in DATASETS.items():
    try:
        validation_rows.append((name, int(dfv.count()), int(len(dfv.columns)), DATASET_PATHS.get(name, "")))
    except Exception:
        validation_rows.append((name, None, None, DATASET_PATHS.get(name, "")))

validation_trusted_outputs = spark.createDataFrame(
    validation_rows,
    schema="dataset string, row_count long, column_count int, output_path string"
)

validation_extra = spark.createDataFrame(
    [
        ("join_validation", "rows_master_eq_application", float(master_rows == app_rows)),
        ("join_validation", "duplicated_sk_id_curr_master", float(dup_clients)),
        ("metadata", "categorical_columns_count", float(len(cat_cols))),
        ("metadata", "numeric_columns_count", float(len(num_cols))),
        ("metadata", "encoded_columns_count", float(len(ohe_cols))),
    ],
    schema="check_group string, check_name string, check_value double"
)

target_distribution_trusted = (
    master.groupBy("target")
    .agg(
        F.count(F.lit(1)).alias("clientes"),
        F.round(F.count(F.lit(1)) * 100.0 / F.sum(F.count(F.lit(1))).over(Window.partitionBy()), 4).alias("pct_clientes")
    )
)

null_summary_master_dataset = null_summary(master, top_n=80)

guardar_spark_en_refined(validation_trusted_outputs, "validation_trusted_outputs")
guardar_spark_en_refined(target_distribution_trusted, "target_distribution_trusted")
guardar_spark_en_refined(null_summary_master_dataset, "null_summary_master_dataset")
guardar_spark_en_refined(validation_extra, "validation_trusted_outputs_extra")


# =========================
# Track C decision matrix
# =========================
decision_rows = [
    ("application_train", "days_birth", "edad en dias", "age_years = abs(days_birth)/365", "transformar", "trusted/application_base_clean"),
    ("application_train", "days_employed", "valor anomalo 365243", "flag_days_employed_anomaly + years_employed limpio", "flag + limpieza", "trusted/application_base_clean"),
    ("application_train", "code_gender", "categoria XNA", "mapear XNA a Unknown", "normalizar categoria", "trusted/application_base_clean"),
    ("application_train", "amt_credit/amt_income_total", "ratio de capacidad", "credit_income_ratio", "feature derivada", "trusted/application_base_clean"),
    ("bureau", "multiples filas por cliente", "granularidad por credito", "agregacion a nivel sk_id_curr", "consolidar", "trusted/bureau_agg"),
    ("bureau_balance", "snapshots mensuales", "historial de mora temporal", "agregacion bb_pct_dpd y bb_max_status_numeric", "consolidar temporal", "trusted/bureau_balance_agg"),
    ("installments_payments", "pagos esperados vs reales", "senal de atraso y subpago", "payment_delay y payment_ratio + agregados", "feature de comportamiento", "trusted/installments_agg"),
    ("credit_card_balance", "saldo y limite", "uso de credito", "cc_utilization_ratio", "feature de utilizacion", "trusted/credit_card_agg"),
    ("pos_cash_balance", "mora mensual", "dpd historico", "pos_max_sk_dpd_def y tasas de completion", "feature de riesgo", "trusted/pos_cash_agg"),
]

track_c_decision_matrix = spark.createDataFrame(
    decision_rows,
    schema="""
        fuente string,
        variable string,
        hallazgo_eda string,
        transformacion_realizada string,
        decision string,
        salida_trusted string
    """
)

guardar_spark_en_refined(track_c_decision_matrix, "track_c_decision_matrix")

print("Pipeline Trusted Feature Engineering finalizado correctamente.")
