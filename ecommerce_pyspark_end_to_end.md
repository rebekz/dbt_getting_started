# Tutorial End-to-End: Pipeline Analitika Data E-commerce dengan PySpark

## Pendahuluan: Dari Data Mentah hingga Insight Operasional

Tutorial ini mendemonstrasikan siklus lengkap pembangunan pipeline analitika e-commerce menggunakan **PySpark**. Fokusnya adalah menerjemahkan pola yang ada di `ecommerce_analytics_end_to_end.md` (versi dbt) ke ekosistem Spark dengan tahapan:

1. **Ingestion & Bronze Layer** – Memindahkan data mentah ke storage yang siap diolah
2. **Cleansing & Silver Layer** – Membersihkan serta menormalkan data dengan transformasi PySpark
3. **Aggregation & Gold Layer** – Menyusun metrik bisnis (RFM, CLV, churn risk)
4. **ML Feature Engineering** – Menyiapkan fitur siap latih untuk model prediktif

**Studi Kasus**: Memahami perilaku pelanggan e-commerce untuk:
- Memprediksi customer lifetime value (CLV)
- Mengidentifikasi pelanggan berisiko churn
- Mengoptimalkan strategi retensi dan personalisasi

---

## Arsitektur Pipeline

```
Raw CSV/Parquet         Bronze Tables              Silver Views                  Gold Tables             ML Features
─────────────────       ──────────────             ──────────────               ───────────────          ───────────────
raw_customers  ─────►   bronze.customers   ─────►  silver.stg_customers   ───►  gold.fct_customer_metrics ──►  ml.customer_features
raw_orders     ─────►   bronze.orders      ─────►  silver.stg_orders      ───►  gold.dim_customer_segments ─┐
raw_order_items─────►   bronze.order_items ─┘     silver.stg_order_items ──►  gold.fct_product_performance │
raw_products   ─────►   bronze.products    ─────►  silver.stg_products    ───►                              └─► downstream ML/BI
```

Pipeline ini cocok dijalankan di lingkungan Spark apa pun (local, EMR, Databricks, Synapse). Tutorial menggunakan mode **local Spark session** agar mudah direplikasi.

---

## Modul 1: Data Wrangling — Fondasi dengan PySpark

### 1.1. Menyiapkan Lingkungan

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.1 delta-spark==3.1.0 pandas great-expectations
```

Struktur direktori minimal:

```
ecommerce_pyspark/
  ├── data_seeds/
  │   ├── raw_customers.csv
  │   ├── raw_orders.csv
  │   ├── raw_order_items.csv
  │   └── raw_products.csv
  ├── notebooks/
  └── src/
      └── pipeline.py
```

### 1.2. Menyalin Seed Data

Gunakan dataset yang identik dengan versi dbt agar hasil akhir dapat dibandingkan langsung.

**data_seeds/raw_customers.csv**
```csv
customer_id,email,first_name,last_name,country,created_at
1,john.doe@email.com,John,Doe,US,2023-01-15
2,jane.smith@email.com,Jane,Smith,UK,2023-02-20
3,bob.jones@email.com,Bob,Jones,US,2023-03-10
4,alice.wilson@email.com,Alice,Wilson,CA,2023-01-25
5,charlie.brown@email.com,Charlie,Brown,US,2023-04-05
```

(Susun file `raw_orders.csv`, `raw_order_items.csv`, dan `raw_products.csv` persis seperti referensi.)

### 1.3. Membuat Bronze Layer

`src/pipeline.py`
```python
from pathlib import Path
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ecommerce-pyspark-pipeline")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

base_path = Path("./lakehouse")
bronze_path = base_path / "bronze"
bronze_path.mkdir(parents=True, exist_ok=True)

def ingest_csv(name: str) -> None:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"data_seeds/raw_{name}.csv")
    )

    (
        df.write
        .mode("overwrite")
        .format("delta")
        .save(str(bronze_path / name))
    )

for dataset in ["customers", "orders", "order_items", "products"]:
    ingest_csv(dataset)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

for dataset in ["customers", "orders", "order_items", "products"]:
    spark.read.format("delta").load(str(bronze_path / dataset)).createOrReplaceTempView(f"bronze_{dataset}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{dataset}
        USING DELTA
        LOCATION '{(bronze_path / dataset).as_posix()}'
    """)
```

Bronze layer menyimpan salinan 1:1 dari data mentah dengan format Delta Lake (bisa diganti Parquet kalau belum memakai Delta).

---

## Modul 2: Silver Layer — Cleansing dan Standarisasi

### 2.1. Transformasi Customers

```python
from pyspark.sql import functions as F

silver_path = base_path / "silver"
silver_path.mkdir(parents=True, exist_ok=True)

stg_customers = (
    spark.table("bronze.customers")
    .select(
        "customer_id",
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.upper(F.trim("country")).alias("country"),
        F.to_date("created_at").alias("customer_created_at"),
        F.when(F.col("email").contains("@"), F.lit(True)).otherwise(F.lit(False)).alias("is_valid_email"),
        F.current_timestamp().alias("spark_loaded_at"),
    )
    .filter("is_valid_email")
)

stg_customers.write.mode("overwrite").format("delta").save(str(silver_path / "stg_customers"))
spark.read.format("delta").load(str(silver_path / "stg_customers")).createOrReplaceTempView("stg_customers")
```

### 2.2. Transformasi Orders, Order Items, Products

```python
stg_orders = (
    spark.table("bronze.orders")
    .select(
        "order_id",
        "customer_id",
        F.to_date("order_date").alias("order_date"),
        F.lower(F.trim("status")).alias("order_status"),
        F.col("shipping_cost").cast("decimal(10,2)").alias("shipping_cost"),
        F.year("order_date").alias("order_year"),
        F.month("order_date").alias("order_month"),
        F.quarter("order_date").alias("order_quarter"),
        F.date_format("order_date", "EEEE").alias("order_day_of_week"),
        F.when(F.col("status") == "completed", F.lit(True)).otherwise(F.lit(False)).alias("is_completed"),
        F.current_timestamp().alias("spark_loaded_at"),
    )
)

stg_order_items = (
    spark.table("bronze.order_items")
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        (F.col("quantity") * F.col("unit_price")).cast("decimal(10,2)").alias("line_total"),
        F.current_timestamp().alias("spark_loaded_at"),
    )
    .filter(F.col("quantity") > 0)
)

stg_products = (
    spark.table("bronze.products")
    .select(
        "product_id",
        F.trim("product_name").alias("product_name"),
        F.initcap(F.trim("category")).alias("category"),
        F.col("cost_price").cast("decimal(10,2)").alias("cost_price"),
        F.col("list_price").cast("decimal(10,2)").alias("list_price"),
        (F.col("list_price") - F.col("cost_price")).cast("decimal(10,2)").alias("profit_margin"),
        (((F.col("list_price") - F.col("cost_price")) / F.col("list_price")) * 100).cast("decimal(5,2)").alias("profit_margin_pct"),
        F.current_timestamp().alias("spark_loaded_at"),
    )
)

for name, df in {
    "stg_orders": stg_orders,
    "stg_order_items": stg_order_items,
    "stg_products": stg_products,
}.items():
    df.write.mode("overwrite").format("delta").save(str(silver_path / name))
    spark.read.format("delta").load(str(silver_path / name)).createOrReplaceTempView(name)
```

Jika ingin memakai Spark SQL, daftar view di atas memudahkan eksekusi query mirip dbt (`spark.sql("SELECT ...")`).

---

## Modul 3: Intermediate Layer — Agregasi Customer & Produk

### 3.1. `int_customer_orders`

```python
int_customer_orders = spark.sql(
    """
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.shipping_cost,
        SUM(oi.line_total) AS order_subtotal,
        SUM(oi.line_total) + o.shipping_cost AS order_total,
        COUNT(oi.order_item_id) AS items_count
    FROM stg_orders o
    LEFT JOIN stg_order_items oi ON o.order_id = oi.order_id
    WHERE o.is_completed = true
    GROUP BY o.order_id, o.customer_id, o.order_date, o.shipping_cost
    """
)

int_customer_orders.write.mode("overwrite").format("delta").save(str(base_path / "intermediate" / "int_customer_orders"))
int_customer_orders.createOrReplaceTempView("int_customer_orders")
```

### 3.2. `int_customer_rfm`

```python
spark.sql("CREATE OR REPLACE TEMP VIEW analysis_date AS SELECT MAX(order_date) AS max_date FROM int_customer_orders")

int_customer_rfm = spark.sql(
    """
    WITH rfm_calc AS (
        SELECT
            co.customer_id,
            DATEDIFF('day', MAX(co.order_date), ad.max_date) AS recency_days,
            COUNT(DISTINCT co.order_id) AS frequency,
            SUM(co.order_total) AS monetary_value,
            AVG(co.order_total) AS avg_order_value,
            MIN(co.order_date) AS first_order_date,
            MAX(co.order_date) AS last_order_date,
            DATEDIFF('day', MIN(co.order_date), MAX(co.order_date)) AS customer_tenure_days
        FROM int_customer_orders co
        CROSS JOIN analysis_date ad
        GROUP BY co.customer_id, ad.max_date
    )

    SELECT
        *,
        CASE
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END AS recency_score,
        CASE
            WHEN frequency >= 5 THEN 5
            WHEN frequency >= 4 THEN 4
            WHEN frequency >= 3 THEN 3
            WHEN frequency >= 2 THEN 2
            ELSE 1
        END AS frequency_score,
        CASE
            WHEN monetary_value >= 500 THEN 5
            WHEN monetary_value >= 300 THEN 4
            WHEN monetary_value >= 200 THEN 3
            WHEN monetary_value >= 100 THEN 2
            ELSE 1
        END AS monetary_score,
        recency_score + frequency_score + monetary_score AS rfm_total_score,
        CONCAT(recency_score, frequency_score, monetary_score) AS rfm_segment_code
    FROM rfm_calc
    """
)

int_customer_rfm.write.mode("overwrite").format("delta").save(str(base_path / "intermediate" / "int_customer_rfm"))
int_customer_rfm.createOrReplaceTempView("int_customer_rfm")
```

### 3.3. `int_product_metrics`

```python
int_product_metrics = spark.sql(
    """
    WITH product_sales AS (
        SELECT
            oi.product_id,
            COUNT(DISTINCT o.order_id) AS orders_count,
            COUNT(DISTINCT o.customer_id) AS customers_count,
            SUM(oi.quantity) AS units_sold,
            SUM(oi.line_total) AS revenue,
            AVG(oi.unit_price) AS avg_selling_price,
            MIN(o.order_date) AS first_sale_date,
            MAX(o.order_date) AS last_sale_date
        FROM stg_order_items oi
        JOIN stg_orders o ON oi.order_id = o.order_id
        WHERE o.is_completed = true
        GROUP BY oi.product_id
    )

    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.cost_price,
        p.list_price,
        p.profit_margin,
        p.profit_margin_pct,
        COALESCE(ps.orders_count, 0) AS orders_count,
        COALESCE(ps.customers_count, 0) AS customers_count,
        COALESCE(ps.units_sold, 0) AS units_sold,
        COALESCE(ps.revenue, 0) AS revenue,
        COALESCE(ps.avg_selling_price, 0) AS avg_selling_price,
        COALESCE(ps.revenue - (ps.units_sold * p.cost_price), 0) AS total_profit,
        ps.first_sale_date,
        ps.last_sale_date
    FROM stg_products p
    LEFT JOIN product_sales ps ON p.product_id = ps.product_id
    """
)

int_product_metrics.write.mode("overwrite").format("delta").save(str(base_path / "intermediate" / "int_product_metrics"))
int_product_metrics.createOrReplaceTempView("int_product_metrics")
```

---

## Modul 4: Gold Layer — Data Marts & Business Metrics

### 4.1. `fct_customer_metrics`

```python
fct_customer_metrics = spark.sql(
    """
    SELECT
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.country,
        c.customer_created_at,
        r.recency_days,
        r.frequency,
        r.monetary_value,
        r.avg_order_value,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,
        r.rfm_segment_code,
        r.customer_tenure_days,
        r.first_order_date,
        r.last_order_date,
        r.monetary_value AS current_clv,
        CASE
            WHEN r.rfm_total_score >= 12 THEN r.monetary_value * 2.0
            WHEN r.rfm_total_score >= 9 THEN r.monetary_value * 1.5
            WHEN r.rfm_total_score >= 6 THEN r.monetary_value * 1.2
            ELSE r.monetary_value * 0.8
        END AS predicted_clv_12m,
        CASE
            WHEN r.recency_days > 180 AND r.frequency = 1 THEN 'High'
            WHEN r.recency_days > 120 THEN 'Medium'
            WHEN r.recency_days > 90 THEN 'Low'
            ELSE 'Very Low'
        END AS churn_risk,
        current_timestamp() AS spark_updated_at
    FROM stg_customers c
    LEFT JOIN int_customer_rfm r ON c.customer_id = r.customer_id
    """
)

gold_path = base_path / "gold"
gold_path.mkdir(parents=True, exist_ok=True)

fct_customer_metrics.write.mode("overwrite").format("delta").save(str(gold_path / "fct_customer_metrics"))
fct_customer_metrics.createOrReplaceTempView("fct_customer_metrics")
```

### 4.2. `dim_customer_segments`

```python
dim_customer_segments = spark.sql(
    """
    SELECT
        customer_id,
        email,
        first_name,
        last_name,
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Big Spenders'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cant Lose Them'
            WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score <= 2 THEN 'Hibernating'
            WHEN recency_score <= 1 AND frequency_score <= 1 AND monetary_score <= 1 THEN 'Lost'
            ELSE 'Others'
        END AS customer_segment,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_total_score,
        monetary_value,
        frequency AS total_orders,
        avg_order_value,
        predicted_clv_12m,
        churn_risk,
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'VIP treatment, exclusive offers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'Onboarding campaign, product recommendations'
            WHEN recency_score <= 2 AND monetary_score >= 3 THEN 'Win-back campaign, special discount'
            WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Re-engagement or sunset'
            ELSE 'Standard nurture campaign'
        END AS recommended_action
    FROM fct_customer_metrics
    """
)

dim_customer_segments.write.mode("overwrite").format("delta").save(str(gold_path / "dim_customer_segments"))
dim_customer_segments.createOrReplaceTempView("dim_customer_segments")
```

### 4.3. `fct_product_performance`

```python
fct_product_performance = spark.sql(
    """
    WITH percentile_stats AS (
        SELECT
            percentile_cont(0.8) WITHIN GROUP (ORDER BY revenue) AS p80,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY revenue) AS p50,
            percentile_cont(0.2) WITHIN GROUP (ORDER BY revenue) AS p20
        FROM int_product_metrics
    )

    SELECT
        ipm.*,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS revenue_rank,
        ROW_NUMBER() OVER (ORDER BY units_sold DESC) AS units_rank,
        ROW_NUMBER() OVER (ORDER BY total_profit DESC) AS profit_rank,
        revenue / SUM(revenue) OVER (PARTITION BY category) AS category_revenue_share,
        CASE
            WHEN revenue >= p80 THEN 'Star'
            WHEN revenue >= p50 THEN 'Performer'
            WHEN revenue >= p20 THEN 'Average'
            ELSE 'Underperformer'
        END AS performance_tier
    FROM int_product_metrics ipm
    CROSS JOIN percentile_stats
    """
)

fct_product_performance.write.mode("overwrite").format("delta").save(str(gold_path / "fct_product_performance"))
fct_product_performance.createOrReplaceTempView("fct_product_performance")
```

---

## Modul 5: Machine Learning Feature Preparation

### 5.1. `ml_customer_features`

```python
ml_customer_features = spark.sql(
    """
    WITH customer_behavior AS (
        SELECT
            customer_id,
            STDDEV(order_total) AS order_value_volatility,
            MAX(order_total) AS max_order_value,
            MIN(order_total) AS min_order_value,
            AVG(DATEDIFF('day', LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), order_date)) AS avg_days_between_orders,
            CASE
                WHEN COUNT(*) >= 3 THEN
                    SUM(CASE WHEN order_date >= DATEADD('day', -90, CURRENT_DATE) THEN 1 ELSE 0 END) :: DOUBLE /
                    NULLIF(SUM(CASE WHEN order_date >= DATEADD('day', -180, CURRENT_DATE) AND order_date < DATEADD('day', -90, CURRENT_DATE) THEN 1 ELSE 0 END), 0)
                ELSE NULL
            END AS order_trend_ratio
        FROM int_customer_orders
        GROUP BY customer_id
    )

    SELECT
        cm.customer_id,
        CASE WHEN cm.churn_risk IN ('High', 'Medium') THEN 1 ELSE 0 END AS is_churn_risk,
        cm.country,
        DATEDIFF('day', cm.customer_created_at, CURRENT_DATE) AS account_age_days,
        cm.recency_days,
        cm.frequency,
        cm.monetary_value,
        cm.avg_order_value,
        cm.recency_score,
        cm.frequency_score,
        cm.monetary_score,
        cm.rfm_total_score,
        cm.customer_tenure_days,
        CASE WHEN cm.frequency > 0 THEN cm.monetary_value / cm.frequency ELSE 0 END AS revenue_per_order,
        CASE WHEN cm.customer_tenure_days > 0 THEN cm.frequency * 30.0 / cm.customer_tenure_days ELSE 0 END AS orders_per_month,
        cb.order_value_volatility,
        cb.max_order_value,
        cb.min_order_value,
        cb.avg_days_between_orders,
        cb.order_trend_ratio,
        CASE WHEN cm.recency_days <= 30 THEN 1 ELSE 0 END AS is_active_last_30d,
        CASE WHEN cm.recency_days <= 90 THEN 1 ELSE 0 END AS is_active_last_90d,
        cm.predicted_clv_12m,
        CASE WHEN cm.monetary_value > 0 THEN cm.predicted_clv_12m / cm.monetary_value ELSE 0 END AS clv_growth_multiplier,
        CURRENT_TIMESTAMP() AS feature_generated_at
    FROM fct_customer_metrics cm
    LEFT JOIN customer_behavior cb ON cm.customer_id = cb.customer_id
    """
)

ml_path = base_path / "ml"
ml_path.mkdir(parents=True, exist_ok=True)

ml_customer_features.write.mode("overwrite").format("delta").save(str(ml_path / "ml_customer_features"))
ml_customer_features.createOrReplaceTempView("ml_customer_features")
```

### 5.2. Export ke Pandas untuk Modeling

```python
feature_pdf = ml_customer_features.toPandas()
feature_pdf.to_parquet("artifacts/ml_customer_features.parquet", index=False)
```

Dataset ini siap dipakai pipeline ML (scikit-learn, XGBoost, Databricks AutoML).

---

## Modul 6: Analisis dan Visualisasi

Gunakan Spark SQL untuk eksplorasi lalu tarik ke Pandas/Seaborn untuk plotting.

```python
segment_df = spark.sql(
    """
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        SUM(monetary_value) AS total_revenue,
        AVG(monetary_value) AS avg_customer_value,
        AVG(predicted_clv_12m) AS avg_predicted_clv,
        SUM(predicted_clv_12m) AS total_predicted_value
    FROM dim_customer_segments
    GROUP BY customer_segment
    ORDER BY total_revenue DESC
    """
)

segment_pdf = segment_df.toPandas()

import seaborn as sns
import matplotlib.pyplot as plt

sns.barplot(data=segment_pdf, x="customer_segment", y="total_revenue")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("artifacts/segment_revenue.png", dpi=150)
```

Query tambahan:

```sql
-- Churn risk distribution by segment
SELECT
    customer_segment,
    churn_risk,
    COUNT(*) AS customer_count,
    AVG(monetary_value) AS avg_value,
    AVG(recency_days) AS avg_recency
FROM dim_customer_segments
GROUP BY 1, 2
ORDER BY 1, 2;

-- Top products
SELECT
    performance_tier,
    category,
    product_name,
    revenue,
    total_profit,
    profit_margin_pct,
    units_sold
FROM fct_product_performance
WHERE performance_tier IN ('Star', 'Performer')
ORDER BY revenue DESC
LIMIT 20;

-- RFM heatmap data
SELECT
    recency_score,
    frequency_score,
    COUNT(*) AS customer_count,
    AVG(monetary_value) AS avg_value
FROM fct_customer_metrics
GROUP BY recency_score, frequency_score
ORDER BY recency_score, frequency_score;
```

---

## Modul 7: Data Quality & Testing

### 7.1. Validasi dengan Great Expectations (Opsional)

```python
from great_expectations.dataset import SparkDFDataset

ge_dataset = SparkDFDataset(fct_customer_metrics)
ge_dataset.expect_column_values_to_not_be_null("customer_id")
ge_dataset.expect_column_values_to_be_between("predicted_clv_12m", min_value=0)
ge_dataset.expect_column_values_to_be_between("recency_score", min_value=1, max_value=5)
results = ge_dataset.validate()
assert results["success"], "Data quality checks failed"
```

### 7.2. Custom Assertion di PySpark

```python
negative_clv = fct_customer_metrics.filter("predicted_clv_12m < 0").count()
assert negative_clv == 0, "Found negative predicted CLV"

invalid_rfm = fct_customer_metrics.filter(
    (F.col("recency_score") < 1) | (F.col("recency_score") > 5) |
    (F.col("frequency_score") < 1) | (F.col("frequency_score") > 5) |
    (F.col("monetary_score") < 1) | (F.col("monetary_score") > 5)
).count()
assert invalid_rfm == 0, "RFM scores out of range"
```

---

## Modul 8: Orkestrasi & Deploy

### 8.1. Menjalankan Pipeline

```bash
# Jalankan seluruh pipeline lokal
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/pipeline.py

# Menjalankan hanya modul ML (mengandalkan output Gold layer)
spark-submit src/jobs/build_ml_features.py
```

Pastikan `build_ml_features.py` memuat ulang tabel `gold` sebelum menulis ke layer ML.

### 8.2. Airflow DAG Contoh

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pyspark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    bronze = SparkSubmitOperator(
        task_id="bronze_ingestion",
        application="src/jobs/ingest_bronze.py",
    )

    silver = SparkSubmitOperator(
        task_id="silver_transformations",
        application="src/jobs/build_silver.py",
    )

    gold = SparkSubmitOperator(
        task_id="gold_marts",
        application="src/jobs/build_gold.py",
    )

    ml = SparkSubmitOperator(
        task_id="ml_features",
        application="src/jobs/build_ml_features.py",
    )

    bronze >> silver >> gold >> ml
```

### 8.3. Incremental Load Strategy

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, str(gold_path / "fct_customer_metrics"))

target.alias("target").merge(
    fct_customer_metrics.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

Tambahkan filter `WHERE` berbasis `customer_created_at` atau timestamp lain untuk mengurangi footprint incremental.

---

## Modul 9: Business Use Cases

### 9.1. Campaign Churn Prevention

```sql
SELECT
    customer_id,
    email,
    first_name,
    last_name,
    customer_segment,
    monetary_value,
    recency_days,
    churn_risk,
    recommended_action
FROM dim_customer_segments
WHERE churn_risk IN ('High', 'Medium')
  AND monetary_value > 200
  AND customer_segment IN ('At Risk', 'Cant Lose Them')
ORDER BY monetary_value DESC;
```

### 9.2. Marketing Budget Allocation

```sql
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(predicted_clv_12m) AS total_predicted_value,
    AVG(predicted_clv_12m) AS avg_predicted_clv,
    SUM(predicted_clv_12m) * 0.10 AS suggested_marketing_budget,
    SUM(predicted_clv_12m) * 0.10 / COUNT(*) AS budget_per_customer
FROM dim_customer_segments
GROUP BY customer_segment
ORDER BY total_predicted_value DESC;
```

### 9.3. Produk Recommendation Prep

```sql
WITH customer_product_purchases AS (
    SELECT
        o.customer_id,
        oi.product_id,
        COUNT(*) AS purchase_count,
        SUM(oi.line_total) AS total_spent
    FROM stg_orders o
    JOIN stg_order_items oi ON o.order_id = oi.order_id
    WHERE o.is_completed = true
    GROUP BY o.customer_id, oi.product_id
)
SELECT * FROM customer_product_purchases;
```

Dataset ini dapat diekspor ke Spark MLlib ALS atau rekomendasi berbasis jaringan.

---

## Ringkasan & Best Practices

### Alur Kerja

1. **Bronze Layer** – Snapshot mentah di Delta Lake
2. **Silver Layer** – Transformasi PySpark terstandarisasi, siap analytics
3. **Gold Layer** – Marts bisnis untuk tim BI/Produk
4. **ML Layer** – Fitur terkurasi siap untuk pelatihan model

### Praktik Terbaik

1. **Schema-on-write** dengan Delta Lake untuk audit trail
2. **Notebook + Modular Jobs**: prototipe di notebook, produksi via modul `src/jobs`
3. **Data Contracts**: Validasi dengan Great Expectations / assert PySpark
4. **Orkestrasi**: Gunakan Airflow/Dagster/Databricks Jobs untuk penjadwalan
5. **Monitoring**: Catat metrik kualitas & performa ke observability stack

### Langkah Berikutnya

1. Integrasi pipeline dengan lakehouse production (S3 + Glue Catalog/Unity Catalog)
2. Tambahkan incremental load berbasis watermark untuk tabel besar
3. Sambungkan output Gold/ML ke BI tool (Tableau, PowerBI) dan layanan inferensi ML

---

## Appendix: Spark Command Cheat Sheet

```bash
# Development
pyspark --packages io.delta:delta-spark_2.12:3.1.0
spark-submit --master local[4] src/pipeline.py

# Selective module execution
spark-submit src/jobs/build_silver.py --date 2024-07-01
spark-submit src/jobs/build_gold.py --full-refresh

# Table maintenance
spark.sql "VACUUM gold.fct_customer_metrics RETAIN 168 HOURS"
spark.sql "OPTIMIZE gold.fct_product_performance ZORDER BY (category)"

# Documentation/lineage
python scripts/generate_lineage.py  # gunakan open-source seperti OpenLineage
```

### PySpark Patterns Reference

```python
# Window function
from pyspark.sql import functions as F, Window
Window.partitionBy("customer_id").orderBy(F.col("order_date"))

# Conditional aggregation
F.sum(F.when(F.col("status") == "completed", 1).otherwise(0))

# Handling null division
F.when(F.col("denom") != 0, F.col("num") / F.col("denom")).otherwise(0)

# Delta MERGE incremental
DeltaTable.forName(spark, "gold.fct_customer_metrics").alias("t").merge(...)
```
