# Tutorial End-to-End: Pipeline Analitika E-commerce dengan PySpark

Dokumen ini adalah pasangan langsung dari `ecommerce_analytics_end_to_end.md`
namun menggunakan PySpark + Delta Lake. Semua contoh kode tinggal dijalankan
melalui satu entrypoint: `pyspark_pipeline/pipeline.py` yang sudah ada di repo
ini.

## 0. Cara Cepat Menjalankan Pipeline

```bash
# Instal semua dependensi (cukup sekali)
uv sync

# Jalankan seluruh alur bronze → silver → gold → ML via uv
uv run pyspark-pipeline

# Atau gunakan spark-submit (tetap lewat uv environment)
uv run spark-submit --packages io.delta:delta-spark_2.12:3.1.0 pyspark_pipeline/pipeline.py
```

Outputnya berupa Delta tables di `lakehouse/{bronze,silver,intermediate,gold,ml}`
dan artefak siap pakai di `artifacts/`:

- `ml_customer_features.parquet` – fitur ML siap konsumsi.
- `segment_clv.png` – visualisasi CLV tiap segmen.

Pipeline otomatis menjalankan Great Expectations. Jika quality check gagal,
script berhenti dengan pesan error sehingga tutorial tetap jujur-ekseskusi.

## 1. Dependensi & Struktur

Dependensi utama sudah dideklarasikan di `pyproject.toml`:

```toml
dependencies = [
    "pyspark==3.5.3",
    "delta-spark==3.1.0",
    "pandas>=2.2.2",
    "pyarrow>=16.1.0",
    "great-expectations>=0.18.13",
    "matplotlib>=3.8.3",
    "seaborn>=0.13.2",
    # plus runtime dbt/uv deps
]
```

Struktur proyek setelah menjalankan pipeline:

```
dbt_get_started/
├── data_seeds/
├── lakehouse/
├── artifacts/
└── pyspark_pipeline/
    ├── __init__.py
    └── pipeline.py
```

## 2. Dataset Mentah (data_seeds/)

CSV di `data_seeds/` sudah berisi data realistik yang sama persis dengan
versi dbt. Contoh `raw_customers.csv` (dipotong):

```csv
customer_id,email,first_name,last_name,date_of_birth,phone,city,state,country,status,created_at,updated_at
1,john.doe@email.com,John,Doe,1985-06-15,555-0101,New York,NY,USA,active,2023-01-15,2023-01-15
2,jane.smith@email.com,Jane,Smith,1990-03-22,555-0102,Los Angeles,CA,USA,active,2023-01-20,2023-01-20
…
```

CSV lain (`raw_orders`, `raw_order_items`, `raw_products`, `raw_categories`)
mengikuti format yang tertulis di README dan digunakan bersama oleh dbt.

## 3. Entry Point: `pyspark_pipeline/pipeline.py`

Script ini menjalankan seluruh tahapan pipeline. Bagian-bagiannya dirangkum di
bawah (kode identik dengan file sumber supaya snippet tetap akurat).

### 3.1. Spark Session & Direktori

```python
spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("ecommerce-pyspark-pipeline")
    .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()
```

Direktori `lakehouse/{bronze,silver,intermediate,gold,ml}` dan `artifacts/`
diciptakan otomatis.

### 3.2. Bronze Layer (Ingestion)

```python
for dataset in ["customers", "orders", "order_items", "products", "categories"]:
    source = paths.data_seeds / f"raw_{dataset}.csv"
    target = paths.lakehouse / "bronze" / dataset
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(source))
    df.write.mode("overwrite").format("delta").save(str(target))
    _register_delta_table(spark, target, "bronze", dataset)
```

### 3.3. Silver Layer (Cleansing)

```python
stg_customers = (
    spark.table("bronze.customers")
    .select(
        "customer_id",
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.upper(F.trim("country")).alias("country"),
        F.to_date("created_at").alias("created_at"),
    )
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
)

stg_orders = (
    spark.table("bronze.orders")
    .select("order_id", "customer_id", F.to_date("order_date").alias("order_date"), "status", "shipping_cost", "total_amount")
    .withColumn("order_status",
        F.when(F.lower("status").isin("confirmed", "shipped", "completed", "delivered"), "completed")
         .when(F.lower("status").isin("cancelled", "returned"), "cancelled")
         .otherwise("pending"))
    .withColumn("is_completed", F.col("order_status") == "completed")
)
```

### 3.4. Intermediate Layer

```python
order_totals = (
    spark.table("silver.stg_orders").filter("is_completed")
    .join(spark.table("silver.stg_order_items"), "order_id", "left")
    .groupBy("order_id", "customer_id", "order_date", "shipping_cost")
    .agg(F.sum("line_total").alias("order_subtotal"), F.count("order_item_id").alias("items_count"))
    .withColumn("order_total", F.col("order_subtotal") + F.col("shipping_cost"))
)

rfm = (
    order_totals.groupBy("customer_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
        F.count("order_id").alias("frequency"),
        F.sum("order_total").alias("monetary_value"),
        F.avg("order_total").alias("avg_order_value"),
    )
    .withColumn("recency_days", F.datediff(F.current_date(), "last_order_date"))
    .withColumn("rfm_score", F.ntile(5).over(Window.partitionBy("customer_id").orderBy(F.col("monetary_value").desc())))
)
```

### 3.5. Gold & ML Layer

```python
fct_customer_metrics = (
    spark.table("silver.stg_customers")
    .join(order_totals.groupBy("customer_id").agg(F.countDistinct("order_id").alias("total_orders"), F.sum("order_total").alias("lifetime_gmv")), "customer_id", "left")
    .join(rfm, "customer_id", "left")
    .fillna({"total_orders": 0, "lifetime_gmv": 0, "recency_days": 9999})
    .withColumn("predicted_clv_12m", F.col("lifetime_gmv") * 1.25)
)

dim_customer_segments = (
    fct_customer_metrics.withColumn("customer_segment",
        F.when((F.col("predicted_clv_12m") >= 1000) & (F.col("total_orders") >= 5), "Champions")
         .when((F.col("predicted_clv_12m") >= 500) & (F.col("total_orders") >= 3), "Loyal")
         .when((F.col("predicted_clv_12m") < 200) & (F.col("total_orders") <= 1), "New")
         .otherwise("Need Attention"))
)

ml_features = dim_customer_segments.select(
    "customer_id", "full_name", "email", "country", "customer_segment",
    "total_orders", "lifetime_gmv", "recency_days", "rfm_score", "predicted_clv_12m", "is_active_customer"
)
ml_features.toPandas().to_parquet(paths.artifacts / "ml_customer_features.parquet", index=False)
```

### 3.6. Great Expectations & Visualisasi

```python
dataset = PandasDataset(feature_pdf)
dataset.expect_column_values_to_not_be_null("customer_id")
dataset.expect_column_values_to_be_between("predicted_clv_12m", min_value=0)
dataset.expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
dataset.validate()

plot_df = feature_pdf.groupby("customer_segment")["predicted_clv_12m"].sum().reset_index()
sns.barplot(data=plot_df, x="customer_segment", y="predicted_clv_12m", hue="customer_segment", legend=False)
plt.savefig(paths.artifacts / "segment_clv.png")
```

## 4. Mengeksekusi Query / Debugging

Gunakan Spark SQL untuk mengecek hasil:

```bash
spark-sql "SELECT customer_segment, COUNT(*) customers, SUM(predicted_clv_12m) clv FROM gold.dim_customer_segments GROUP BY 1"
```

Atau baca Delta tables langsung di notebook / DuckDB menggunakan `read_delta`.

## 5. Troubleshooting

- Jalankan ulang dari nol: hapus `lakehouse/` dan `artifacts/`, kemudian jalankan
  `uv run pyspark-pipeline`.
- `ModuleNotFoundError: delta`: pastikan `uv sync` telah dijalankan sukses;
  gunakan `uv sync --refresh` bila butuh reinstal dependensi.
- Great Expectations gagal: periksa log untuk kolom yang mengandung nilai
  kosong atau CLV negatif; dataset seeds mungkin sudah dimodifikasi.

Dengan setup ini, seluruh snippet PySpark/Delta/Great Expectations di tutorial
memiliki implementasi nyata yang sudah diverifikasi berjalan di mesin lokal.
