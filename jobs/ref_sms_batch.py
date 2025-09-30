#!/usr/bin/env python3
"""
ref_sms_batch.py - Batch processing for REF_SMS with Reports 1-4

- Report 1: daily revenue
- Report 2: 15-min revenue by paytype
- Report 3: min/max per 15-min window by paytype
- Report 4: record count + revenue per 15-min window by paytype
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType
from pyspark.sql.functions import col, to_timestamp, date_format, sum as _sum, min as _min, max as _max, count as _count, window

# Config
INPUT_DIR = "/opt/spark-data/REF_SMS"
REF_MAP_PATH = os.getenv("REF_MAP_PATH", "/opt/spark-data/REF_map/ref.csv")
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"
OUTPUT_BASE = os.getenv("OUTPUT_BASE", "s3a://sms-reports" if USE_MINIO else "/opt/spark-data/output")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")

# Schema
schema = StructType([
    StructField("ROAMSTATE_519", StringType(), True),
    StructField("CUST_LOCAL_START_DATE_15", StringType(), True),
    StructField("CDR_ID_1", StringType(), True),
    StructField("CDR_SUB_ID_2", StringType(), True),
    StructField("CDR_TYPE_3", StringType(), True),
    StructField("SPLIT_CDR_REASON_4", StringType(), True),
    StructField("RECORD_DATE", StringType(), True),
    StructField("PAYTYPE_515", StringType(), True),
    StructField("DEBIT_AMOUNT_42", DoubleType(), True),
    StructField("SERVICEFLOW_498", StringType(), True),
    StructField("EVENTSOURCE_CATE_17", StringType(), True),
    StructField("USAGE_SERVICE_TYPE_19", StringType(), True),
    StructField("SPECIALNUMBERINDICATOR_534", DoubleType(), True),
    StructField("BE_ID_30", DoubleType(), True),
    StructField("CALLEDPARTYIMSI_495", StringType(), True),
    StructField("CALLINGPARTYIMSI_494", StringType(), True),
])

# Spark session with configs
builder = SparkSession.builder.appName("REF_SMS_Batch_Processing")

if USE_MINIO:
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("="*60)
print("REF_SMS BATCH PROCESSING - ALL REPORTS")
print("="*60)
print(f"Reading from: {INPUT_DIR}")
print(f"Output to: {OUTPUT_BASE}")
print(f"Reference map: {REF_MAP_PATH}")
print(f"Using MinIO: {USE_MINIO}")
print("="*60)

try:
    # Load ref map
    print("Loading reference map...")
    ref_df = spark.read.option("header", True).csv(REF_MAP_PATH)
    ref_df = ref_df.withColumnRenamed(ref_df.columns[0], "paytype") \
                   .withColumnRenamed(ref_df.columns[1], "paytype_label") \
                   .withColumn("paytype", col("paytype").cast("integer"))
    
    print(f"✓ Reference map loaded successfully ({ref_df.count()} paytype mappings)")

    # Load input CSVs
    print("Loading input CSV files...")
    df = spark.read.option("header", True).schema(schema).csv(INPUT_DIR)
    
    total_records = df.count()
    print(f"✓ Loaded {total_records:,} records from {INPUT_DIR}")

    # Parse & clean
    print("Applying transformations...")
    df = df.withColumn("event_time", to_timestamp(col("RECORD_DATE"), "yyyy/MM/dd HH:mm:ss")) \
           .withColumn("paytype", col("PAYTYPE_515").cast("integer")) \
           .withColumn("debit_amount", col("DEBIT_AMOUNT_42").cast(DecimalType(14,2)) / 10) \
           .filter(col("event_time").isNotNull() & col("debit_amount").isNotNull())

    # Join with ref map
    df = df.join(ref_df, on="paytype", how="left")
    
    clean_records = df.count()
    print(f"✓ Transformations applied ({clean_records:,} clean records)")

    # ---------- Report 1: Daily revenue ----------
    print("\nProcessing Report 1: Daily revenue...")
    daily = df.withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) \
              .groupBy("date") \
              .agg(_sum("debit_amount").alias("daily_revenue"))

    daily_out = f"{OUTPUT_BASE.rstrip('/')}/daily_revenue"
    daily.coalesce(1).write.mode("overwrite").option("header", True).csv(daily_out)
    
    daily_count = daily.count()
    print(f"✓ Report 1 completed: {daily_count} daily records → {daily_out}")

    # ---------- Report 2: 15-min revenue by paytype ----------
    print("\nProcessing Report 2: 15-min revenue by paytype...")
    revenue_15min = df.groupBy(
        window(col("event_time"), "15 minutes"),
        col("paytype")
    ).agg(_sum("debit_amount").alias("revenue"))

    revenue_15min_flat = revenue_15min.select(
        col("window.start").alias("RECORD_DATE"),
        col("paytype"),
        col("revenue")
    ).orderBy("RECORD_DATE", "paytype")

    report2_out = f"{OUTPUT_BASE.rstrip('/')}/revenue_15min"
    revenue_15min_flat.coalesce(1).write.mode("overwrite").option("header", True).csv(report2_out)
    
    revenue_count = revenue_15min_flat.count()
    print(f"✓ Report 2 completed: {revenue_count} 15-min revenue records → {report2_out}")

    # ---------- Report 3: Min/Max per 15-min window by paytype ----------
    print("\nProcessing Report 3: Min/Max per 15-min window by paytype...")
    minmax_15min = df.groupBy(
        window(col("event_time"), "15 minutes"),
        col("paytype")
    ).agg(
        _min("debit_amount").alias("min_revenue"),
        _max("debit_amount").alias("max_revenue")
    )

    minmax_flat = minmax_15min.select(
        col("window.start").alias("RECORD_DATE"),
        col("paytype"),
        col("min_revenue"),
        col("max_revenue")
    ).orderBy("RECORD_DATE", "paytype")

    report3_out = f"{OUTPUT_BASE.rstrip('/')}/minmax_15min"
    minmax_flat.coalesce(1).write.mode("overwrite").option("header", True).csv(report3_out)
    
    minmax_count = minmax_flat.count()
    print(f"✓ Report 3 completed: {minmax_count} min/max records → {report3_out}")

    # ---------- Report 4: Record count + revenue per 15-min window by paytype ----------
    print("\nProcessing Report 4: Record count + revenue per 15-min window by paytype...")
    count_revenue_15min = df.groupBy(
        window(col("event_time"), "15 minutes"),
        col("paytype_label")
    ).agg(
        _sum("debit_amount").alias("revenue"),
        _count("*").alias("record_count")
    )

    count_flat = count_revenue_15min.select(
        col("window.start").alias("RECORD_DATE"),
        col("paytype_label").alias("Pay_type"),
        col("record_count"),
        col("revenue")
    ).orderBy("RECORD_DATE", "paytype_label")

    report4_out = f"{OUTPUT_BASE.rstrip('/')}/count_revenue_15min"
    count_flat.coalesce(1).write.mode("overwrite").option("header", True).csv(report4_out)
    
    count_revenue_count = count_flat.count()
    print(f"✓ Report 4 completed: {count_revenue_count} count/revenue records → {report4_out}")

    # Summary
    print("\n" + "="*60)
    print("BATCH PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"Total input records: {total_records:,}")
    print(f"Clean records processed: {clean_records:,}")
    print("\nOutput locations:")
    print(f"  Report 1 (Daily): {daily_out}")
    print(f"  Report 2 (15-min Revenue): {report2_out}")
    print(f"  Report 3 (15-min Min/Max): {report3_out}")
    print(f"  Report 4 (15-min Count/Revenue): {report4_out}")
    print("="*60)

except Exception as e:
    print(f"\n❌ ERROR during batch processing: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    spark.stop()
    print("✅ Spark session stopped.")
