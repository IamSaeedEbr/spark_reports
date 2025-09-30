#!/usr/bin/env python3
"""
report3_streaming.py - Structured Streaming for Report 3 only: Min/Max per 15-min window by paytype

This script processes streaming CSV files and generates 15-minute min/max aggregations
by paytype, saving results as CSV files to MinIO/S3.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType
from pyspark.sql.functions import col, to_timestamp, min as _min, max as _max, window

# Config
INPUT_DIR = os.getenv("INPUT_DIR", "/opt/spark-data/REF_SMS_streaming")
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"
OUTPUT_BASE = os.getenv("OUTPUT_BASE", "s3a://sms-reports" if USE_MINIO else "/opt/spark-data/output")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "s3a://sms-checkpoints" if USE_MINIO else "/opt/spark-data/checkpoints")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")

# Streaming config
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")  # How often to check for new files
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")   # Late data tolerance

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

def write_minmax_15min_to_csv(df, epoch_id):
    """Custom function to write 15-min min/max by paytype to CSV"""
    try:
        if df.count() > 0:
            print(f"Processing 15-min min/max epoch {epoch_id} with {df.count()} rows")
            
            # Write to a single location (overwrites previous with complete results)
            output_path = f"{OUTPUT_BASE}/minmax_15min"
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(output_path)
            
            # Show sample data for monitoring
            print(f"15-min min/max data for epoch {epoch_id} (showing first 10 rows):")
            df.show(10, truncate=False)
            
            print(f"✓ Successfully written 15-min min/max CSV for epoch {epoch_id} to {output_path}")
        else:
            print(f"No 15-min min/max data in epoch {epoch_id}")
    except Exception as e:
        print(f"❌ Error in 15-min min/max epoch {epoch_id}: {str(e)}")
        import traceback
        traceback.print_exc()

# Spark session with streaming configs
builder = SparkSession.builder.appName("Report3_15min_MinMax_Streaming") \
    .config("spark.sql.streaming.checkpointLocation.reset", "false") \
    .config("spark.sql.adaptive.enabled", "false")  # Disable AQE for streaming

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
print("REPORT 3: 15-MIN MIN/MAX BY PAYTYPE - STREAMING")
print("="*60)
print(f"Reading from: {INPUT_DIR}")
print(f"Output to: {OUTPUT_BASE}/minmax_15min")
print(f"Checkpoint: {CHECKPOINT_BASE}/minmax_15min")
print(f"Trigger interval: {TRIGGER_INTERVAL}")
print(f"Watermark delay: {WATERMARK_DELAY}")
print(f"Using MinIO: {USE_MINIO}")
print("="*60)

# Create streaming DataFrame - monitors INPUT_DIR for new CSV files
print("Creating streaming DataFrame...")
streaming_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 5) \
    .csv(INPUT_DIR)

print("✓ Streaming DataFrame created")

# Parse & clean
print("Applying transformations...")
streaming_df = streaming_df.withColumn("event_time", to_timestamp(col("RECORD_DATE"), "yyyy/MM/dd HH:mm:ss")) \
                          .withColumn("paytype", col("PAYTYPE_515").cast("integer")) \
                          .withColumn("debit_amount", col("DEBIT_AMOUNT_42").cast(DecimalType(14,2)) / 10) \
                          .filter(col("event_time").isNotNull() & col("debit_amount").isNotNull())

# Add watermark for handling late data
streaming_df_watermarked = streaming_df.withWatermark("event_time", WATERMARK_DELAY)

print("✓ Transformations applied")

# ---------- Report 3: 15-min min/max by paytype ----------
print("Creating 15-minute min/max aggregation...")
minmax_15min = streaming_df_watermarked.groupBy(
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

print("✓ Aggregation created")

# Start streaming query with COMPLETE mode
print("Starting streaming query...")
minmax_query = minmax_flat.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_minmax_15min_to_csv) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/minmax_15min") \
    .queryName("minmax_15min_stream") \
    .start()

print("✓ Streaming query started successfully!")
print(f"Query ID: {minmax_query.id}")
print(f"Query Name: {minmax_query.name}")
print("\nWaiting for streaming data...")
print("Press Ctrl+C to stop the application\n")

try:
    # Monitor the streaming query status
    while minmax_query.isActive:
        status = minmax_query.status
        print(f"Query Status: {status['message'] if 'message' in status else 'Running'}")
        
        # Show recent progress
        progress = minmax_query.lastProgress
        if progress:
            batch_id = progress.get('batchId', 'N/A')
            input_rows = progress.get('inputRowsPerSecond', 0)
            processed_rows = progress.get('processedRowsPerSecond', 0)
            print(f"  Batch: {batch_id}, Input: {input_rows} rows/sec, Processed: {processed_rows} rows/sec")
        
        # Check for exceptions
        exception = minmax_query.exception()
        if exception:
            print(f"❌ Query exception: {exception}")
            break
            
        time.sleep(30)  # Check every 30 seconds
        
except KeyboardInterrupt:
    print("\n⏹️  Shutdown requested by user")
except Exception as e:
    print(f"\n❌ Monitoring error: {e}")
finally:
    print("\nStopping streaming query...")
    if minmax_query.isActive:
        minmax_query.stop()
    spark.stop()
    print("✅ Report 3 streaming application stopped successfully.")
    print(f"Final output location: {OUTPUT_BASE}/minmax_15min")
