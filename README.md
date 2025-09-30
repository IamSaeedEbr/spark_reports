# REF_SMS Structured Streaming Reports

This project converts batch REF_SMS processing into real-time streaming using Apache Spark Structured Streaming with Docker deployment.

## 📁 Project Structure

```
project/
├── docker-compose.yml        # Docker services
├── README.md
├── jobs/
│   ├── ref_sms_batch.py          # Batch processing for REF_SMS with Reports 1-4
│   ├── report1_streaming.py      # Daily revenue
│   ├── report2_streaming.py      # 15-min revenue by paytype  
│   ├── report3_streaming.py      # 15-min min/max by paytype
│   ├── report4_streaming.py      # 15-min count/revenue by paytype
│   └── simulate_streaming.py     # Data simulator for testing
└── data/
    ├── REF_SMS/              # Source batch files
    ├── REF_SMS_streaming/    # Streaming input (monitored by Spark)
    ├── REF_map/              # Reference mapping (only for Report 4)
    ├── output                # results in Local mode
    └── checkpoints           # checkpoints in Local mode
```
## 📊 Data Overview

The original **REF_CBS_SMS2.csv** file (~750K rows) has been split into **15 chunks** for streaming simulation:

- **Size**: ~50,000 rows per chunk  
- **Location**: `data/REF_SMS/`
- **Purpose**: Simulate real-time data arrival by copying chunks sequentially

The reference mapping file **Ref.xlsx** is converted to **ref.csv** containing paytype labels:
```
PayType,value
0,Prepaid  
1,Postpaid
```

## 🚀 Quick Start

### 1. **Setup Environment**
```bash
# Start Docker services (Spark + MinIO)
docker-compose up -d
```

### 2. **Run a Report**
```bash
# Choose any report (1-4), e.g.:
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/report1_streaming.py  
# OR for batch mode
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ref_sms_batch.py     
```

### 3. **Start Data Simulation** (in another terminal)
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/simulate_streaming.py
```

### 4. **Monitor Results**
- **Console**: Real-time progress and sample data
- **CSV Files**: `data/output/` or MinIO bucket
- **Logs**: Docker container logs



## 🔧 Configuration

### Environment Variables
```bash
# Input/Output
INPUT_DIR="/opt/spark-data/REF_SMS_streaming"
OUTPUT_BASE="s3a://sms-reports" #or "/opt/spark-data/output"
CHECKPOINT_BASE="s3a://sms-checkpoints" #or "/opt/spark-data/checkpoints"

# Streaming Settings
TRIGGER_INTERVAL="30 seconds"
WATERMARK_DELAY="10 minutes"

# MinIO 
USE_MINIO="true"
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS="minio"
MINIO_SECRET="minio123"

# Simulation
COPY_INTERVAL=30        # Seconds between file copies
FILES_PER_BATCH=1       # Files copied per batch
```

## 🔄 How It Works

### Data Flow
```
REF_SMS (source files)
    ↓ (jobs/simulate_streaming.py copies files every 30s)
REF_SMS_streaming (monitored by Spark)
    ↓ (Spark processes in 30s intervals)
Output CSV files (MinIO/local storage)
```

### Key Features
- **Real-time Processing**: 30-second trigger intervals
- **Fault Tolerance**: Automatic checkpointing
- **Late Data Handling**: 10-minute watermark
- **Docker Ready**: Full containerized deployment


## 📈 Output Formats

### Report 1: Daily Revenue
```csv
date,daily_revenue
2020-08-30,9386861742.500000
```

### Report 2: 15-min Revenue by Paytype
```csv
RECORD_DATE,paytype,revenue
2020-08-30T00:00:00.000Z,0,1722946762.300000
2020-08-30T00:00:00.000Z,1,652891472.600000
2020-08-30T00:15:00.000Z,0,1661709363.300000
2020-08-30T00:15:00.000Z,1,620119097.900000
```

### Report 3: 15-min Min/Max by Paytype
```csv
RECORD_DATE,paytype,min_revenue,max_revenue
2020-08-30T00:00:00.000Z,0,-545000.000000,1642630.000000
2020-08-30T00:00:00.000Z,1,0.000000,2180000.000000
2020-08-30T00:15:00.000Z,0,-545000.000000,1097630.000000
2020-08-30T00:15:00.000Z,1,0.000000,2180000.000000
```

### Report 4: 15-min Count/Revenue by Paytype
```csv
RECORD_DATE,Pay_type,record_count,revenue
2020-08-30T00:00:00.000Z,Postpaid,53094,652891472.600000
2020-08-30T00:00:00.000Z,Prepaid,135930,1722946762.300000
2020-08-30T00:15:00.000Z,Postpaid,51286,620119097.900000
2020-08-30T00:15:00.000Z,Prepaid,133479,1661709363.300000
```