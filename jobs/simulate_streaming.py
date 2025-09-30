#!/usr/bin/env python3
"""
simulate_streaming.py - Simulate streaming by copying CSV files to a streaming directory
"""

import os
import shutil
import time
import glob
import uuid
from datetime import datetime

# Config
SOURCE_DIR = "/opt/spark-data/REF_SMS"  # Original batch data directory
STREAMING_DIR = "/opt/spark-data/REF_SMS_streaming"  # Directory the streaming job monitors
COPY_INTERVAL = 30  # Seconds between copying files
FILES_PER_BATCH = 1  # How many files to copy per batch


def setup_streaming_dir():
    """Create the streaming directory if it doesn't exist"""
    if not os.path.exists(STREAMING_DIR):
        os.makedirs(STREAMING_DIR, exist_ok=True)
        print(f"Created streaming directory: {STREAMING_DIR}")
    else:
        # Clear any existing files
        for file in glob.glob(f"{STREAMING_DIR}/*.csv"):
            os.remove(file)
        print(f"Cleared existing files from: {STREAMING_DIR}")


def get_source_files():
    """Get list of CSV files from source directory"""
    csv_files = glob.glob(f"{SOURCE_DIR}/*.csv")
    if not csv_files:
        print(f"No CSV files found in {SOURCE_DIR}")
        return []
    print(f"Found {len(csv_files)} CSV files in source directory")
    csv_files.sort()
    return csv_files


def copy_files_to_stream(source_files, start_idx, files_per_batch):
    """Copy a batch of files to the streaming directory"""
    copied = 0
    for i in range(files_per_batch):
        if start_idx + i >= len(source_files):
            break

        source_file = source_files[start_idx + i]
        # Create unique filename to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"data_{timestamp}_{unique_id}.csv"
        dest_file = os.path.join(STREAMING_DIR, filename)

        shutil.copy2(source_file, dest_file)
        print(f"Copied: {os.path.basename(source_file)} -> {filename}")
        copied += 1

    return copied


def simulate_streaming():
    """Main simulation function"""
    print("Setting up streaming simulation...")
    setup_streaming_dir()

    source_files = get_source_files()
    if not source_files:
        return

    print(f"Starting simulation with {FILES_PER_BATCH} files every {COPY_INTERVAL} seconds")
    print("Press Ctrl+C to stop simulation\n")

    current_idx = 0
    batch_num = 1

    try:
        while current_idx < len(source_files):
            print(f"--- Batch {batch_num} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

            copied = copy_files_to_stream(source_files, current_idx, FILES_PER_BATCH)
            current_idx += copied

            if copied == 0:
                print("All files have been processed. Starting over...")
                current_idx = 0
                batch_num = 0

            print(f"Batch {batch_num} complete. Waiting {COPY_INTERVAL} seconds...\n")
            batch_num += 1

            time.sleep(COPY_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping streaming simulation...")


if __name__ == "__main__":
    simulate_streaming()