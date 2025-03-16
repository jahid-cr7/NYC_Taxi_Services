#import os
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor

# Set input and output folders
input_folder = r"C:/Users/abrar/Downloads/Update 2023/2023/yellow" #C:\Users\abrar\Downloads\Update 2023\2023\yellow
output_folder = r"C:/Users/abrar/Downloads/Update 2023/2023csv/yellow"

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

def convert_parquet_to_csv(file, chunk_size=500000):
    """ Convert a large Parquet file into multiple smaller CSV files with optimizations. """
    parquet_file = os.path.join(input_folder, file)
    base_csv_name = file.replace(".parquet", "")

    try:
        # Read Parquet file in chunks
        table = pq.read_table(parquet_file)
        df = table.to_pandas()

        # Convert large int64 columns to int32 if possible
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].max() <= 2147483647:
                df[col] = df[col].astype('int32')

        # Split into smaller CSV files (chunk processing)
        for i, chunk in enumerate(range(0, len(df), chunk_size)):
            chunk_df = df.iloc[chunk:chunk + chunk_size]
            chunk_csv_file = os.path.join(output_folder, f"{base_csv_name}_part{i+1}.csv")
            chunk_df.to_csv(chunk_csv_file, index=False)
            print(f"Saved chunk {i+1}: {chunk_csv_file}")

    except Exception as e:
        print(f"Error converting {file}: {e}")

# Get list of Parquet files
parquet_files = [f for f in os.listdir(input_folder) if f.endswith(".parquet")]

# Use multi-threading for faster conversion
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(convert_parquet_to_csv, parquet_files)

print("All files converted successfully!")
