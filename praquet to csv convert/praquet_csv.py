import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Set the folder paths
#input_folder = r"C:/Users/abrar/Downloads/Update 2023/2024/Green" #C:\Users\abrar\Downloads\Update 2023\2024\Green
#output_folder = r"C:/Users/abrar/Downloads/Update 2023/2024csv/green" #C:\Users\abrar\Downloads\Update 2023\2024csv\green
#input_folder = r"C:/Users/abrar/Downloads/Update 2023/2024/green"
#output_folder = r"C:/Users/abrar/Downloads/Update 2023/2024csv/green"
input_folder = r"C:/Users/abrar/Downloads/Update 2023/2019/fhv" #C:\Users\abrar\Downloads\Update 2023\2023\yellow
output_folder = r"C:/Users/abrar/Downloads/Update 2023/2019csv/fhv"#C:\Users\abrar\Downloads\Update 2023\2023csv\yellow
#C:\Users\alami\Downloads\Update 2023\2021csv\yellow

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

def convert_parquet_to_csv(file):
    """ Convert a single Parquet file to CSV with optimizations. """
    parquet_file = os.path.join(input_folder, file)
    csv_file = os.path.join(output_folder, file.replace(".parquet", ".csv"))

    try:
        # Read in chunks
        df = pd.read_parquet(parquet_file, engine="fastparquet")

        # Optimize memory by downcasting numeric types
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = df[col].astype('int32')

        # Write to CSV in chunks
        df.to_csv(csv_file, index=False, chunksize=100000)
        print(f"Converted {file} -> {csv_file}")

    except Exception as e:
        print(f"Error converting {file}: {e}")

# Get list of Parquet files
parquet_files = [f for f in os.listdir(input_folder) if f.endswith(".parquet")]

# Use multi-threading for faster conversion
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(convert_parquet_to_csv, parquet_files)

print("All files converted successfully!")