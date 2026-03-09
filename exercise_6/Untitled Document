import pandas as pd
import argparse
import os

def parquet_to_csv(parquet_file, csv_file, nrows=None):
    """
    Convert a Parquet file to CSV safely.
    
    Args:
        parquet_file (str): Path to input Parquet file.
        csv_file (str): Path to output CSV file.
        nrows (int, optional): Number of rows to write. Writes all rows if None.
    """
    # Check if file exists
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Input file not found: {parquet_file}")

    # Read Parquet
    df = pd.read_parquet(parquet_file)
    
    # Optionally limit rows
    if nrows is not None:
        df = df.head(nrows)
    
    # Write to CSV without index
    df.to_csv(csv_file, index=False)
    print(f"Successfully converted {parquet_file} -> {csv_file} ({len(df)} rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Parquet to CSV safely")
    parser.add_argument("parquet_file", help="Input Parquet file path")
    parser.add_argument("csv_file", help="Output CSV file path")
    parser.add_argument("--nrows", type=int, default=None, help="Number of rows to write (optional)")
    args = parser.parse_args()

    parquet_to_csv(args.parquet_file, args.csv_file, args.nrows)
