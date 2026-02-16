import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
DATA_DIR = Path("data") / "fhv"

def download_and_convert_files():
    DATA_DIR.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020, 2021]:
        for month in range(1, 13):
            parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = DATA_DIR / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = DATA_DIR / csv_gz_filename

            try:
                response = requests.get(f"{BASE_URL}/{csv_gz_filename}", stream=True)
                if not response.ok:
                    print(f"Skipping {csv_gz_filename} (HTTP {response.status_code})")
                    continue

                with open(csv_gz_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"Converting {csv_gz_filename} to Parquet...")
                con = duckdb.connect()
                con.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}', ignore_errors=true, strict_mode=false))
                    TO '{parquet_filepath}' (FORMAT PARQUET)
                """)
                con.close()
                print(f"Completed {parquet_filename}")
            except Exception as e:
                print(f"Failed {csv_gz_filename}: {e}")
                if parquet_filepath.exists():
                    parquet_filepath.unlink()
            finally:
                if csv_gz_filepath.exists():
                    csv_gz_filepath.unlink()

def load_into_duckdb():
    con = duckdb.connect("dev.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute("""
        CREATE OR REPLACE TABLE main.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)
    print("Loaded FHV data into main.fhv_tripdata")
    con.close()

if __name__ == "__main__":
    download_and_convert_files()
    load_into_duckdb()
