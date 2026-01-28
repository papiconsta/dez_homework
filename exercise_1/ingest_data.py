#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm 
import pyarrow.parquet as pq

import click

@click.command()
@click.option('--pg_user', required=True, help='PostgreSQL username')
@click.option('--pg_password', required=True, help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default='5432', help='PostgreSQL port')
@click.option('--pg_db', required=True, help='Database name')
@click.option('--target_table', required=True, help='Target table name')
@click.option('--chunk_size', required=True, help='Desired chunk size for data ingestion', type=int)
@click.option('--target_dataset', required=True, help='Path to the target dataset file')

def run(pg_user, pg_password, pg_host, pg_port, pg_db, target_table, chunk_size, target_dataset):

    # taxi_csv = pd.read_csv('./ny_taxi_postgres_data/taxi_zone_lookup.csv')
    # taxi_parq = pd.read_parquet('./ny_taxi_postgres_data/green_tripdata_2025-11.parquet', engine='pyarrow')

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    if target_dataset.endswith('.csv'):
        # CSV: use pandas chunked reading
        df_sample = pd.read_csv(target_dataset, nrows=0)
        print(pd.io.sql.get_schema(df_sample, name=target_table, con=engine))
        for chunk in pd.read_csv(target_dataset, chunksize=chunk_size):
            chunk.to_sql(target_table, con=engine, if_exists='append', index=False)

    elif target_dataset.endswith('.parquet'):
        # Parquet: use pyarrow batch reading
        df_sample = pd.read_parquet(target_dataset).head(0)
        print(pd.io.sql.get_schema(df_sample, name=target_table, con=engine))
        parquet_file = pq.ParquetFile(target_dataset)
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df = batch.to_pandas()
            df.to_sql(target_table, con=engine, if_exists='append', index=False)

    else:
        raise ValueError(f"Unsupported file format: {target_dataset}")


if __name__ == '__main__':
    run()