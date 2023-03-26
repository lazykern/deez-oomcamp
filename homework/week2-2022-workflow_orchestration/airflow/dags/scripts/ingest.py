import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def postgres_ingest(
    user: str,
    password: str,
    host: str,
    port: str,
    database: str,
    table_name: str,
    csv_file_path: str,
):
    print(user, host, database)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    engine.connect()

    print("Database connection established")
    print(f"Database: {database}")
    print("Starting data ingestion")
    print(f"Reading {csv_file_path} into a pandas dataframe")

    t_start = time()
    df_iter = pd.read_csv(
        csv_file_path,
        iterator=True,
        chunksize=100000,
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    )

    df = next(df_iter)

    df.to_sql(table_name, engine, if_exists="replace")

    t_end = time()
    print(f"Data ingestion for first batch completed in {t_end - t_start} seconds")

    for i, df in enumerate(df_iter):
        t_start = time()
        df.to_sql(table_name, engine, if_exists="append")
        t_end = time()

        print(f"Data ingestion for batch #{i+2} completed in {t_end - t_start} seconds")
