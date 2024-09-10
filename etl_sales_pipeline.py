import pandas as pd
import numpy as np
import json
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_config = {
        'host': os.getenv('PG_HOST'),
        'dbname': os.getenv('PG_DATABASE'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD')
    }
    
directory = "data"

def etl_pipeline_sales(directory, db_config):
    """
    ETL pipeline for processing sales data.

    Args:
        directory (str): Directory containing the sales data files.
        db_config (dict): Database configuration for connecting to PostgreSQL.
    """
    try:
        # Extract data
        data = extract_data_sales(directory)

        # Transform data
        transformed_data_sales, transformed_data_sales_detail = transform_data_sales(data)

        # Load sales data
        with psycopg2.connect(**db_config) as conn:
            load_data_sales(conn, transformed_data_sales)

        # Load sales detail data
        with psycopg2.connect(**db_config) as conn:
            load_data_sales_detail(conn, transformed_data_sales_detail)

    except Exception as e:
        print(f"An error occurred in the ETL pipeline: {e}")

etl_pipeline_sales(directory, db_config)

with psycopg2.connect(**db_config) as conn:
    with conn.cursor() as cur:
        # Query total records in 'sales' table
        cur.execute('SELECT COUNT(*) AS total_records FROM sales;')
        total_sales_records = cur.fetchone()[0]

        # Query total records in 'sales_detail' table
        cur.execute('SELECT COUNT(*) AS total_records FROM sales_detail;')
        total_sales_detail_records = cur.fetchone()[0]

        # Print results
        print(f"Total number of records in 'sales' table: {total_sales_records}")
        print(f"Total number of records in 'sales_detail' table: {total_sales_detail_records}")