import pandas as pd
import numpy as np
import json
import os
import logging

import psycopg2
from dotenv import load_dotenv

from extract_data import extract_data_sales
from transform_data import transform_data_sales
from load_data import load_data_sales 


load_dotenv()

db_config = {
        'host': os.getenv('PG_HOST'),
        'dbname': os.getenv('PG_DATABASE'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD')
    }
    
directory = "data"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('etl_pipeline.log'),  # Log to a file
                    ])

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
        transformed_data_sales = transform_data_sales(data)

        # Load sales data
        with psycopg2.connect(**db_config) as conn:
            load_data_sales(conn, transformed_data_sales)

    except Exception as e:
        logging.error(f"An error occurred in the ETL pipeline: {e}")


if __name__ == "__main__":
    etl_pipeline_sales(directory, db_config)

with psycopg2.connect(**db_config) as conn:
    with conn.cursor() as cur:
        # Query total records in 'sales' table
        cur.execute('SELECT COUNT(*) AS total_records FROM sales;')
        total_sales_records = cur.fetchone()[0]

        # Print results
        print(f"Total number of records in 'sales' table: {total_sales_records}")