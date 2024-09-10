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

def extract_data_sales(directory):
    """
    Extracts and processes sales-related data from JSON files in the specified directory.
    
    Args:
    - directory (str): Directory path containing JSON files to be processed.
    
    Returns:
    - pd.DataFrame: Combined DataFrame containing extracted and processed sales data.
    
    Raises:
    - Exception: If no valid data is found in the specified directory.
    
    """
    data_frames = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
                data = json.load(file)
                df = pd.json_normalize(data, sep='_')
                
                df_purchase = df[df['event_name'] == 'purchase']
                
                df_purchase = df_purchase.explode('items').reset_index(drop=True)
                
                if 'items' in df_purchase.columns:
                    items_df = pd.json_normalize(df_purchase['items'])
                    df_purchase = pd.concat([df_purchase.drop(columns=['items']), items_df], axis=1)
                
                df_purchase = df_purchase[[
                    'event_date',
                    'event_value_in_usd',
                    'user_id',
                    'user_pseudo_id', 
                    'ecommerce_transaction_id',
                    'item_id',
                    'quantity',
                    'price',
                    'price_in_usd'
                ]].rename(columns={
                    'quantity': 'item_quantity',
                    'price': 'item_price',
                    'price_in_usd': 'item_price_in_usd'
                })
                
                data_frames.append(df_purchase)
    
    if not data_frames:
        raise Exception("No valid data found in the specified directory.")
    
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df


def transform_data_sales(df):
    """
    Transform sales data to create both summary and detail views.

    Args:
        df (pandas.DataFrame): Raw sales data.

    Returns:
        tuple: A tuple containing the transformed summary dataframe and the detailed dataframe.
    """
    
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    df['event_value_in_usd'] = pd.to_numeric(df['event_value_in_usd'], errors='coerce').round(2)
    df['user_pseudo_id'] = df['user_pseudo_id'].astype('string')
    df['ecommerce_transaction_id'] = df['ecommerce_transaction_id'].astype('string')
    df['item_id'] = df['item_id'].astype('string')
    df['item_quantity'] = pd.to_numeric(df['item_quantity'], errors='coerce')
    df['item_price'] = pd.to_numeric(df['item_price'], errors='coerce').round(2)
    df['item_price_in_usd'] = pd.to_numeric(df['item_price_in_usd'], errors='coerce').round(2)

    df['total_sales'] = df['item_quantity'] * df['item_price']
    df['total_sales_in_usd'] = df['item_quantity'] * df['item_price_in_usd']

    df_summary = df.groupby(['event_date', 'ecommerce_transaction_id', 'user_pseudo_id','event_value_in_usd']).agg({
        'item_quantity': 'sum',
        'total_sales': 'sum',
        'total_sales_in_usd': 'sum'
    }).reset_index()
    
    df = df.dropna(subset=['item_id'])

    return df_summary, df

def load_data_sales(conn, data):
    """
    Loads sales data into the 'sales' table in the PostgreSQL database.

    Args:
        conn (psycopg2 connection): Active connection to the PostgreSQL database.
        data (pd.DataFrame): DataFrame containing sales data to be loaded into the database.

    """
    with conn.cursor() as cur:

        for idx, row in data.iterrows():
            cur.execute('''
            INSERT INTO sales (
                event_date,
                ecommerce_transaction_id,
                user_pseudo_id,
                event_value_in_usd,
                item_quantity,
                total_sales,
                total_sales_in_usd
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            ''', (
                row['event_date'],
                row['ecommerce_transaction_id'],
                row['user_pseudo_id'],
                row['event_value_in_usd'],
                row['item_quantity'],
                row['total_sales'],
                row['total_sales_in_usd']
            ))

        conn.commit()
        print("Data loaded successfully into 'sales' table.")

def load_data_sales_detail(conn, data):
    """
    Load sales_detail data into the PostgreSQL database.

    Args:
        conn (psycopg2.connection): Connection object to the PostgreSQL database.
        data (pandas.DataFrame): Transformed sales detail data.
    """
    with conn.cursor() as cur:

        insert_query = '''
        INSERT INTO sales_detail (
            ecommerce_transaction_id,
            item_id,
            item_quantity,
            item_price
        ) VALUES (%s, %s, %s, %s)
        '''

        for idx, row in data.iterrows():
            cur.execute(insert_query, (
                row['ecommerce_transaction_id'],
                row['item_id'],
                row['item_quantity'],
                row['item_price']
            ))

        conn.commit()
        print("Data loaded successfully into 'sales_detail' table.")


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