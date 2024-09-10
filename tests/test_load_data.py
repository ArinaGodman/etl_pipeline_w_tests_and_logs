import pytest
import pandas as pd
import psycopg2
import sys
import os
import logging

from psycopg2 import sql

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from load_data import load_data_sales

# Define test database configuration
TEST_DB_CONFIG = {
    'dbname': 'test_db',
    'user': 'test_user',
    'password': 'test_password',
    'host': 'localhost',
    'port': '5432'
}

@pytest.fixture(scope='module')
def db_connection():
    conn = psycopg2.connect(**TEST_DB_CONFIG)
    conn.autocommit = True  # Ensuring that operations like CREATE TABLE are committed
    yield conn
    conn.close()

@pytest.fixture(scope='module')
def setup_database(db_connection):
    # Creating test tables
    with db_connection.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            event_date DATE,
            ecommerce_transaction_id VARCHAR PRIMARY KEY,
            user_pseudo_id VARCHAR,
            event_value_in_usd NUMERIC,
            item_quantity INTEGER,
            total_sales NUMERIC,
            total_sales_in_usd NUMERIC
        );
        """)
    yield
    # Dropping the test tables
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS sales;")

def test_load_data_sales(db_connection, setup_database):
    logging.info("Starting test_load_data_sales")

    data = {
        'event_date': ['2024-09-10'],
        'ecommerce_transaction_id': ['trans1'],
        'user_pseudo_id': ['pseudo1'],
        'event_value_in_usd': [100.0],
        'item_quantity': [2],
        'total_sales': [100.0],
        'total_sales_in_usd': [100.0]
    }
    df = pd.DataFrame(data)
    
    logging.info("Data prepared for insertion: %s", df)

    load_data_sales(db_connection, df)
    
    # Verifying the data was loaded correctly
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM sales WHERE ecommerce_transaction_id = %s;", ('trans1',))
        result = cur.fetchone()
    
    logging.info("Query result for ecommerce_transaction_id 'trans1': %s", result)

    assert result is not None
    assert result[0] == pd.to_datetime('2024-09-10').date()
    assert result[1] == 'trans1'
    assert result[2] == 'pseudo1'
    assert result[3] == 100.0
    assert result[4] == 2
    assert result[5] == 100.0
    assert result[6] == 100.0

def test_load_data_sales_unique_violation(db_connection, setup_database):
    logging.info("Starting test_load_data_sales_unique_violation")

    data = {
        'event_date': ['2024-09-10'],
        'ecommerce_transaction_id': ['trans1'],  # This will cause a unique violation
        'user_pseudo_id': ['pseudo1'],
        'event_value_in_usd': [100.0],
        'item_quantity': [2],
        'total_sales': [100.0],
        'total_sales_in_usd': [100.0]
    }
    df = pd.DataFrame(data)
    
    logging.info("Data prepared for insertion: %s", df)
    
    # Inserting the first row
    load_data_sales(db_connection, df)
    
    # Inserting the same row again to trigger a unique violation
    try:
        load_data_sales(db_connection, df)
    except psycopg2.errors.UniqueViolation as e:
        logging.error("Caught expected unique violation error: %s", e)
    
    # Checking that the row was inserted only once
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sales WHERE ecommerce_transaction_id = %s;", ('trans1',))
        count = cur.fetchone()[0]
    
    logging.info("Number of rows with ecommerce_transaction_id 'trans1': %d", count)
    
    assert count == 1
