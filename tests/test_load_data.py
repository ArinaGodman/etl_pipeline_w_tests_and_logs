import pytest
import pandas as pd
import psycopg2
import sys
import os

from psycopg2 import sql

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from load_data import load_data_sales

TEST_DB_CONFIG = {
    'dbname': 'test_db',
    'user': 'postgres',
    'password': '12345678',
    'host': 'localhost'
}

@pytest.fixture(scope='module')
def db_connection():
    """
    Fixture to create and close a database connection.
    Ensures the connection is closed after tests complete.
    """
    try:
        conn = psycopg2.connect(**TEST_DB_CONFIG)
        conn.autocommit = True  # Ensuring that operations like CREATE TABLE are committed
        yield conn
    finally:
        conn.close()

@pytest.fixture(scope='module')
def setup_database(db_connection):
    """
    Fixture to set up the database before tests and clean up after tests.
    This will create and later drop the test table 'sales'.
    """
    with db_connection.cursor() as cur:
        # Create a test table
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
    # Drop the test table after tests complete
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS sales;")

def load_data_sales(conn, df):
    """
    Function to load data into the 'sales' table.
    """
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
            INSERT INTO sales (event_date, ecommerce_transaction_id, user_pseudo_id, event_value_in_usd,
                               item_quantity, total_sales, total_sales_in_usd)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                row['event_date'], row['ecommerce_transaction_id'], row['user_pseudo_id'],
                row['event_value_in_usd'], row['item_quantity'], row['total_sales'], row['total_sales_in_usd']
            ))


def test_load_data_sales(db_connection, setup_database):
    """
    Test loading data into the sales table and verify it was inserted correctly.
    """
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

    # Function to load data into the sales table
    load_data_sales(db_connection, df)
    
    # Verify the data was inserted correctly
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM sales WHERE ecommerce_transaction_id = %s;", ('trans1',))
        result = cur.fetchone()

    # Assertions to check if the inserted data matches expected values
    assert result is not None
    assert result[0] == pd.to_datetime('2024-09-10').date()  # event_date
    assert result[1] == 'trans1'  # ecommerce_transaction_id
    assert result[2] == 'pseudo1'  # user_pseudo_id
    assert result[3] == 100.0  # event_value_in_usd
    assert result[4] == 2  # item_quantity
    assert result[5] == 100.0  # total_sales
    assert result[6] == 100.0  # total_sales_in_usd

def test_load_data_sales_unique_violation(db_connection, setup_database):
    """
    Test inserting duplicate records to trigger a unique constraint violation.
    """
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
    
    # Inserting the first row (should work)
    load_data_sales(db_connection, df)
    
    # Inserting the same row again to trigger a unique violation
    try:
        load_data_sales(db_connection, df)
        assert False, "Expected a UniqueViolation error"
    except psycopg2.errors.UniqueViolation:
        # This is expected due to the unique constraint on ecommerce_transaction_id
        pass
    
    # Verifying that only one row was inserted
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sales WHERE ecommerce_transaction_id = %s;", ('trans1',))
        count = cur.fetchone()[0]
    
    assert count == 1  # Ensuring only one row exists with the same ecommerce_transaction_id