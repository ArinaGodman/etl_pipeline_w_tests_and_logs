import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from transform_data import transform_data_sales  

# Sample valid data for testing
@pytest.fixture
def valid_data():
    data = {
        'event_date': ['2023-01-01', '2023-01-01', '2023-01-02'],
        'event_value_in_usd': [100.0, 50.0, 75.0],
        'user_pseudo_id': ['user_1', 'user_2', 'user_3'],
        'ecommerce_transaction_id': ['txn_1', 'txn_2', 'txn_3'],
        'item_id': ['item_1', 'item_2', 'item_3'],
        'item_quantity': [2, 3, 1],
        'item_price': [10.0, 15.0, 25.0],
        'item_price_in_usd': [9.0, 14.5, 24.5]
    }
    return pd.DataFrame(data)

# Sample invalid data for testing (with missing/invalid values)
@pytest.fixture
def invalid_data():
    data = {
        'event_date': ['2023-01-01', 'not_a_date', '2023-01-02'],
        'event_value_in_usd': [100.0, 'invalid_value', 75.0],
        'user_pseudo_id': ['user_1', None, 'user_3'],
        'ecommerce_transaction_id': ['txn_1', 'txn_2', None],
        'item_id': ['item_1', 'item_2', 'item_3'],
        'item_quantity': [2, 'invalid', None],
        'item_price': [10.0, 'invalid_price', 25.0],
        'item_price_in_usd': [9.0, None, 'not_a_number']
    }
    return pd.DataFrame(data)

def test_transform_valid_data(valid_data):
    """
    Test the transform_data_sales function with valid data.
    """
    df_summary = transform_data_sales(valid_data)
    
    # Checking if the returned dataframe has the expected columns
    expected_columns = ['event_date', 'ecommerce_transaction_id', 'user_pseudo_id', 
                        'event_value_in_usd', 'item_quantity', 'total_sales', 'total_sales_in_usd']
    
    assert list(df_summary.columns) == expected_columns
    
    # Checking if the number of rows is correct (since it's grouped)
    assert len(df_summary) == 3
    
    # Checking if total_sales and total_sales_in_usd are calculated correctly
    assert df_summary['total_sales'].sum() == 90.0
    assert df_summary['total_sales_in_usd'].sum() == 86.0

def test_transform_invalid_data(invalid_data):
    """
    Test the transform_data_sales function with invalid data.
    """
    df_summary = transform_data_sales(invalid_data)
    
    # Checking if the function can handle NaN and invalid data types (via coercion)
    assert df_summary['total_sales'].isnull().sum() == 1  # One invalid row should be coerced to NaN
    assert df_summary['total_sales_in_usd'].isnull().sum() == 1
    
    # Checking if the valid rows are still processed correctly
    assert len(df_summary) == 3  # Original rows count should be preserved (no row dropped by groupby)

def test_transform_empty_dataframe():
    """
    Test the transform_data_sales function with an empty DataFrame.
    """
    df = pd.DataFrame({
        'event_date': [],
        'event_value_in_usd': [],
        'user_pseudo_id': [],
        'ecommerce_transaction_id': [],
        'item_id': [],
        'item_quantity': [],
        'item_price': [],
        'item_price_in_usd': []
    })
    
    df_summary = transform_data_sales(df)
    
    # The result should be an empty DataFrame
    assert df_summary.empty

def test_transform_missing_columns():
    """
    Test the transform_data_sales function when required columns are missing.
    """
    df = pd.DataFrame({
        'event_date': ['2023-01-01', '2023-01-02'],
        # Missing important columns like 'user_pseudo_id', 'item_quantity', etc.
    })
    
    with pytest.raises(KeyError):
        transform_data_sales(df)