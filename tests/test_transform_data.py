import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from transform_data import transform_data_sales  

@pytest.fixture
def sample_data():
    data = {
        'event_date': ['2024-09-10', '2024-09-11'],
        'event_value_in_usd': [100.0, 200.0],
        'user_pseudo_id': ['pseudo1', 'pseudo2'],
        'ecommerce_transaction_id': ['trans1', 'trans2'],
        'item_id': ['item1', 'item2'],
        'item_quantity': [2, 3],
        'item_price': [50.0, 75.0],
        'item_price_in_usd': [50.0, 75.0]
    }
    df = pd.DataFrame(data)
    return df

def test_transform_data_sales(sample_data):

    transformed_df = transform_data_sales(sample_data)
    
    # Checking if the transformation is correct
    assert isinstance(transformed_df, pd.DataFrame)
    
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['event_date'])
    assert pd.api.types.is_numeric_dtype(transformed_df['event_value_in_usd'])
    
    # Checking that 'item_price' and 'item_price_in_usd' are numeric and rounded
    assert pd.api.types.is_numeric_dtype(transformed_df['item_price'])
    assert pd.api.types.is_numeric_dtype(transformed_df['item_price_in_usd'])
    
    # Checking calculated columns
    expected_total_sales = sample_data['item_quantity'] * sample_data['item_price']
    expected_total_sales_in_usd = sample_data['item_quantity'] * sample_data['item_price_in_usd']
    
    assert all(transformed_df['total_sales'] == expected_total_sales)
    assert all(transformed_df['total_sales_in_usd'] == expected_total_sales_in_usd)
    
    # Checking that 'item_id' column exists and no NaN values
    assert 'item_id' in transformed_df.columns
    assert transformed_df['item_id'].notna().all()
