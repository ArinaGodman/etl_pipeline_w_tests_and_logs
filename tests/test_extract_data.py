import sys
import os
import pytest
import json
import pandas as pd

# Adding the root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extract_data import extract_data_sales 

@pytest.fixture
def temp_json_dir():
    # Set teting up temporary JSON directory for testing
    dir_name = 'temp_test_dir'
    os.makedirs(dir_name, exist_ok=True)
    
    data = [
        {
            "event_name": "purchase",
            "event_date": "2024-09-10",
            "event_value_in_usd": 100.0,
            "user_id": "user1",
            "user_pseudo_id": "pseudo1",
            "ecommerce_transaction_id": "trans1",
            "items": [
                {
                    "item_id": "item1",
                    "quantity": 2,
                    "price": 50.0,
                    "price_in_usd": 50.0
                }
            ]
        }
    ]
    
    with open(os.path.join(dir_name, 'test_data.json'), 'w') as f:
        json.dump(data, f)
    
    yield dir_name
    
    # Cleanup
    for file in os.listdir(dir_name):
        os.remove(os.path.join(dir_name, file))
    os.rmdir(dir_name)

def test_extract_data_sales(temp_json_dir):
    df = extract_data_sales(temp_json_dir)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'event_date' in df.columns
    assert 'ecommerce_transaction_id' in df.columns
    assert df['ecommerce_transaction_id'].iloc[0] == 'trans1'
