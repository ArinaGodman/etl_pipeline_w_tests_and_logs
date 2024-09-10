
import pandas as pd
import logging

def transform_data_sales(df):
    """
    Transform sales data to create both summary and detail views.

    Args:
        df (pandas.DataFrame): Raw sales data.

    Returns:
        tuple: A tuple containing the transformed summary dataframe and the detailed dataframe.
    """
    
    logging.info("Starting data transformation.")
    
    # Record the initial size of the DataFrame
    initial_size = len(df)
    logging.info("Initial DataFrame size: %d records", initial_size)
    
    try:
        # Perform data type conversions and transformations
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

        df = df.dropna(subset=['item_id'])
        
        # Record the size of the DataFrame after transformation
        final_size = len(df)
        logging.info("Data transformation completed. Initial size: %d records, Final size: %d records", initial_size, final_size)
        
    except Exception as e:
        logging.error("Error during data transformation: %s", e)
        raise
    
    return df
