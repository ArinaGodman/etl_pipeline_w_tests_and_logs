import pandas as pd
import os
import json
import logging

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
    logging.info("Starting to extract data from directory: %s", directory)
    data_frames = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            logging.info("Processing file: %s", file_path)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
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
                    logging.info("Successfully processed file: %s", file_path)
            
            except json.JSONDecodeError as e:
                logging.error("JSON decode error for file %s: %s", file_path, e)
            except Exception as e:
                logging.error("Error processing file %s: %s", file_path, e)
    
    if not data_frames:
        logging.error("No valid data found in the specified directory: %s", directory)
        raise Exception("No valid data found in the specified directory.")
    
    combined_df = pd.concat(data_frames, ignore_index=True)
    logging.info("Data extraction completed successfully. Total records extracted: %d", len(combined_df))
    return combined_df
