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