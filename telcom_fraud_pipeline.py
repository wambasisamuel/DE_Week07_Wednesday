import pandas as pd
import pymongo
import logging

# Extraction function
def extract_data(call_logs_path, billing_data_path):
    # Load call log data from CSV file
    call_logs = pd.read_csv(call_logs_path)

    # Load billing data from CSV file
    billing_data = pd.read_csv(billing_data_path)

    # create common column name
    call_logs = call_logs.rename(columns={"call_date": "date"})
    billing_data = billing_data.rename(columns={"transaction_date": "date"})

    # Merge the two datasets based on common columns
    merged_data = pd.merge(call_logs, billing_data, on=['date'])

    # Convert call duration to minutes for easier analysis
    merged_data['duration_minutes'] = merged_data['call_duration'] / 60

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data extraction completed.")

    return merged_data

# Transformation function
def transform_data(df):
    # Data cleaning and handling missing values
    df.drop_duplicates(inplace=True)
    df.fillna(value={'call_type': 'unknown', 'call_duration': 0}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])

    print(df.columns)

    # Group and aggregate the data
    grouped_data = df.groupby(['customer_id', pd.Grouper(key='date', freq='D')]).agg({
    'duration_minutes': ['sum', 'mean', 'max'],
    'call_type': ['count', 'nunique']})
    grouped_data.reset_index(inplace=True)
    grouped_data.columns = ['customer_id', 'date', 'total_minutes', 'mean_minutes', 'max_minutes',
                        'total_calls', 'unique_calls']

    # Identify patterns in the data
    suspicious_data = grouped_data[(grouped_data['total_minutes'] >= 1000) | (grouped_data['total_calls'] >= 100)]
    # Use data compression techniques to optimize performance
    suspicious_data['customer_id'] = pd.to_numeric(suspicious_data['customer_id'], downcast='unsigned')

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data transformation completed.")
    
    return suspicious_data

# Loading function
def load_data(transformed_data, host, port, db_name, collection_name):
    # Connect to MongoDB
    client = pymongo.MongoClient(host, port, ssl=True)
    db = client[db_name]
    collection = db[collection_name]

    # Create indexes on the collection
    collection.create_index([('customer_id', pymongo.ASCENDING)])
    collection.create_index([('date', pymongo.ASCENDING)])

    # Use bulk inserts to optimize performance
    bulk = collection.initialize_unordered_bulk_op()
    for index, row in data.iterrows():
      document = row.to_dict()
      bulk.insert(document)
    

    # Use the write concern option to ensure that data is written to disk
    bulk.execute({'w': 1, 'j': True})

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data loading completed.")

# Example usage
if __name__ == '__main__':
    call_logs_path = 'https://raw.githubusercontent.com/wambasisamuel/DE_Week07_Wednesday/main/call_logs.csv'
    billing_data_path = 'https://raw.githubusercontent.com/wambasisamuel/DE_Week07_Wednesday/main/billing_systems.csv'
    data = extract_data(call_logs_path, billing_data_path)
    transformed_data = transform_data(data)
    host = '104.131.120.201'
    port = 27017
    db_name = 'frauddb'
    collection_name = 'billing'
    load_data(transformed_data, host, port, db_name, collection_name)