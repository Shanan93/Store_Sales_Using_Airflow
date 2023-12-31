def data_cleaner():
    # Import necessary libraries
    import pandas as pd
    import re
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv("~/store_files_airflow/raw_store_transactions.csv")
    # Define a function to clean store location by removing non-alphanumeric characters
    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()
    # Define a function to clean product ID by extracting the first sequence of digits
    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id
    # Define a function to remove dollar signs and convert to float
    def remove_dollar(amount):
        return float(amount.replace('$', ''))
    # Apply the clean_store_location function to the 'STORE_LOCATION' column
    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    # Apply the clean_product_id function to the 'PRODUCT_ID' column
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))
    # Iterate over specified columns and apply the remove_dollar function to remove dollar signs and convert to float
    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
    # Write the cleaned DataFrame to a new CSV file
    df.to_csv('~/store_files_airflow/clean_store_transactions.csv', index=False)

