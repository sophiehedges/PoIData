from sqlalchemy import create_engine
import pandas as pd
import os

# SQL Server connection setup (Windows Authentication)
server = 'YourServerName' # Update with your server name
database = 'PointOfInterest'

# Create an engine using SQLAlchemy for Windows Authentication
engine = create_engine(f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

# Base folder path
base_folder = r'C:\YourFilePathHere\PoIData' # Update with your file path here

# Subfolders for each city
folders = {
    'Manchester': os.path.join(base_folder, 'Machester'),  # note the misspelling "Machester" (sic)
    'Birmingham': os.path.join(base_folder, 'Birmingham')
}

# Loop through each city folder
for city, folder_path in folders.items():
    print(f"Processing city: {city}")  # Print the city being processed
    # Loop through each subfolder (which should have the format YYYY-MM-City-poi_XXXX)
    for subfolder in os.listdir(folder_path):
        subfolder_path = os.path.join(folder_path, subfolder)
        if os.path.isdir(subfolder_path):
            # Loop through each CSV file in the subfolder
            for filename in os.listdir(subfolder_path):
                if filename.endswith('.csv') and filename.startswith('poi-extract-'):
                    full_path = os.path.join(subfolder_path, filename)
                    print(f"Processing file: {filename} from folder {subfolder}")  # Print the file being processed
                    
                    # Load the CSV data into a DataFrame
                    df = pd.read_csv(full_path, delimiter='|')  # Adjust delimiter if needed
                    print("Data read successfully.")

                    # Ensure ref_no and supply_date are the same type in both dataframes
                    df['ref_no'] = df['ref_no'].astype(str)
                    df['supply_date'] = pd.to_datetime(df['supply_date'])

                    # Determine the correct table based on the folder (city)
                    if city == 'Manchester':
                        table_name = 'POI_Manchester'
                    elif city == 'Birmingham':
                        table_name = 'POI_Birmingham'

                    # Fetch existing ref_no and supply_date from the database
                    existing_data_query = f"SELECT ref_no, supply_date FROM {table_name}"
                    existing_data = pd.read_sql(existing_data_query, engine)

                    # Ensure the columns in existing_data have the same data type as in df
                    existing_data['ref_no'] = existing_data['ref_no'].astype(str)
                    existing_data['supply_date'] = pd.to_datetime(existing_data['supply_date'])

                    # Remove duplicates by checking if ref_no and supply_date exist in the database
                    filtered_df = pd.merge(df, existing_data, on=['ref_no', 'supply_date'], how='left', indicator=True)
                    filtered_df = filtered_df[filtered_df['_merge'] == 'left_only'].drop(columns=['_merge'])

                    # Check if there is data left to insert after removing duplicates
                    if not filtered_df.empty:
                        print(f"Inserting {len(filtered_df)} records into {table_name}...")
                        # Import the filtered data into the correct SQL table
                        filtered_df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
                        print(f"Imported {len(filtered_df)} records from {filename} into {table_name}.")
                    else:
                        print(f"No new records to insert from {filename}.")
