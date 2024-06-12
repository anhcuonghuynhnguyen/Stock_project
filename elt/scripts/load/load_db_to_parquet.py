import pandas as pd
import datetime
from sqlalchemy import create_engine

# Connection details for PostgreSQL
DATABASE_TYPE = 'postgresql'
ENDPOINT = 'localhost'  # Address of the PostgreSQL server
USER = 'anhcu'  # PostgreSQL username
PASSWORD = 'admin'  # PostgreSQL password
PORT = 5432  # Default port for PostgreSQL
DATABASE = 'datasource'  # Name of the database

# Create an engine to connect to PostgreSQL
engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

# Read SQL query from a file
def read_query_from_file(file_path):
    # Open and read the content of the SQL file
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Execute the query and save the result to a Parquet file
def query_to_parquet(query, conn, parquet_file_path):
    # Execute the query and fetch the result into a DataFrame
    df = pd.read_sql(query, conn)
    print(df.info())
    
    # Save the DataFrame to a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow')

# Path to the SQL query file
query_file_path = r'/home/anhcu/Project/Stock_project/elt/scripts/extract/extract_db_to_parquet.sql'

# Path to the output Parquet file
date = datetime.date.today().strftime("%Y_%m_%d")
parquet_file_path = r'/home/anhcu/Project/Stock_project/elt/data/completed/load_db_to_dl/load_db_to_dl_' + f"{date}.parquet"

# Read the SQL query from the file
query = read_query_from_file(query_file_path)

# Execute the query and save the result to a Parquet file
query_to_parquet(query, engine, parquet_file_path)
print(f"Saved data from database to parquet successfully at {parquet_file_path}")