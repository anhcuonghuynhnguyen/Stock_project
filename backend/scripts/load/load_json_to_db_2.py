import psycopg2
import json
import os

def get_latest_file_in_directory(directory, extension):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def insert_data_from_json(file_path, table_name, columns, conflict_columns):
    with open(file_path, 'r') as file:
        data = [json.loads(line) for line in file]

    if not data:
        print(f"No data found in {file_path}")
        return

    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)
    
    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns_str}) DO NOTHING
    """

    conn = psycopg2.connect(
        host="localhost",
        database="datasource",
        user="anhcu",
        password="admin"
    )
    cur = conn.cursor()
    
    for record in data:
        values = [record[col] for col in columns]
        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted data into {table_name}")

if __name__ == "__main__":
    insert_data_from_json(
        get_latest_file_in_directory('/home/anhcu/Project/Stock_project/backend/data/processed/transformed_to_database_exchanges','.json'),
        'exchanges',
        ['exchange_region_id', 'exchange_name'],
        ['exchange_name']
    )