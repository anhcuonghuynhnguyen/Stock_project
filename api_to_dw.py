from flask import Flask, jsonify, request
import duckdb

# Create a new Flask application
app = Flask(__name__)

# Define an endpoint to retrieve data from the 'dim_time' table
@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select all data from the 'dim_time' table
    query = 'SELECT * FROM dim_time'
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_companies' table
@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select specific columns from the 'dim_companies' table
    query = '''
                SELECT company_id, company_name, company_time_stamp, company_ticket, 
                    company_is_delisted, company_exchange_name, company_industry_name, 
                    company_industry_sector, company_sic_industry, company_sic_sector 
                FROM dim_companies
            '''
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_topics' table
@app.route('/dim_topics', methods=['GET'])
def get_dim_topics():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select all data from the 'dim_topics' table
    query = 'SELECT * FROM dim_topics'
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_news' table
@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select specific columns from the 'dim_news' table
    query = '''
                SELECT new_id, new_title, new_url, new_time_published, new_authors, new_source, 
                        new_overall_sentiment_score, new_overall_sentiment_label 
                FROM dim_news
            '''
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Define an endpoint to retrieve data from the 'fact_news_companies' table
@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select all data from the 'fact_news_companies' table
    query = 'SELECT * FROM fact_news_companies'
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Define an endpoint to retrieve data from the 'fact_news_topics' table
@app.route('/fact_news_topics', methods=['GET'])
def get_fact_news_topics():
    # Connect to the DuckDB database
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    # SQL query to select all data from the 'fact_news_topics' table
    query = 'SELECT * FROM fact_news_topics'
    # Execute the query and fetch all results
    result = duck_conn.execute(query).fetchall()
    # Get the column names from the query result
    columns = [desc[0] for desc in duck_conn.description]
    # Combine column names and row data into a list of dictionaries
    data = [dict(zip(columns, row)) for row in result]
    # Close the database connection
    duck_conn.close()
    # Return the data as a JSON response
    return jsonify(data)

# Run the application on the specified host and port
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
