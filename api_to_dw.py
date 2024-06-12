from flask import Flask, jsonify, request
import duckdb

app = Flask(__name__)

# Định nghĩa các endpoint cho từng bảng

@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM dim_time'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM dim_companies'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_topics', methods=['GET'])
def get_dim_topics():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM dim_topics'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM dim_news'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/fact_candles', methods=['GET'])
def get_fact_candles():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM fact_candles'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM fact_news_companies'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

@app.route('/fact_news_topics', methods=['GET'])
def get_fact_news_topics():
    duck_conn = duckdb.connect('/home/anhcu/Project/Stock_project/datawarehouse.duckdb')
    query = 'SELECT * FROM fact_news_topics'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
