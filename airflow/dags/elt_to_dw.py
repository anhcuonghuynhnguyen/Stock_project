from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import subprocess
import logging

# Add the paths to sys.path
sys.path.append('/home/anhcu/Project/Stock_project/elt/scripts/transform')
import transform_to_datawarehouse_1
import transform_to_datawarehouse_2
import transform_to_datawarehouse_3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ELT_to_Data_Warehouse',
    default_args=default_args,
    description='ETL DAG for Data Warehouse',
    schedule_interval='1 1 * * *',  # Chạy khi được kích hoạt bởi DAG khác
    start_date=days_ago(1),
    catchup=False,
) as dag:
    crawl_news = BashOperator(
        task_id='crawl_news',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/elt/scripts/extract/crawl_news.py',
    )

    crawl_ohlcs = BashOperator(
        task_id='crawl_ohlcs',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/elt/scripts/extract/crawl_ohlcs.py',
    )

    load_api_to_parquet = BashOperator(
        task_id='load_api_to_parquet',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/elt/scripts/load/load_api_to_parquet.py',
    )

    load_db_to_parquet = BashOperator(
        task_id='load_db_to_parquet',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/elt/scripts/load/load_db_to_parquet.py',
    )

    def run_shell_script():
        result = subprocess.run(['bash', '/home/anhcu/Project/Stock_project/elt/scripts/load/load_parquet_to_hdfs.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise Exception(f"Shell script failed with error: {result.stderr.decode()}")
        output = result.stdout.decode()
        print(output)
        return output
        
    load_parquet_to_hdfs = PythonOperator(
        task_id='load_parquet_to_hdfs',
        python_callable=run_shell_script,
        do_xcom_push=True
    )

    # load_parquet_to_hdfs = BashOperator(
    #     task_id='load_parquet_to_hdfs',
    #     bash_command='bash /home/anhcu/Project/Stock_project/elt/scripts/load/load_parquet_to_hdfs.sh',
    #     do_xcom_push=True,  # Sử dụng do_xcom_push để đẩy kết quả lên XCom
    # )

    def process_latest_files(**kwargs):
        ti = kwargs['ti']
        latest_files = ti.xcom_pull(task_ids='load_parquet_to_hdfs')
        
        if latest_files is None:
            logging.error("No files were pulled from XCom. Check the 'load_parquet_to_hdfs' task.")
            raise ValueError("No files were pulled from XCom. Check the 'load_parquet_to_hdfs' task.")
        
        logging.info(f"Files pulled from XCom: {latest_files}")

        file_list = latest_files.strip().split('\n')
                
        for file_path in file_list:
            if "datalake/companies" in file_path:
                process_companies(file_path)
            elif "datalake/ohlcs" in file_path:
                process_ohlcs(file_path)
            elif "datalake/news" in file_path:
                process_news(file_path)

    def process_companies(hdfs_path):
        transform_to_datawarehouse_1.process(hdfs_path)

    def process_ohlcs(hdfs_path):
        transform_to_datawarehouse_2.process(hdfs_path)

    def process_news(hdfs_path):
        transform_to_datawarehouse_3.process(hdfs_path)

    process_files = PythonOperator(
        task_id='process_latest_files',
        python_callable=process_latest_files,
        provide_context=True,
    )

    # Định nghĩa thứ tự chạy các task
    crawl_news >> load_api_to_parquet
    crawl_ohlcs >> load_api_to_parquet
    crawl_news >> load_db_to_parquet
    crawl_ohlcs >> load_db_to_parquet
    load_api_to_parquet >> load_parquet_to_hdfs
    load_db_to_parquet >> load_parquet_to_hdfs
    load_parquet_to_hdfs >> process_files
