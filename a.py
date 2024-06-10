from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

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
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ELT_to_Data_Warehouse',
    default_args=default_args,
    description='ETL DAG for Data Warehouse',
    schedule_interval=None,  # Chạy khi được kích hoạt bởi DAG khác
    start_date=days_ago(1),
    catchup=False,
) as dag:

    wait_for_etl_to_database = ExternalTaskSensor(
        task_id='wait_for_etl_to_database',
        external_dag_id='ETL_to_Database',
        external_task_id=None,  # Chờ toàn bộ DAG hoàn thành
        timeout=600,  # Thời gian chờ tối đa 10 phút
        allowed_states=['success', 'running'],  # Chỉ cần success và running
        failed_states=['failed'],  # Chỉ cần failed
        mode='poke',  # Sử dụng chế độ poke để kiểm tra định kỳ
    )

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

    load_parquet_to_hdfs = BashOperator(
        task_id='load_parquet_to_hdfs',
        bash_command='/bin/bash /home/anhcu/Project/Stock_project/elt/scripts/load/load_parquet_to_hdfs.sh',
        xcom_push=True,
        template_ext=[]  # Disable Jinja templating for bash_command
    )

    def process_latest_files(**kwargs):
        ti = kwargs['ti']
        latest_files = ti.xcom_pull(task_ids='load_parquet_to_hdfs')
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
    wait_for_etl_to_database >> [crawl_news, crawl_ohlcs]
    [crawl_news, crawl_ohlcs] >> load_api_to_parquet
    [crawl_news, crawl_ohlcs] >> load_db_to_parquet
    [load_api_to_parquet, load_db_to_parquet] >> load_parquet_to_hdfs
    load_parquet_to_hdfs >> process_files
