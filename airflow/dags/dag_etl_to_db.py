from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Định nghĩa các tham số mặc định của DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['anhcuonghuynhnguyen@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    dag_id='Manual_ETL_to_Database',
    default_args=default_args,
    description='A manual ETL DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Crawl companies
    crawl_companies = BashOperator(
        task_id='crawl_companies',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/extract/crawl_companies.py',
    )

    # Task 2: Crawl markets
    crawl_markets = BashOperator(
        task_id='crawl_markets',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/extract/crawl_markets.py',
    )

    # Task 3: Transform to database 1
    transform_to_database_1 = BashOperator(
        task_id='transform_to_database_1',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/transform/transform_to_database_1.py',
    )

    # Task 4: Load JSON to DB 1
    load_json_to_db_1 = BashOperator(
        task_id='load_json_to_db_1',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/load/load_json_to_db_1.py',
    )

    # Task 5: Transform to database 2
    transform_to_database_2 = BashOperator(
        task_id='transform_to_database_2',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/transform/transform_to_database_2.py',
    )

    # Task 6: Load JSON to DB 2
    load_json_to_db_2 = BashOperator(
        task_id='load_json_to_db_2',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/load/load_json_to_db_2.py',
    )

    # Task 7: Transform to database 3
    transform_to_database_3 = BashOperator(
        task_id='transform_to_database_3',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/transform/transform_to_database_3.py',
    )

    # Task 8: Load JSON to DB 3
    load_json_to_db_3 = BashOperator(
        task_id='load_json_to_db_3',
        bash_command='/bin/python3 /home/anhcu/Project/Stock_project/backend/scripts/load/load_json_to_db_3.py',
    )

    # Task 9: Send email on success
    send_email = EmailOperator(
        task_id='send_email',
        to='anhcuonghuynhnguyen@gmail.com',
        subject='DAG Manual_ETL_to_Database Succeeded',
        html_content='<p>The DAG Manual_ETL_to_Database has completed successfully.</p>',
    )

    # Định nghĩa thứ tự chạy các task
    [crawl_companies, crawl_markets] >> transform_to_database_1 >> load_json_to_db_1 >> transform_to_database_2 >> load_json_to_db_2 >> transform_to_database_3 >> load_json_to_db_3 >> send_email
