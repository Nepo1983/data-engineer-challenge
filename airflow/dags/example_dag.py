from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

default_args = {
    'owner': 'data_engineer_candidate',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_etl_pipeline',
    default_args=default_args,
    description='Simple Hello World example',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'hello_world'],
)

def hello_world_task():
    """
    Simple Hello World task that inserts a message into the data warehouse.
    """
    print("Hello World! This is a simple Airflow DAG example.")
    
    conn = psycopg2.connect(
        host='postgres_warehouse',
        database='data_warehouse',
        user='postgres',
        password='postgres',
        port=5432
    )
    
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO connection_test (message) VALUES (%s)",
        ("Hello World from Airflow! Executed: " + str(datetime.now()),)
    )
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Successfully inserted Hello World message into data warehouse!")
    return "Hello World task completed"

hello_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_task,
    dag=dag,
)
