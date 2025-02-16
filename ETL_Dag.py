#This DAG will extract the data from CSV file and transforms it by converting names to uppercase, and loads the results by saving them to a new file.

import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# File paths
BASE_DIR = os.path.dirname(__file__)  # Airflow DAGs folder
INPUT_FILE = os.path.join(BASE_DIR, "data.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "transformed_data.csv")

# Extract function: Read data from CSV
def extract():
    with open(INPUT_FILE, mode='r') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data  # Data will be passed via XCom

# Transform function: Convert names to uppercase
def transform(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    
    transformed_data = [
        {'id': row['id'], 'name': row['name'].upper(), 'age': row['age']}
        for row in extracted_data
    ]
    
    ti.xcom_push(key='transformed_data', value=transformed_data)

# Load function: Save transformed data to a new file
def load(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    with open(OUTPUT_FILE, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["id", "name", "age"])
        writer.writeheader()
        writer.writerows(transformed_data)

# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 9),
    'retries': 2,
}

with DAG(
    dag_id='file_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
