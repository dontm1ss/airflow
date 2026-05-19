from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

def process_data():
    print("Обработка данных началась...")
    data = [10, 20, 30, 40, 50]
    processed = [x * 2 for x in data]
    print(f"Результат обработки: {processed}")
    
    # Сохраняем результат в файл внутри контейнера
    result_path = "/tmp/airflow_lab2/result.txt"
    with open(result_path, "w") as f:
        from datetime import datetime
        f.write(f"Результат: {processed}\nВремя: {datetime.now()}")
    
    return processed

default_args = {
    'owner': 'andrey',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_processing_lab2',
    default_args=default_args,
    description='DAG для обработки данных',
    schedule='@daily',  # ИЗМЕНЕНО: вместо schedule_interval
    catchup=False,
    tags=['lab2'],
)

create_dir = BashOperator(
    task_id='create_temp_dir',
    bash_command='mkdir -p /tmp/airflow_lab2',
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

write_result = BashOperator(
    task_id='write_result_to_file',
    bash_command='echo "Обработка завершена в $(date)" >> /tmp/airflow_lab2/result.txt',
    dag=dag,
)

create_dir >> process_task >> write_result