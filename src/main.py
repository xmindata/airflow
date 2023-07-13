# add the basedir
import os
import sys
# set up the base dir
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.data_collection import collect_data
from src.data_preprocessing import process_data
from src.train_model import train_model
from src.inference import make_predictions


dag = DAG(
    'iot_device_failure',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    tags=['iot_sensor_failure']
)

task1 = PythonOperator(
    task_id='collect_data',
    python_callable=collect_data,
    op_kwargs={'file_path': f'{base_dir}/data/from/data.csv', 'url': None},
    dag=dag
)

task2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={
        'input_file_path': f'{base_dir}/data/from/data.csv', 
        'output_train_data_path': f'{base_dir}/data/to/train_data', 
        'output_test_data_path': f'{base_dir}/data/to/test_data',
        'scaler_output_path': f'{base_dir}/data/to/pipeline_model'
    },
    dag=dag
)

task3 = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    op_kwargs={
        'input_train_data_path': f'{base_dir}/data/to/train_data',
        'input_test_data_path': f'{base_dir}/data/to/test_data',
        'output_file_path': f'{base_dir}/data/to/model',
        'scaler_input_path': f'{base_dir}/data/to/pipeline_model'
    },
    dag=dag
)

task4 = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    op_kwargs={
        'input_file_path': f'{base_dir}/data/to/new_data.csv',
        'model_input_path': f'{base_dir}/data/to/model',
        'scaler_input_path': f'{base_dir}/data/to/pipeline_model',
        'output_file_path': f'{base_dir}/data/to/predictions.csv'
    },
    dag=dag
)

task1 >> task2 >> task3 >> task4