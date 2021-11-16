# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'HaPhan Tran',
    'start_date': days_ago(0),
    'email': ['haphantran@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# defining the DAG

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task: unzip data

download_and_unzip_command = """
    wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -P /home/project/airflow/dags/finalassignment/staging
    tar zxvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz

    """
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= download_and_unzip_command,
    dag=dag,
)

# task 2: extract data from csv

extract_data_from_csv = BashOperator (
    task_id='extract_data',
    bash_command = 'cut -d "," -f 1-4 vehicle-data.csv > csv_data.csv',
    dag=dag
)

# task 3: extract data from tsv


extract_data_from_tsv = BashOperator (
    task_id='extract_data',
    bash_command = 'cut  -f 5-7 --output-delimiter "," tollplaza-data.tsv > tsv_data.csv',
    dag=dag
)


# task 4: extract data from fixed-width file


extract_data_from_fixed_width = BashOperator (
    task_id='extract_data',
    bash_command = 'cut -c 59-61,63-67 --output-delimiter ","  payment-data.txt > fixed_width_data.csv',
    dag=dag
)


# task 5: consolidate data

consolidate_data = BashOperator (
    task_id='extract_data',
    bash_command = 'paste -d "," csv_data.csv tsv_data.csv fixed_width_data.csv  > extracted_data.csv',
    dag=dag
)
