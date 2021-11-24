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
    curl -o /home/project/airflow/dags/finalassignment/staging/tolldata.tgz https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz 
    tar zxvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/
    """
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= download_and_unzip_command,
    dag=dag,
)

# task 2: extract data from csv

extract_data_from_csv = BashOperator (
    task_id='extract_data_CSV',
    bash_command = 'cut -d "," -f 1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag
)

# task 3: extract data from tsv


extract_data_from_tsv = BashOperator (
    task_id='extract_data_TSV',
    bash_command = "cut -f 5-7 --output-delimiter , /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | sed $'s/[^[:print:]\t]//g'  > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag
)


# task 4: extract data from fixed-width file


extract_data_from_fixed_width = BashOperator (
    task_id='extract_data_FixedW',
    bash_command = "cut -c 59-61,63-67 --output-delimiter ','  /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    dag=dag
)


# task 5: consolidate data

consolidate_data = BashOperator (
    task_id='consolidate_data',
    bash_command = """
                paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv  > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv
                """,
    dag=dag
)

# task 6: transofrm_data
transform_data = BashOperator (
    task_id='transform_data',
    bash_command = """
                awk -F, '{$4=toupper($4)}1' OFS=, /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > transformed_data.csv
                """,
    dag=dag
)


# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
