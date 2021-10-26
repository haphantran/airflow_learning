# Airflow

Airflow DAG is a python program. It consists of these logical blocks.
- Imports
- DAG Arguments
- DAG Definition
- Task Definitions
- Task Pipeline

## Imports
Example
```python

# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
```

## DAG Arguments

```python
default_args = {
    'owner': 'HaPhan Tran',
    'start_date': days_ago(0),
    'email': ['haphantran@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

## DAG definition
Main information about DAG: name, description, scheduleetc...

```python
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL read server log',
    schedule_interval=timedelta(days=1),
)
```

## Task Definitions
Information about task id, command, which dag the task belongs...
```python
extract_data = BashOperator(
    task_id='extract',
    bash_command='cut server_log.txt -d "#" -f 1,4',
    dag=dag,
)
```

## Task Pipeline
After define all the tasks, you need to specify the sequence that those tasks will be triggered.

```python
# task pipeline
download >> extract_data >> transform_data >> load_data
```

