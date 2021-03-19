from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello():
    return 'Hello world!'


def goodbye():
    return 'goodbye everyone'

greeting_dag = DAG(dag_id = 'greeting_dag', start_date=datetime.now())

goodbye_task = PythonOperator(task_id='goodbye_task',
                            python_callable=goodbye,
                            dag=greeting_dag)

hello_task = PythonOperator(task_id='hello_task', 
                            python_callable=hello,
                            dag=greeting_dag)

hello_task >> goodbye_task
