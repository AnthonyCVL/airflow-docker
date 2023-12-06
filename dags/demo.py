from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import logging
import testbi

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def process_ftp():
    """
    fw_generico.ftp()
    """
    logging.info("ftp")

def process_fastload():
    """
    fw_generico.fastload()
    """
    logging.info("fastload")

def process_transformation():
    """
    fw_generico.rn()
    """
    logging.info("transformation")

def process_storeprocedure():
    """
    fw_generico.rn()
    """
    logging.info("storeprocedure")

def select_process(n):
    if n==1:
        process_ftp()
    elif n==2:
        process_fastload()
    elif n==3:
        process_transformation()
    elif n==4:
        process_storeprocedure()

def create_task(task_name, sequence):
    def task_function(task_name):
        select_process(task_name)

    return PythonOperator(
        task_id=f'{task_name}_{sequence}',
        python_callable=task_function
    )

# Utiliza un operador BranchPython para decidir la siguiente tarea en función del progreso
def decide_next_task(**kwargs):
    ti = kwargs['ti']
    for task in task_list:
        if not ti.get_task_instance(task).current_state() == "success":
            return task.task_id
    return "end"

def get_tasks():
    tasks = [1, 2, 4, 3, 2, 4]
    return tasks

with DAG(
    'demo',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example']
) as dag:
    start = PythonOperator(task_id="start", python_callable=lambda: logging.info("Comienzo"))
    end = PythonOperator(task_id="end", python_callable=lambda: logging.info("Fin"))
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=decide_next_task,
    )

    tasks = get_tasks()

    # Crear tareas dinámicamente en función del arreglo
    # task_list = [create_task(task_name) for task_name in tasks]
    task_list = [create_task(task_name, sequence) for sequence, task_name in enumerate(tasks, start=1)]

    # Define la secuencia de ejecución de tareas
    start >> task_list[0] >> branch_task
    for i in range(len(task_list) - 1):
        branch_task >> task_list[i + 1]
    branch_task >> end
