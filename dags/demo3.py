from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import logging
import json
import testbihttp
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

data = {
    "dag_id": 1,
    "dag_name": "dag_main",
    "tasks": [
        {
            "id": 1,
            "name": "primero",
            "type": 1,
            "predecessor": []
        },
        {
            "id": 2,
            "name": "segundo",
            "type": 1,
            "predecessor": []
        },
        {
            "id": 3,
            "name": "tercero",
            "type": 2,
            "predecessor": [1]
        },
        {
            "id": 4,
            "name": "cuarto",
            "type": 2,
            "predecessor": [2]
        },
        {
            "id": 5,
            "name": "quinto",
            "type": 3,
            "predecessor": [3,4]
        }        
    ]
}

def process_ftp(layout, fecha):
    """
    fw_generico.ftp()
    """
    logging.info("FTP Ejecutándose")

def process_fastload(layout, fecha):
    logging.info("fastload")
    URL = 'http://host.docker.internal:5000/fg_fastload'
    request_json = {
        'layout': layout,
        'fecha': fecha
    }
    response = requests.post(URL, json=request_json)
    if response.status_code == 200:  # Verificar si la solicitud fue exitosa
        result = response.json()  # Obtener el resultado como JSON
        # Hacer algo con 'result', como imprimirlo
        logging.info(result)
    else:
        logging.info(f"Error en la solicitud: {response.status_code}")
    logging.info("END fastload")

def process_transformation(layout, fecha):
    logging.info("START transformation")
    URL = 'http://host.docker.internal:5000/fg_transformation'
    request_json = {
        'layout': layout,
        'fecha': fecha
    }
    response = requests.post(URL, json=request_json)
    if response.status_code == 200:  # Verificar si la solicitud fue exitosa
        result = response.json()  # Obtener el resultado como JSON
        # Hacer algo con 'result', como imprimirlo
        logging.info(result)
    else:
        logging.info(f"Error en la solicitud: {response.status_code}")
    logging.info("END transformation")

def process_storeprocedure(layout, fecha):
    logging.info("START storeprocedure")
    URL = 'http://host.docker.internal:5000/fg_storeprocedure'
    request_json = {
        'layout': layout,
        'fecha': fecha
    }
    response = requests.post(URL, json=request_json)
    if response.status_code == 200:  # Verificar si la solicitud fue exitosa
        result = response.json()  # Obtener el resultado como JSON
        # Hacer algo con 'result', como imprimirlo
        logging.info(result)
    else:
        logging.info(f"Error en la solicitud: {response.status_code}")
    logging.info("END storeprocedure")

def select_process(n, layout, fecha):
    if n==1:
        process_ftp(layout, fecha)
    elif n==2:
        process_fastload(layout, fecha)
    elif n==3:
        process_transformation(layout, fecha)
    elif n==4:
        process_storeprocedure(layout, fecha)

def select_process_http(n):
    if n==1:
        return "fg_ftp"
    elif n==2:
        return "fg_fastload"
    elif n==3:
        return "fg_transformation"
    elif n==4:
        return "fg_storeprocedure"

def create_task(task_name, task_type, layout, fecha):
    def task_function(n, layout, fecha):
        select_process(n, layout, fecha)

    return PythonOperator(
        task_id=f'{task_name}',
        python_callable=task_function,
        op_args=[task_type, layout, fecha]
    )

def get_tasks(dag_id):
    tasks = testbihttp.get_tasks_by_dag_id(dag_id)
    return tasks

def get_taskname_by_id(tasks, task_id):
    for task in tasks:
        if task["task_id"] == task_id:
            return task["name"]
    return None 

def schedule_tasks():
    # Obtiene las tareas
    tasks = get_tasks()

    # Crear tareas dinámicamente en función del arreglo
    task_list = [create_task(task_name, sequence) for sequence, task_name in enumerate(tasks, start=1)]

    for i in range(len(task_list) - 1):
        task_list[i] >> task_list[i + 1]

def schedule_tasks2(dag_id):
    task_data=get_tasks(dag_id)
    
    # Crea un diccionario para almacenar las tareas y sus objetos PythonOperator
    tasks_dict = {}
    
    # Crea las tareas y almacena los objetos PythonOperator en el diccionario
    for task_info in task_data:
        task_name = get_taskname_by_id(task_data, task_info['task_id'])
        task = create_task(task_name,task_info['task_type'],task_info['layout'],task_info['fecha'])
        tasks_dict[task_name] = task
    # Configura las dependencias entre las tareas basadas en los predecesores
    for task_info in task_data:
        task_name = get_taskname_by_id(task_data, task_info['task_id'])
        predecessors =  [int(valor) for valor in task_info['predecessor'].split(',')] if task_info['predecessor'] else []
        if predecessors:
            for predecessor_id in predecessors:
                task_predecessor_name = get_taskname_by_id(task_data, predecessor_id)
                #tasks_dict[task_name] >> tasks_dict[task_predecessor_name]
                tasks_dict[task_predecessor_name] >> tasks_dict[task_name] 

result = testbihttp.get_dags()

for row in result:
    name = 'HTTP'+row["name"]
    description = row["description"]
    schedule_interval = int(row["schedule_interval"])
    start_date = int(row["start_date"])
    tags = row["tags"].split(",")

    with DAG(
        name,
        default_args=default_args,
        description=description,
        schedule_interval=timedelta(days=schedule_interval),
        start_date=days_ago(start_date),
        tags=tags
    ) as dag:
        schedule_tasks2(row["dag_id"])
