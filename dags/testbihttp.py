import requests

URL = 'http://ms-python-teradata-nirvana-qa.apps.ocptest.gp.inet/getTableData2'

def getWebQuery(query):
    request_json = {
        'type': 2,
        'query': query,
        'cache_enabled': 'true',
        'cache_refresh':'true'
    }
    result = requests.post(URL, json=request_json)
    return result

def get_dags():
    qry = f"SELECT * FROM D_EWAYA_CONFIG.GD_AFDag_HTTP WHERE state=1"
    return getWebQuery(qry).json()['result']

def get_tasks_by_dag_id(dag_id):
    qry = f"SELECT * FROM D_EWAYA_CONFIG.GD_AFTask_HTTP WHERE state=1 and dag_id={dag_id}"
    return getWebQuery(qry).json()['result']

def get_dag_by_id(dag_id):
    qry = f"SELECT * FROM D_EWAYA_CONFIG.GD_AFDag_HTTP WHERE state=1 and dag_id={dag_id}"
    return getWebQuery(qry).json()['result']

if __name__=="__main__":
    result = get_dag_by_id(1)[0]
    name = result["name"]
    description = result["description"]
    schedule_interval = int(result["schedule_interval"])
    start_date = int(result["start_date"])
    tags = result["tags"].split(",")
    
    print(result)
    print(name)
    print(description)
    print(schedule_interval)
    print(start_date)
    print(tags)

    tasks=get_tasks_by_dag_id(1)
    print(tasks)