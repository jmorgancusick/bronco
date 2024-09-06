import airflow_client.client

from airflow_client.client.api import dag_run_api
from airflow_client.client.model.dag_run import DAGRun

from datetime import datetime

num_runs = 10

dag_id = 'blogpost'

client_config = airflow_client.client.Configuration(
    host="http://localhost:8080/api/v1",
    username='airflow',
    password='airflow'
)

run_config = {
    'url': 'https://github.com/rearc/sample-terraform.git',
    'branch': 'main',
    'version': '1.7.4',
}

dt_str = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")

for i in range(num_runs):
    run_id = f'{dt_str}_{i}'

    with airflow_client.client.ApiClient(client_config) as api_client:
        dag_run_api_instance = dag_run_api.DAGRunApi(api_client)
        try:
            dag_run = DAGRun(
                dag_run_id=run_id,
                conf=run_config
            )
            api_response = dag_run_api_instance.post_dag_run(dag_id, dag_run)
        except Exception as e:
            print(f'Ran into exception: {e}')