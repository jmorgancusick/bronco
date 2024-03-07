from pathlib import Path
from pprint import pprint

from airflow.decorators import task, task_group

@task(task_id='print_context')
def print_context(ds=None, **kwargs):
    pprint(kwargs)
    print(ds)

@task(task_id='workspace_init')
def workspace_init(dag=None, run_id=None):
    workspace_path = Path('/opt/airflow/workspace')
    dag_id = dag.dag_id

    dag_workspace = workspace_path / f'{dag_id=!s}'
    run_workspace = dag_workspace / f'{run_id=!s}'

    dag_workspace.mkdir(parents=True, exist_ok=True)
    run_workspace.mkdir()
    
    return str(run_workspace)

@task_group(group_id='setup')
def setup():
    print_context() >> workspace_init()