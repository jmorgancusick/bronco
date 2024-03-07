from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param

from tasks.common import setup
from tasks.git import clone as git_clone
from tasks.terraform import init, plan as tf_plan
from tasks.tools.common import repo_name

@task(task_id='clone_kwargs')
def clone_kwargs(params=None, task_instance=None):
    """Map params to clone arguments, so that the clone task can be reused across many DAGs"""
    return [
        {
            'url': params['url'],
            'to_path': str(Path(task_instance.xcom_pull(task_ids='setup.workspace_init')) / repo_name(params['url'])),
            'branch': params['branch'],
        }
    ]

@task(task_id='tf_kwargs')
def tf_kwargs(params=None, task_instance=None):
    """Map params to terraform init/plan arguments, so that these tasks can be reused across many DAGs"""
    return [
        {
            'version': params['version'],
            'cwd': task_instance.xcom_pull(task_ids='clone.clone')[0]
        }
    ]

@task_group(group_id='plan')
def plan():
    tf_kwargs_t = tf_kwargs()
    init.expand_kwargs(tf_kwargs_t) >> tf_plan.expand_kwargs(tf_kwargs_t)

@task_group(group_id='clone')
def clone():
    git_clone.expand_kwargs(clone_kwargs())

with DAG(
    'simple_plan',
    description='A DAG to plan a terraform project',
    params={
        'url': Param(
            default='https://github.com/rearc/sample-terraform.git',
            type='string',
            description='The repo url to clone',
        ),
        'branch': Param(
            default='main',
            type='string',
            description='The repo branch to clone',
        ),
        'version': Param(
            default='1.7.4',
            type='string',
            description='Terraform version',
        )
    },
    default_args={
        'execution_timeout': timedelta(minutes=180),
        'trigger_rule': 'none_failed_min_one_success',
        'weight_rule': 'upstream', # depth-first execution
    },
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['terraform']
) as dag:
    setup() >> clone() >> plan()