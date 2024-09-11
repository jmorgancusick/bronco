from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param

from tasks.common import setup
from tasks.git import clone as git_clone
from tasks.terraform import init, apply as tf_apply
from tasks.tools.common import repo_name

import random

class BadLuckException(Exception):
    pass

class ReallyBadLuckException(BadLuckException):
    pass

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

@task(task_id='skill_check')
def skill_check():
    roll = random.randint(1,20)
    if roll == 1:
        print('Yikes!')
        raise ReallyBadLuckException(f'You rolled a {roll} out of 20')
    if roll <= 5:
        print('Better luck next time!')
        raise BadLuckException(f'You rolled a {roll} out of 20')

@task(task_id='init_kwargs')
def init_kwargs(params=None, task_instance=None):
    """Map params to terraform init/plan arguments, so that these tasks can be reused across many DAGs"""
    return [
        {
            'version': params['version'],
            'cwd': task_instance.xcom_pull(task_ids='clone.clone')[0],
            'options': ['-no-color', f'-backend-config=./deployments/{params["deployment"]}/backend.hcl'],
            'env': {'TF_PLUGIN_CACHE_DIR': str(Path.home() / '.terraform.d' / 'plugin-cache')},
        }
    ]

@task(task_id='apply_kwargs')
def apply_kwargs(params=None, task_instance=None):
    """Map params to terraform init/plan arguments, so that these tasks can be reused across many DAGs"""
    return [
        {
            'version': params['version'],
            'cwd': task_instance.xcom_pull(task_ids='clone.clone')[0],
            'options': ['-no-color', f'-var-file=./deployments/{params["deployment"]}/terraform.tfvars', '-auto-approve'],
        }
    ]

@task_group(group_id='apply')
def apply():
    skill_check() >> init.expand_kwargs(init_kwargs()) >> tf_apply.expand_kwargs(apply_kwargs())

@task_group(group_id='clone')
def clone():
    git_clone.expand_kwargs(clone_kwargs())

with DAG(
    'airflow_summit_demo',
    description='A DAG to apply a terraform project',
    params={
        'url': Param(
            default='https://github.com/jmorgancusick/airflow-summit-demo.git',
            type='string',
            description='The repo url to clone',
        ),
        'branch': Param(
            default='main',
            type='string',
            description='The repo branch to clone',
        ),
        'version': Param(
            default='1.9.5',
            type='string',
            description='Terraform version',
        ),
        'deployment': Param(
            default='0',
            type='string',
            description='Deployment Name',
        ),
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
    setup() >> clone() >> apply()