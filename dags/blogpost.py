from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.edgemodifier import Label
from airflow.models.param import Param

from tasks.common import print_context, workspace_init
from tasks.git import clone as git_clone
from tasks.terraform import init, plan as tf_plan
from tasks.tools.common import repo_name

import random
import time

class BadLuckException(Exception):
    pass

class ReallyBadLuckException(BadLuckException):
    pass

@task(task_id='clone')
def clone():
    time.sleep(random.randint(5, 15))

@task_group(group_id='setup')
def setup():
    print_context() >> workspace_init() >> clone()

@task.branch(task_id='check_terraform_version')
def check_terraform_version():
    options = ["init_0.14", "generate_tags", "generate_tags", "generate_tags", "generate_tags", "generate_tags"]
    time.sleep(random.randint(1, 2))
    return f'plan.{random.choice(options)}'

@task(task_id='generate_tags')
def generate_tags():
    time.sleep(random.randint(5, 15))

@task(task_id='init_0.14')
def init_0_14():
    roll = random.randint(1,20)
    if roll == 1:
        print('Yikes!')
        raise ReallyBadLuckException(f'You rolled a {roll} out of 20')
    if roll <= 5:
        print('Better luck next time!')
        raise BadLuckException(f'You rolled a {roll} out of 20')

    time.sleep(random.randint(5, 15))

@task(task_id='plan_0.14')
def plan_0_14():
    time.sleep(random.randint(5, 15))

@task(task_id='plan_1.7')
def plan_1_7():
    time.sleep(random.randint(5, 15))

@task_group(group_id='plan')
def plan():
    check_terraform_version_t = check_terraform_version() 
    init_0_14_t = init_0_14()
    
    check_terraform_version_t >> Label('0.13') >>  generate_tags() >> init_0_14_t
    check_terraform_version_t >> Label('0.14') >> init_0_14_t

    init_0_14_t >> plan_0_14() >> plan_1_7()

@task(task_id='open_pr')
def open_pr():
    time.sleep(random.randint(5, 15))

@task(task_id='is_pr_merged')
def is_pr_merged():
    time.sleep(random.randint(5, 15))

@task_group(group_id='quality_control')
def quality_control():
    open_pr() >> is_pr_merged()

@task(task_id='jenkins_push')
def jenkins_push():
    time.sleep(random.randint(5, 15))

@task(task_id='jenkins_apply')
def jenkins_apply():
    time.sleep(random.randint(5, 15))

@task_group(group_id='apply')
def apply():
    jenkins_push() >> jenkins_apply()

with DAG(
    'blogpost',
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
    setup() >> plan() >> quality_control() >> apply()