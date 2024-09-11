from pathlib import Path

from airflow.decorators import task
from airflow.hooks.subprocess import SubprocessHook

import os

@task(task_id='init')
def init(version, cwd, options=[], env={}):
    tf_bin = Path('/opt/airflow/terraform') / version / 'terraform'
    
    result = SubprocessHook().run_command(command=[tf_bin, 'init', *options], cwd=cwd, env=(env|dict(os.environ)))
    if result.exit_code:
        raise RuntimeError(f'Terraform init failed with exit code: {result.exit_code}')

@task(task_id='plan')
def plan(version, cwd, options=[], env={}):
    tf_bin = Path('/opt/airflow/terraform') / version / 'terraform'

    result = SubprocessHook().run_command(command=[tf_bin, 'plan', *options], cwd=cwd, env=(env|dict(os.environ)))
    if result.exit_code:
        raise RuntimeError(f'Terraform plan failed with exit code: {result.exit_code}')
    
@task(task_id='apply')
def apply(version, cwd, options=[], env={}):
    tf_bin = Path('/opt/airflow/terraform') / version / 'terraform'

    result = SubprocessHook().run_command(command=[tf_bin, 'apply', *options], cwd=cwd, env=(env|dict(os.environ)))
    if result.exit_code:
        raise RuntimeError(f'Terraform apply failed with exit code: {result.exit_code}')