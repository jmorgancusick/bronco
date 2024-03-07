from pathlib import Path

from airflow.decorators import task
from airflow.hooks.subprocess import SubprocessHook

@task(task_id='init')
def init(version, cwd):
    tf_bin = Path('/opt/airflow/terraform') / version / 'terraform'
    
    result = SubprocessHook().run_command(command=[tf_bin, 'init'], cwd=cwd)
    if result.exit_code:
        raise RuntimeError(f'Terraform init failed with exit code: {result.exit_code}')

@task(task_id='plan')
def plan(version, cwd):
    tf_bin = Path('/opt/airflow/terraform') / version / 'terraform'

    result = SubprocessHook().run_command(command=[tf_bin, 'plan'], cwd=cwd)
    if result.exit_code:
        raise RuntimeError(f'Terraform plan failed with exit code: {result.exit_code}')