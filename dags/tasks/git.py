from git import Repo

from airflow.decorators import task

@task(task_id='clone')
def clone(url, to_path, branch):
    Repo.clone_from(url=url, to_path=to_path, branch=branch)
    return to_path