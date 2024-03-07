def repo_name(repo_url):
    return repo_url.split('.git')[0].split('/')[-1]