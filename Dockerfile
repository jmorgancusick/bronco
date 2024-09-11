FROM apache/airflow:2.10.1-python3.12

USER root

# Install packages
RUN apt-get update
RUN apt-get install -y curl git unzip jq

USER airflow

# Install pipenv
RUN pip3 install pipenv
ENV PATH="${PATH}:~/.local/bin"

# Install python dependencies
ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN pipenv install --system

# Install terraform versions
ENV TF_VERSIONS="1.6.6|1.7.4|1.9.5"
ADD scripts/install_terraform.py install_terraform.py
RUN python3 install_terraform.py --os linux --arch arm64 --versions ${TF_VERSIONS}