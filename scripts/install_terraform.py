import argparse
import subprocess

from pathlib import Path

def install(os, arch, version):
    releases_base_url = 'https://releases.hashicorp.com/terraform'
    zip_file_name = f'terraform_{version}_{os}_{arch}.zip'
    release_url = f'{releases_base_url}/{version}/{zip_file_name}'

    install_base_path = Path('/opt/airflow/terraform')
    install_dir = install_base_path / version

    subprocess.run(['curl', '-O', release_url], check=True)
    subprocess.run(['mkdir', '-p', str(install_dir)], check=True)
    subprocess.run(['unzip', zip_file_name, '-d', str(install_dir)], check=True)

    print(f'Installed Terraform {version}.')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--os', type=str)
    parser.add_argument('--arch', type=str)
    parser.add_argument('--versions', type=str, help='a pipe separated list of terraform versions (must be Major.Minor.Patch)')

    args = parser.parse_args()

    for version in args.versions.split('|'):
        install(args.os, args.arch, version)

if __name__ ==  '__main__':
    main()