import subprocess
import sys


def sqlmesh_cli():
    subprocess.run(["sqlmesh", "-p", "sqlmesh", *sys.argv[1:]], check=True)
