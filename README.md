![CircleCI](https://img.shields.io/circleci/build/gh/aptakhin/groza-python)

Prototype for browser-server transport for PostgresQL

Python 3.6+ and PostgreSQL 9+ are required.

Unix-like installation:

    python3 -m venv venv
    venv/bin/pip install --upgrade pip
    venv/bin/pip install -r requirements.txt

Windows installation:
    
    python3 -m venv venv
    venv/Scripts/pip install --upgrade pip
    venv/Scripts/pip install -r ./requirements.txt

PostgreSQL database `hstore` extension required:

    CREATE EXTENSION hstore;
   
    
    venv/bin/pip install pytest-pep8 
    PYTHONPATH=. venv/bin/py.test --pep8
