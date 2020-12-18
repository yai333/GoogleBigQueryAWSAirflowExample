#!/bin/bash

virtualenv -p python3.7 venv

source venv/bin/activate

pip install -r requirements.txt 

cd venv/lib/python3.7

zip -r site-packages.zip site-packages/

mv site-packages.zip ../../../site-packages.zip

cd ../../../

deactivate

aws s3 cp site-packages.zip s3://airflow-demo-personalise/


