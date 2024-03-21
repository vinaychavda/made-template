#!/bin/bash

pip3 install --upgrade pip
pip3 install -r ./project/requirements.txt
python3 ./project/pipeline.py
python3 ./project/test_project.py