#!/bin/bash

pip install --upgrade pip
pip install -r ./project/requirements.txt
python project/pipeline.py
python project/test_project.py "../main"