#!/bin/bash

pip3 install --upgrade pip
pip3 install -r requirements.txt
python3 pipeline.py
python3 test_project.py