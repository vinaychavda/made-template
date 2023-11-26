#!/bin/bash

# Export kaggle.json to os env for Kaggle authentication
KAGGLE_JSON_PATH=".~/kaggle/kaggle.json"
KAGGLE_CONFIG_DIR=$(dirname "$KAGGLE_JSON_PATH")
export KAGGLE_CONFIG_DIR

export KAGGLE_USERNAME="${{ secrets.KAGGLE_USERNAME }}"
export KAGGLE_KEY="${{ secrets.KAGGLE_KEY }}"

pip install --upgrade pip
pip install -r ./project/requirements.txt
python project/pipeline.py