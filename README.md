[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Project Work 3 Kaggle Export](https://github.com/vinaychavda/made-template/actions/workflows/project-test.yml/badge.svg)](https://github.com/vinaychavda/made-template/actions/workflows/project-test.yml)
[![Python Version](https://img.shields.io/badge/Python-3.11.0-blue.svg)](https://www.python.org/downloads/release/python-3.x.x/)

# Data Engineering and Analysis Pipeline

## About

This repository is dedicated to showcasing the implementation of advanced data engineering methods in Python and Jayvee.
The focus is on a pipeline that includes exercises, a project, and thorough analysis of the Kaggle dataset.

## Repository Structure

- **Project:** The main project file is located in the `project` folder.
- **Exercises:**
    - Exercise 1: [exercise1.jv](./exercises/exercise1.jv)
    - Exercise 2: [exercise2.py](./exercises/exercise2.py)
    - Exercise 3: [exercise3.jv](./exercises/exercise3.jv)
    - Exercise 4: [exercise4.py](./exercises/exercise4.py)
    - Exercise 5: [exercise5.jv](./exercises/exercise5.jv)

- **Data:** The `data` folder contains the `data.sqlite` file, which is used in the pipeline.

- **Pipeline:** The pipeline logic is implemented in `pipeline.py` under the `project` folder.

- **Exploration Notebook:** The analysis of the dataset is presented in the `exploration.ipynb` notebook.

# Kaggle Datasets

The project utilizes the following Kaggle datasets:

1. [World Population Dataset](https://www.kaggle.com/datasets/iamsouravbanerjee/world-population-dataset?select=world_population.csv)
2. [World Emission Dataset](https://www.kaggle.com/datasets/thedevastator/global-fossil-co2-emissions-by-country-2002-2022/data?select=GCB2022v27_MtCO2_flat.csv)

Ensure that you have the necessary credentials and permissions to access Kaggle datasets.


## Project Pipeline

To automate the data pipeline, the following libraries were used:

```python
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import sqlite3
import os
import shutil
import zipfile
import platform
````

# How to Run

Run the pipeline: Execute [pipeline.sh](./project/pipeline.sh) to test the project.

Explore the analysis: Refer to [report.ipynb](./project/report.ipynb) for a detailed description of the analysis and code.

## Requirements

Install the required libraries using:

```bash
pip install -r requirements.txt
```

# Kaggle Dataset

The project uses datasets from Kaggle. Ensure that you have the necessary credentials and permissions to access Kaggle
datasets.

# Testing

Test cases have been implemented in the `.github` folder to check the existence of data.

# Report

For more detailed information, please refer to the [Report Notebook](./project/report.ipynb).
