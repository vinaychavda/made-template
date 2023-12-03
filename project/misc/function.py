import os
import shutil
import sqlite3
import zipfile
import pandas as pd


def extract_and_move_co2_data(old_name: str, new_name: str):
    shutil.move(old_name, new_name)
    with zipfile.ZipFile(new_name, 'r') as zip_ref:
        zip_ref.extractall('co2_emission')


def load_and_fill_missing(file_path, nan_fill_value=0, encoding='latin-1'):
    data = pd.read_csv(file_path, encoding=encoding)
    data.fillna(nan_fill_value, inplace=True)
    return data


def save_to_csv_and_sql(data, csv_path, sql_path, table_name):
    data.to_csv(csv_path, index=False)
    conn = sqlite3.connect(sql_path)
    data.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.close()


def test_exercise(expected_output_file):
    if os.path.isfile(expected_output_file):
        print(f"\t[SUCCESS] Found output file {expected_output_file}")
    else:
        print(f"\t[ERROR] Can not find expected output file: {expected_output_file}.")
        return
