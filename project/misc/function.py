import shutil
import sqlite3
import zipfile
import pandas as pd
import time
from typing import Callable, Any


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

def drop_invalid_col(df: pd.DataFrame, column: str, valid: Callable[[Any], bool]) -> pd.DataFrame:
    try:
        # Keep only rows where the specified column satisfies the validation function
        df = df.loc[df[column].apply(valid)]
    except KeyError:
        raise KeyError(f'The specified column "{column}" does not exist in the DataFrame.')
    except Exception as e:
        raise Exception(f'An unexpected error occurred while dropping invalid rows: {e}')

    return df
