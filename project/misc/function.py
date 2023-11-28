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


def extract_csv_from_url(url: str, max_tries: int = 5, sec_wait_before_retry: float = 5) -> pd.DataFrame:
    df = None
    for i in range(1, max_tries + 1):
        try:
            df = pd.read_csv(url, sep=';', decimal=',')
            break
        except:
            print(f'Couldn\'t extract csv from given url! (Try {i}/{max_tries})')
            if i < max_tries: time.sleep(sec_wait_before_retry)
    if df is None:
        raise Exception(f'Failed to extract csv from given url {url}')
    return df


def drop_invalid_col(df: pd.DataFrame, column: str, valid: Callable[[Any], bool]) -> pd.DataFrame:
    df = df.loc[df[column].apply(valid)]
    return df
