import os
import shutil
import urllib.request
import zipfile
from datetime import time

import pandas as pd
from pathlib import Path
from sqlalchemy import BIGINT, FLOAT, TEXT


def download_and_extract_zip(url: str, max_tries: int = 5, sec_wait_before_retry: float = 5) -> str:
    data_name = Path(url).stem
    extract_path = os.path.join(os.curdir, data_name)
    zip_name = data_name + '.zip'

    for i in range(1, max_tries + 1):
        try:
            urllib.request.urlretrieve(url, zip_name)
            break
        except:
            print(f'Couldn\'t load zip from given url! (Try {i}/{max_tries})')
            if i < max_tries: time.sleep(sec_wait_before_retry)

    if not os.path.exists(zip_name):
        raise FileNotFoundError(f'Failed to load zip from url {url}')

    with zipfile.ZipFile(zip_name, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    os.remove(zip_name)
    return extract_path


def validate(df: pd.DataFrame, column: str, constraint: callable) -> pd.DataFrame:
    return df.loc[df[column].apply(constraint)]


def celsius_to_fahrenheit(temp_cels: float) -> float:
    return (temp_cels * 9 / 5) + 32


if __name__ == '__main__':
    zip_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
    data_filename = 'data.csv'

    data_path = download_and_extract_zip(zip_url)

    df = pd.read_csv(os.path.join(data_path, data_filename),
                     sep=';', index_col=False,
                     usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)',
                              'Batterietemperatur in °C', 'Geraet aktiv'], decimal=',')

    df.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 'Batterietemperatur in °C': 'Batterietemperatur'},
              inplace=True)

    df['Temperatur'] = celsius_to_fahrenheit(df['Temperatur'])
    df['Batterietemperatur'] = celsius_to_fahrenheit(df['Batterietemperatur'])

    validation_columns = ['Geraet', 'Monat', 'Temperatur', 'Batterietemperatur', 'Geraet aktiv']
    df = validate(df, validation_columns[0], lambda x: x > 0)
    for column in validation_columns[1:]:
        df = validate(df, column, lambda x: True)
    shutil.rmtree(data_path)

    table = 'temperatures'
    database = 'temperatures.sqlite'
    df.to_sql(table, f'sqlite:///{database}', if_exists='replace', index=False, dtype={
        'Geraet': BIGINT, 'Hersteller': TEXT, 'Model': TEXT, 'Monat': BIGINT,
        'Temperatur': FLOAT, 'Batterietemperatur': FLOAT, 'Geraet aktiv': TEXT
    })

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_name = "../data/data.sqlite"
    file_path = os.path.join(current_directory, file_name)
    if os.path.isfile(file_path):
        print(f"\t[SUCCESS] Found output file {file_path}")
    else:
        print(f"\t[ERROR] Can not find expected output file: {file_path}.")
        raise FileNotFoundError(f'Failed to find output file {file_path}')


    print('Datapipeline finished successfully')
    print(f'Data is stored in table "{table}" in database "{database}"')
