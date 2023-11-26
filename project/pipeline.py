import zipfile
import shutil
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

def extract_and_move(old_name: str, new_name: str):
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

if __name__ == "__main__":
    api = KaggleApi()
    api.authenticate()
    
    # Download and extract CO2 emission dataset
    api.dataset_download_file('thedevastator/global-fossil-co2-emissions-by-country-2002-2022/data','GCB2022v27_MtCO2_flat.csv')
    extract_and_move('GCB2022v27_MtCO2_flat.csv.zip', 'co2_emission.zip')
    
    # Download and preprocess world population dataset
    api.dataset_download_file('iamsouravbanerjee/world-population-dataset','world_population.csv')
    co2_data = load_and_fill_missing('co2_emission/GCB2022v27_MtCO2_flat.csv')
    population_data = load_and_fill_missing('world_population.csv', nan_fill_value=0)
    
    # Save datasets to CSV and SQLite
    save_to_csv_and_sql(co2_data, 'data/GCB2022v27_MtCO2_flat.csv', 'data/co2_emission_test1.db', 'co2_emission_data')
    save_to_csv_and_sql(population_data, 'data/world_population.csv', 'data/population_data.db', 'population_data')
