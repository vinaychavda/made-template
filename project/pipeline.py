from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import sqlite3
import os
import shutil
import zipfile

# Constants
DATA_SET_1 = 'thedevastator/global-fossil-co2-emissions-by-country-2002-2022/data'
DATA_SET_1_CSV_FILE_NAME = 'GCB2022v27_MtCO2_flat.csv'
DATA_SET_1_FOLDER_NAME = 'co2_emission'
DB_NAME = 'data.sqlite'
DATA_SET_1_TABLE_NAME = 'co2_emission_data'


DATA_SET_2 = 'iamsouravbanerjee/world-population-dataset'
DATA_SET_2_CSV_FILE_NAME = 'world_population.csv'
DATA_SET_2_TABLE_NAME = 'population_data'

encoding = 'latin-1'
nan_fill_value = 0

script_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(script_dir, '..', 'data')


def extract_and_move_co2_data(old_name: str, new_name: str):
    shutil.move(old_name, new_name)
    with zipfile.ZipFile(new_name, 'r') as zip_ref:
        zip_ref.extractall('co2_emission')


def reshape_population_data(df):
    df_melted = pd.melt(df, id_vars=['Rank', 'Country Code', 'Country', 'Capital', 'Continent', 'Area (kmÂ²)',
                                     'Density (per kmÂ²)', 'Growth Rate', 'World Population Percentage'],
                        var_name='Year',
                        value_name='Population')
    df_melted['Year'] = df_melted['Year'].str.extract('(\d+)').astype(int)
    return df_melted


def co2_pipeline():
    db_file_path = os.path.join(data_folder, DB_NAME)
    # Download and extract CO2 emission dataset
    api.dataset_download_file(DATA_SET_1, DATA_SET_1_CSV_FILE_NAME)
    extract_and_move_co2_data(DATA_SET_1_CSV_FILE_NAME + '.zip', DATA_SET_1_FOLDER_NAME + '.zip')
    df = pd.read_csv(DATA_SET_1_FOLDER_NAME + '/' + DATA_SET_1_CSV_FILE_NAME, encoding=encoding)
    print("Column Names:", df.columns)
    df = df.rename(columns={
        'ISO 3166-1 alpha-3': 'Country Code',
    })
    df = df.query('Year >= 2015')

    if not df.empty:
        print("\nAfter dropping rows:")
        print(df.head())
        try:
            conn = sqlite3.connect(db_file_path)
            df.to_sql(DATA_SET_1_TABLE_NAME, conn, index=False, if_exists='replace')
        except Exception as e:
            print(f"Error writing to SQLite database: {e}")
        # Exit or handle the error as needed
    else:
        print("DataFrame is empty.")

    # Delete downloaded .zip file
    zip_file_path = os.path.join(script_dir, DATA_SET_1_FOLDER_NAME + '.zip')
    folder_path = os.path.join(script_dir, DATA_SET_1_FOLDER_NAME)

    try:
        os.remove(zip_file_path)
        print(f'Deleted {zip_file_path}')
        shutil.rmtree(folder_path)
        print(f'Deleted {folder_path}')
    except Exception as e:
        print(f"Error deleting files: {e}")


def population_pipeline():
    db_file_path = os.path.join(data_folder, DB_NAME)

    api.dataset_download_file(DATA_SET_2, DATA_SET_2_CSV_FILE_NAME)
    df_without_clean = pd.read_csv(DATA_SET_2_CSV_FILE_NAME, encoding=encoding)
    print("New Column Names:", df_without_clean.columns)
    # Print new column names
    # Change column names
    df_without_clean = df_without_clean.rename(columns={
        'CCA3': 'Country Code',
        'Country/Territory': 'Country',
        '2022 Population': '2022',
        '2020 Population': '2020',
        '2015 Population': '2015',
        '2010 Population': '2010',
        '2005 Population': '2005',
        '2000 Population': '2000',
        '1995 Population': '1995',
        '1990 Population': '1990',
        '1985 Population': '1985',
        '1980 Population': '1980',
        '1975 Population': '1975',
        '1970 Population': '1970',
    })
    df = reshape_population_data(df_without_clean)

    print("Column Names:", df.columns)

    if not df.empty:
        print("\nAfter dropping rows:")
        print(df.head())
        try:
            conn = sqlite3.connect(db_file_path)
            df.to_sql(DATA_SET_2_TABLE_NAME, conn, index=False, if_exists='replace')
        except Exception as e:
            print(f"Error writing to SQLite database: {e}")
        # Exit or handle the error as needed
    else:
        print("DataFrame is empty.")

    # Delete downloaded .csv file
    csv_file_path = os.path.join(script_dir, DATA_SET_2_CSV_FILE_NAME)
    try:
        os.remove(csv_file_path)
        print(f'Deleted {csv_file_path}')
    except Exception as e:
        print(f"Error deleting files: {e}")


if __name__ == "__main__":
    api = KaggleApi()
    api.authenticate()

    # CO2 Pipeline
    co2_pipeline()

    # Population Pipeline
    population_pipeline()

