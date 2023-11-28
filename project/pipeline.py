from kaggle.api.kaggle_api_extended import KaggleApi
from misc.function import extract_and_move_co2_data, load_and_fill_missing, save_to_csv_and_sql
from misc.constant import DATA_SET_1, DATA_SET_1_FILE_NAME, DATA_SET_2, DATA_SET_2_FILE_NAME

if __name__ == "__main__":
    api = KaggleApi()
    api.authenticate()

    # Download and extract CO2 emission dataset
    api.dataset_download_file(DATA_SET_1, DATA_SET_1_FILE_NAME)
    extract_and_move_co2_data('GCB2022v27_MtCO2_flat.csv.zip', 'co2_emission.zip')
    co2_data = load_and_fill_missing('co2_emission/' + DATA_SET_1_FILE_NAME)

    # Download and preprocess world population dataset
    api.dataset_download_file(DATA_SET_2, DATA_SET_2_FILE_NAME)
    population_data = load_and_fill_missing(DATA_SET_2_FILE_NAME, nan_fill_value=0)

    # Save datasets to CSV and SQLite
    save_to_csv_and_sql(co2_data, 'data/' + DATA_SET_1_FILE_NAME, 'data/co2_emission_test1.db', 'co2_emission_data')
    save_to_csv_and_sql(population_data, 'data/' + DATA_SET_2_FILE_NAME, 'data/population_data.db', 'population_data')
