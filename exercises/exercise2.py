import re
import time
from typing import Callable, Any
import pandas as pd
import sqlalchemy


def extract_csv_from_url(url: str, max_attempts: int = 5, wait_time_before_retry: float = 5) -> pd.DataFrame:
    dataframe = None

    for attempt in range(1, max_attempts + 1):
        try:
            dataframe = pd.read_csv(url, sep=';', decimal=',')
            break
        except pd.errors.EmptyDataError:
            print(f'The CSV at the provided URL is empty! (Attempt {attempt}/{max_attempts})')
            break
        except pd.errors.ParserError:
            print(f'Failed to parse the CSV from the provided URL! (Attempt {attempt}/{max_attempts})')
        except pd.errors.HTTPError as e:
            print(f'HTTPError: {e}. (Attempt {attempt}/{max_attempts})')
        except pd.errors.RequestException as e:
            print(f'RequestException: {e}. (Attempt {attempt}/{max_attempts})')
        except Exception as e:
            print(f'An unexpected error occurred: {e}. (Attempt {attempt}/{max_attempts})')

        if attempt < max_attempts:
            time.sleep(wait_time_before_retry)

    if dataframe is None:
        raise Exception(f'Failed to extract CSV from the provided URL: {url}')

    return dataframe


def drop_invalid_col(df: pd.DataFrame, column: str, valid: Callable[[Any], bool]) -> pd.DataFrame:
    try:
        # Keep only rows where the specified column satisfies the validation function
        df = df.loc[df[column].apply(valid)]
    except KeyError:
        raise KeyError(f'The specified column "{column}" does not exist in the DataFrame.')
    except Exception as e:
        raise Exception(f'An unexpected error occurred while dropping invalid rows: {e}')

    return df


if __name__ == '__main__':
    # Url to csv file
    DATA_URL = 'https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV'

    # Extract dataframe from csv (with retries)
    df = extract_csv_from_url(DATA_URL)

    # Drop the Status column
    df = df.drop(columns=['Status'])

    # Drop rows with invalid values
    df = df.dropna()
    df = drop_invalid_col(df, 'Verkehr', lambda x: x in ['FV', 'RV', 'nur DPN'])
    df = drop_invalid_col(df, 'Laenge', lambda x: -90 < x < 90)
    df = drop_invalid_col(df, 'Breite', lambda x: -90 < x < 90)
    df = drop_invalid_col(df, 'IFOPT', lambda x: re.match('^..:[0-9]+:[0-9]+(:[0-9]+)?$', x) is not None)

    # Load dataframe into sqlite database, with matching datatypes
    df.to_sql('trainstops', 'sqlite:///trainstops.sqlite', if_exists='replace', index=False, dtype={
        "EVA_NR": sqlalchemy.BIGINT,
        "DS100": sqlalchemy.TEXT,
        "IFOPT": sqlalchemy.TEXT,
        "NAME": sqlalchemy.TEXT,
        "Verkehr": sqlalchemy.TEXT,
        "Laenge": sqlalchemy.FLOAT,
        "Breite": sqlalchemy.FLOAT,
        "Betreiber_Name": sqlalchemy.TEXT,
        "Betreiber_Nr": sqlalchemy.BIGINT
    })
