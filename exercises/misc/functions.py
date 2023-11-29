import time
import pandas as pd
from typing import Callable, Any


def extract_csv_from_url(url: str,
                         max_attempts: int = 5,
                         wait_time_before_retry: float = 5) -> pd.DataFrame:
    dataframe = None
    for attempt in range(1, max_attempts + 1):
        try:
            dataframe = pd.read_csv(url, sep=';', decimal=',')
            break
        except pd.errors.EmptyDataError:
            print(f'CSV at the provided URL is empty! (Attempt {attempt}/{max_attempts})')
            break
        except (pd.errors.ParserError, pd.errors.HTTPError, pd.errors.RequestException) as e:
            print(f'Error during extraction: {e}. (Attempt {attempt}/{max_attempts})')
        except Exception as e:
            print(f'Unexpected error: {e}. (Attempt {attempt}/{max_attempts})')

        if attempt < max_attempts:
            time.sleep(wait_time_before_retry)

    if dataframe is None:
        raise Exception(f'Failed to extract CSV from the provided URL: {url}')

    return dataframe


def drop_invalid_col(df: pd.DataFrame,
                     column: str,
                     valid: Callable[[Any],
                     bool]) -> pd.DataFrame:
    try:
        df = df.loc[df[column].apply(valid)]
    except KeyError:
        raise KeyError(f'The specified column "{column}" does not exist in the DataFrame.')
    except Exception as e:
        raise Exception(f'Error while dropping invalid rows: {e}')

    return df
