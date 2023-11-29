import re

import sqlalchemy

from exercises.misc.functions import extract_csv_from_url, drop_invalid_col

if __name__ == '__main__':
    # URL to CSV file
    DATA_URL = 'https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV'
    df = extract_csv_from_url(DATA_URL)

    df = df.drop(columns=['Status'])

    # Drop rows with invalid values
    df = df.dropna()
    df = drop_invalid_col(df, 'Verkehr', lambda x: x in ['FV', 'RV', 'nur DPN'])
    df = drop_invalid_col(df, 'Laenge', lambda x: -90 < x < 90)
    df = drop_invalid_col(df, 'Breite', lambda x: -90 < x < 90)
    df = drop_invalid_col(df, 'IFOPT',
                          lambda x: re.match(
                              '^..:[0-9]+:[0-9]+(:[0-9]+)?$', x) is not None)

    # Load dataframe into SQLite database, with matching data types
    df.to_sql('trainstops',
              'sqlite:///trainstops.sqlite',
              if_exists='replace',
              index=False,
              dtype={
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
