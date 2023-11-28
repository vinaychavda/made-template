import re
import sqlalchemy
from project.misc.constant import EXERCISE_2_URL
from project.misc.function import extract_csv_from_url, drop_invalid_col

if __name__ == '__main__':
    # Extract dataframe from csv (with retries)
    df = extract_csv_from_url(EXERCISE_2_URL)

    # Drop the Status column
    df = df.drop(columns=['Status'])

    # Drop rows with invalid values
    df = df.dropna()

    # Valid "Verkehr" values are "FV", "RV", "nur DPN"
    df = drop_invalid_col(df, 'Verkehr', lambda x: x in ['FV', 'RV', 'nur DPN'])

    # Valid "Laenge", "Breite" values are geographic coordinate system values between and including -90 and 90l̥
    df = drop_invalid_col(df, 'Laenge', lambda x: -90 < x < 90)
    df = drop_invalid_col(df, 'Breite', lambda x: -90 < x < 90)

    # Valid "IFOPT" values follow this pattern: <exactly two characters>:<any amount of numbers>:<any amount of
    # numbers><optionally another colon followed by any amount of numbers>
    # This is not the official IFOPT standard, please follow our guidelines and not the official standard
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
