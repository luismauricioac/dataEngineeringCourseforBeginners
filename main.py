import pandas as pd
import sqlalchemy
import pandas
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "luismalamoc"
TOKEN = "BQDKy4doatbwIbyp3xqcSTifQOyLwYoEBrtxdFWUx8or3NVwFTXj-MWHSn7j8iL5TjV2lsCpTtlQ7_lNlBNf0DzfzEMAXdRXNSLtyeWsmmCE6MmHca-hk8C5Hn47AuFTyd8rl1ejNTPrs0IgoUiawg"
ENDPOINT = "https://api.spotify.com/v1/me/player/recently-played?after={unix_timestamp}"


def check_if_valid_data(df: pd.DataFrame, today, yesterday) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    if df.isnull().values.any():
        raise Exception("Null valued found")

    timestamps = df["timestamp"].tolist()

    for timestamp in timestamps:
        timestamp_f = datetime.datetime.strptime(timestamp[0:19], "%Y-%m-%dT%H:%M:%S")
        if timestamp_f < yesterday or timestamp_f > today:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")

    return True


if __name__ == "__main__":
    # Extract
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    today_unix_timestamp = int(today.timestamp()) * 1000

    yesterday = today - datetime.timedelta(days=1)
    yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    endpoint_to_consume = ENDPOINT.format(unix_timestamp=yesterday_unix_timestamp)
    print("Calling to: " + endpoint_to_consume)
    r = requests.get(endpoint_to_consume, headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"[0:10]])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df, today, yesterday):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")