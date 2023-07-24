import extract
import transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

if __name__ == "__main__":

#Importing the songs_df from the Extract.py
    load_df=extract.extract()
    if(transform.Data_Quality(load_df) == False):
        raise ("Failed at Data Validation")
    Transformed_df= transform.transform_df(load_df)
    #The Two Data Frame that need to be Loaded in to the DataBase

#Loading into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    #SQL Query to Create Played Songs
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        name VARCHAR(200),
        album VARCHAR(200),
        artist VARCHAR(200),
        duration VARCHAR(200),
        popularity int(10),
        CONSTRAINT primary_key_constraint PRIMARY KEY (name)
    )
    """

    cursor.execute(sql_query_1)
    print("Opened database successfully")
    
    #We need to only Append New Data to avoid duplicates
    try:
        Transformed_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    #cursor.execute('DROP TABLE my_played_tracks')
    #cursor.execute('DROP TABLE fav_artist')

    conn.close()
    print("Close database successfully")