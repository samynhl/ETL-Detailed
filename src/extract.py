import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time


from dotenv import load_dotenv

import os
import re

load_dotenv(r'./.env')

USER = os.getenv("USER_ID") 
KEY = os.getenv("TOKEN")

# Top 100 songs from the french rap playlist
PLAYLIST_ID = '6UJtsUVYLehiB7UxMSj2sA'

COLUMNS =  ['name','album','artist','release_date','duration','popularity','acousticness','danceability','energy',
                'instrumentalness','liveness','loudness', 'speechiness', 'tempo', 'time_signature']

auth_manager = SpotifyClientCredentials(client_id=USER, client_secret=KEY)
sp = spotipy.Spotify(client_credentials_manager = auth_manager)

def getTrackIDs(user, playlist_id):
    track_ids = []
    playlist = sp.user_playlist(user,playlist_id)
    for item in playlist['tracks']['items']:
        track = item['track']
        track_ids.append(track['id'])
    return track_ids


def getTrackFeatures(track_id):
    track_info = sp.track(track_id)
    feature_info = sp.audio_features(track_id)

    # Track info
    name = track_info['name']
    album = track_info['album']['name']
    artist = track_info['album']['artists'][0]['name']
    release_date = track_info['album']['release_date']
    duration = track_info['duration_ms']
    popularity = track_info['popularity']

    # Track features
    acousticness = feature_info[0]['acousticness']
    danceability = feature_info[0]['danceability']
    energy = feature_info[0]['energy']
    instrumentalness = feature_info[0]['instrumentalness']
    liveness = feature_info[0]['liveness']
    loudness = feature_info[0]['loudness']
    speechiness = feature_info[0]['speechiness']
    tempo = feature_info[0]['tempo']
    time_signature = feature_info[0]['time_signature']

    track_data = [
        name,album,artist,release_date,duration,popularity,
        acousticness,danceability,energy,instrumentalness,liveness,loudness,speechiness,tempo,time_signature,
    ]

    return track_data


def extract():
    track_ids = getTrackIDs('spotify', PLAYLIST_ID)

    track_list = []
    for i, id in enumerate(track_ids):
        time.sleep(.3)
        track_data = getTrackFeatures(id)
        track_list.append(track_data)

    df = pd.DataFrame(track_list, columns=COLUMNS)
    df.to_csv('../data/data.csv')

    return df


if __name__=='main':    
    extract()