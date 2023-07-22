import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from dotenv import load_dotenv

import os
import re


def ari_to_features(ari):

    load_dotenv(r'./.env')

    cid = os.getenv("USER_ID") 
    secret = os.getenv("TOKEN") 

    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    #Audio features
    features = sp.audio_features(ari)[0]
    
    #Artist of the track, for genres and popularity
    artist = sp.track(ari)["artists"][0]["id"]
    artist_pop = sp.artist(artist)["popularity"]
    artist_genres = sp.artist(artist)["genres"]
    
    #Track popularity
    track_pop = sp.track(ari)["popularity"]
    
    #Add in extra features
    features["artist_pop"] = artist_pop
    if artist_genres:
        features["genres"] = " ".join([re.sub(' ','_',i) for i in artist_genres])
    else:
        features["genres"] = "unknown"
    features["track_pop"] = track_pop
    
    return features

if __name__ == "__main__":
    # Debug
    result = ari_to_features("1o0nAjgZwMDK9TI4TTUSNn")
    print(result)