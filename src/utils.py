import json

from dotenv import load_dotenv

import os
import base64

from requests import post

load_dotenv(r'./.env')


USER_ID = os.getenv("USER_ID") 
USER_SECRET = os.getenv("TOKEN") 

def get_token():
    auth_string = USER_ID + ":" + USER_SECRET
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"

    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type" : "client_credentials",
        "scope": "user-read-recently-played"}

    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token