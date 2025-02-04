import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):

    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id="819b78e0b6534f3c872ac5a78991aafe", client_secret="a2666e7670a84c4288fa4b4680499838")
    
    sp = spotipy.Spotify(client_credentials_manager= client_credentials_manager)
    playlist_link = "https://open.spotify.com/playlist/5MdJ5vdy8CZZdrm9yssGat"
    playlist_URI = playlist_link.split("/")[-1]

    spotify_data = sp.playlist_tracks(playlist_URI)

    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    client = boto3.client('s3')
    client.put_object(
        Body = json.dumps(spotify_data), 
        Bucket = 'spotify-etl-project-gourav',
        Key = 'raw_data/to_processed/' + filename
        )