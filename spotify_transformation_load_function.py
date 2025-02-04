import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):

    album_list = []

    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']

        album_element = {
            'album_id': album_id,
            'name' : album_name,
            'release_date' : album_release_date,
            'total_tracks' : album_total_tracks,
            'url' : album_url
        }

        album_list.append(album_element)
    return album_list

def artist(data):

    artist_list = []
    for row in data['items']:
        for key, values in row.items():
            if key == 'track':
                for artists in values['artists']:
                    artists_id = artists['id']
                    artists_name = artists['name']
                    artists_url = artists['href']

                    artists_element = {
                    'artists_id': artists_id,
                    'artists_name': artists_name,
                    'artists_url' : artists_url
                    }
                    artist_list.append(artists_element)
    return artist_list

def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['id']

        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'song_duration': song_duration,
            'song_url': song_url,
            'song_popularity': song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-gourav'
    Key = 'raw_data/to_processed/'

    spotify_processed_bucket = s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']

    spotify_data = []
    spotify_keys = []

    for file in spotify_processed_bucket:
        file_key = file['Key']

        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body'].read()
            jsonObject = json.loads(content)
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        print(album_list)

        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artists_id'])

        song_df = pd.DataFrame.from_dict(song_list)

        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        # creating file name to save in respective folder(album, artist, song)
        song_key = 'transformed_data/songs_data/song_transformed_' + str(datetime.now()) + '.csv'

        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Body=song_content, Bucket=Bucket, Key=song_key)

        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'

        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Body=album_content, Bucket=Bucket, Key=album_key)

        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Body=artist_content, Bucket=Bucket, Key=artist_key)

    for key in spotify_keys:
        # Copy the file to the new location
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3.copy_object(
            Bucket=Bucket,
            CopySource=copy_source,
            Key='raw_data/processed/' + key.split('/')[-1]
        )
        # Delete the original file
        s3.delete_object(
            Bucket=Bucket,
            Key=key
        )