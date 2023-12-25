from datetime import datetime
from airflow.decorators import dag, task
from minio.commonconfig import Tags
import requests

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

from minio.error import S3Error
from minio import Minio
from io import BytesIO
from datetime import datetime

defaul_args = {
    'owner': 'airflow',
}

@dag(
    schedule=None,
    start_date=datetime(2023, 9, 2, 10),
    catchup=False,
    default_args=defaul_args
)

def get_track_images():
    SPOTIPY_CLIENT_ID = '04a46b9b8cb14098987f6d928b8c6d24'
    SPOTIPY_CLIENT_SECRET = '13cfc2f56b3d4d67ac8e8c7f7fb000fb'
    client_credentials_manager = SpotifyClientCredentials(
        SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    @task()
    def get_track_id():
        catalog = load_catalog("local")
        tbl = catalog.load_table("gold.data_for_recommendation")

        # Just get 100 row for testing
        track_ids = tbl.scan(
            row_filter=GreaterThanOrEqual("popularity", 40),
            selected_fields=(['track_id']),
        ).to_pandas()['track_id'].tolist()
        return track_ids
    
    @task
    def get_track_image_and_audio(track_ids):
        list_result = []
        for track_id in track_ids:
            try:
                info = sp.track(track_id)
                album_id = info['album']['id']
                track_date = info['album']['release_date']  

                image_url = info['album']['images'][0]['url']
                audio_url = info['preview_url']

                # img_data = requests.get(image_url).content
                # audio_data = requests.get(audio_url).content

                tuple_result = (image_url, audio_url, album_id, track_id, track_date)
                list_result.append(tuple_result)
            except Exception as e:
                    print(e)
        return list_result
    
    @task()
    def load_data_to_minio(list_result):
        # Download data from url
        list_data_downloaded = []
        for r in list_result:
            try:
                image_url = r[0]
                audio_url = r[1]
                album_id = r[2]
                track_id = r[3]
                track_date = r[4]

                img_data = requests.get(image_url).content
                audio_data = requests.get(audio_url).content

                # Get date info from track_date
                dt = datetime.strptime(track_date, '%Y-%m-%d')
                track_year = dt.year
                track_month = dt.month

                tuple_result = (img_data, audio_data, album_id, track_id, track_year, track_month)
                list_data_downloaded.append(tuple_result)

            except Exception as e:
                print(e)

        # Load data to MinIO
        minio_url = 'minio:9000'
        access_key = "Qm6pkTNzxbz3Km6QHVwc"
        secret_key = "wa3LjHTKv3kQOEg8SQjSivwKinh6PKRVZlnYkUbY"
        minio_client = Minio(endpoint=minio_url,
                        access_key=access_key,
                        secret_key=secret_key,
                        secure=False)

        bucket_name = 'warehouse'
        # Create bucket if not exists
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name)
        else:
            pass

        album_image_folder = 'images/albums'
        audio_track_folder = 'audios'
        
        for d in list_data_downloaded:
            try:
                img_data = d[0]
                audio_data = d[1]
                album_id = d[2]
                track_id = d[3]
                track_year = d[4]
                track_month = d[5]

                # Save image
                img_buffer = BytesIO(img_data)
                new_file_name = f"{album_image_folder}/{track_year}/{track_month}/{album_id}.jpg"
                try:
                    minio_client.put_object(
                        bucket_name,
                        new_file_name,
                        data=img_buffer,
                        length=len(img_data),
                        content_type='image/jpg')
                except S3Error as e:
                    pass

                # Save audio
                audio_buffer = BytesIO(audio_data)
                new_file_name = f"{audio_track_folder}/{track_year}/{track_month}/{track_id}.mp3"
                try:
                    minio_client.put_object(
                        bucket_name,
                        new_file_name,
                        data=audio_buffer,
                        length=len(audio_data),
                        content_type='audio/mp3')
                except S3Error as e:
                    pass
                
            except Exception as e:
                print(e)
        
    track_ids = get_track_id()
    list_result = get_track_image_and_audio(track_ids)
    load_data_to_minio(list_result)
get_track_images()
