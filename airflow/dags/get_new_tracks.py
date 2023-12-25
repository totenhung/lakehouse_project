from datetime import datetime
from airflow.decorators import dag, task
from minio.commonconfig import Tags

defaul_args = {
    'owner': 'airflow',
}

@dag(
    schedule=None,
    start_date=datetime(2023, 9, 2, 10),
    catchup=False,
    default_args=defaul_args
)

def get_new_tracks():
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    SPOTIPY_CLIENT_ID = '04a46b9b8cb14098987f6d928b8c6d24'
    SPOTIPY_CLIENT_SECRET = '13cfc2f56b3d4d67ac8e8c7f7fb000fb'
    client_credentials_manager = SpotifyClientCredentials(
        SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    @task()
    def get_lastest_tracks_id():
        new_albums = sp.search(q='tag:new', type='album', limit=50, market = 'VN')['albums']['items']

        new_albums_id = []
        for i in new_albums:
            new_albums_id.append(i['id'])
        lastest_tracks_id = []
        for album in new_albums_id:
            new_tracks = sp.album_tracks(album)['items']
            for track in new_tracks:
                lastest_tracks_id.append(track['id'])
        return lastest_tracks_id
    
    @task()
    def get_old_tracks_id():
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("local")
        table = catalog.load_table("silver.spotify_tracks")

        old_tracks_id = table.scan().to_pandas()['track_id'].to_list()
        return old_tracks_id

    @task()
    def check_new_tracks(lastest_tracks_id, old_tracks_id):
        new_tracks_id = []
        for l in lastest_tracks_id:
            if l not in old_tracks_id:
                new_tracks_id.append(l)
        return new_tracks_id

    @task()
    def extract_tracks_data(new_tracks_id):
        
        string_csv = 'track_id,track_name,release_date,release_date_precision,album_id,artist_ids,disc_number,explicit,external_urls,popularity,acousticness,danceability,duration_ms,energy,instrumentalness,key,liveness,loudness,mode,speechiness,tempo,valence\n'
        for track_id in new_tracks_id:
            try:
                track_info = sp.track(track_id)
                track_name = track_info['name']
                release_date = track_info['album']['release_date']
                release_date_precision = track_info['album']['release_date_precision']
                album_id = track_info['album']['id']
                artists = track_info['artists']
                artist_ids = [artist['id'] for artist in artists]
                disc_number = track_info['disc_number']
                explicit = track_info['explicit']
                external_urls = track_info['external_urls']['spotify']
                popularity = track_info['popularity']

                af = sp.audio_features(track_id)[0]
                acousticness = af['acousticness']
                danceability = af['danceability']
                duration_ms = af['duration_ms']
                energy = af['energy']
                instrumentalness = af['instrumentalness']
                key = af['key']
                liveness = af['liveness']
                loudness = af['loudness']
                mode = af['mode']
                speechiness = af['speechiness']
                tempo = af['tempo']
                valence = af['valence']
                
                info_tracks = f'{track_id},"{track_name}",{release_date},{release_date_precision},{album_id},"{artist_ids}",{disc_number},{explicit},'\
                    f'{external_urls},{popularity},{acousticness},{danceability},{duration_ms},{energy},{instrumentalness},'\
                f'{key},{liveness},{loudness},{mode},{speechiness},{tempo},{valence}' + '\n'

                string_csv = string_csv + info_tracks

            except Exception as e:
                print(f"An error occurred for track ID {track_id}: {e}")
        return string_csv

    @task()
    def load_data_to_minio(string_csv: str):
        from minio.error import S3Error
        from minio import Minio
        from io import BytesIO
        from datetime import date

        minio_url = 'minio:9000'
        access_key = "Qm6pkTNzxbz3Km6QHVwc"
        secret_key = "wa3LjHTKv3kQOEg8SQjSivwKinh6PKRVZlnYkUbY"
        minio_client = Minio(endpoint=minio_url,
                        access_key=access_key,
                        secret_key=secret_key,
                        secure=False)
        
        bucket_name = 'warehouse'
        folder = 'bronze/tracks'

        # Create bucket if not exists
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name)
        else:
            pass
        
        today = date.today()
        string_byte = bytes(string_csv, 'utf-8')
        csv_buffer = BytesIO(string_byte)
        new_file_name = f"{folder}/{today}.csv"
        
        try:
            minio_client.put_object(
                bucket_name,
                new_file_name,
                data=csv_buffer,
                length=len(string_csv),
                content_type='application/csv')
        except S3Error as e:
            pass

            # Set tag for object
        tags = Tags.new_object_tags()
        tags["Status"] = "New"
        minio_client.set_object_tags(bucket_name, new_file_name, tags)
    
    old_tracks_id = get_old_tracks_id()
    lastest_tracks_id = get_lastest_tracks_id()
    new_tracks_id= check_new_tracks(lastest_tracks_id, old_tracks_id)
    string_csv = extract_tracks_data(new_tracks_id)


    load_data_to_minio(string_csv)

get_new_tracks()
