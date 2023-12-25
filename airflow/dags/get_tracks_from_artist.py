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

def get_tracks_from_artist():
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    SPOTIPY_CLIENT_ID = '04a46b9b8cb14098987f6d928b8c6d24'
    SPOTIPY_CLIENT_SECRET = '13cfc2f56b3d4d67ac8e8c7f7fb000fb'
    client_credentials_manager = SpotifyClientCredentials(
        SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    @task()
    def get_lastest_artists_id():
        new_albums = sp.search(q='tag:new', type='album', limit=50, market = 'VN')['albums']['items']

        new_albums_id = []
        for i in new_albums:
            new_albums_id.append(i['id'])
        lastest_artists_id = []
        for album in new_albums_id:
            artists = sp.album(album)['artists']
            for artist in artists:
                lastest_artists_id.append(artist['id'])
        return lastest_artists_id
    
    @task()
    def get_old_artists_id():
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("local")
        table = catalog.load_table("silver.spotify_artists")

        old_artists_id = table.scan().to_pandas()['artist_id'].to_list()
        return old_artists_id
    
    @task()
    def check_new_artists(lastest_artists_id, old_artists_id):
        new_artists_id = []
        for l in lastest_artists_id:
            if l not in old_artists_id:
                new_artists_id.append(l)
        return new_artists_id
    
    @task()
    def get_artist_ids():
        a = 0
        return a

    @task()
    def get_all_tracks_from_artists(a):
        return a

    @task()
    def check_tracks_exist(a):
        return a
    
    @task
    def extract_tracks_data(a):
        return a

    @task
    def load_data_to_minio(a):
        return a 

    a = get_artist_ids()
    b = get_all_tracks_from_artists(a)
    c = check_tracks_exist(b)
    d = extract_tracks_data(c)
    e = load_data_to_minio(d)

get_tracks_from_artist()