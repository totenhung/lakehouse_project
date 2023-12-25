import streamlit as st
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from PIL import Image
import unidecode
from my_modules import get_data_from_minio
from my_modules import recommend_tracks
from my_modules import get_data_from_gold
from my_modules import user_favourite_tracks

# Get data to handle
df = get_data_from_gold.get_all_dataframe()

# Streamlit config
st.set_page_config(
    page_title="Music Recommendation System",
    page_icon="🎧"
)
st.subheader("Music Recommendation System")
# Add css
with open('assets\style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

init_track_id = '6VXVYATpQXEIoZ97NnWCmn'

### Streamlit UI ###
col1, col_blank, col2 =  st.columns([19, 1, 9])
# Search tracks feature
search_term = col1.text_input("Bạn muốn nghe gì?", value='')
m1 = df["track_name"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.contains(search_term, case=False)
df_search_track = df[m1].drop_duplicates(subset= ['track_id']).sort_values(by=['popularity'], ascending=False).head(50)

# Current playing
if 'current_track_recommend' not in st.session_state:
    st.session_state.current_track_recommend = ''
if 'current_track_id' not in st.session_state:
    st.session_state.current_track_id = ''
if 'current_track_name' not in st.session_state:
    st.session_state.current_track_name = ''
if 'current_artist_name' not in st.session_state:
    st.session_state.current_artist_name = ''
if 'current_track_image' not in st.session_state:
    st.session_state.current_track_image = None
# if 'current_artist_image' not in st.session_state:
#     st.session_state.current_artist_image = None
if 'current_track_audio' not in st.session_state:
    st.session_state.current_track_audio = None
# if 'username' not in st.session_state:
#     st.session_state.username = ''


MAX_CHARACTER_IN_TRACK_NAME = 55
if df_search_track is not None:
    count = 0
    for n_row, row in df_search_track.iterrows():
        # Get media data from MinIO
        album_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['album_image'])
        # artist_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['artist_image'])
        track_audio = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['track_audio'])
        track_name = row['track_name']
        artist_name = row['artist_name']
        track_id = row['track_id']

        # UI
        col1.divider()
        col11, col12 = col1.columns([16, 55])

        if(album_image is not None):
            col11.image(album_image)
        
        if len(track_name) <= MAX_CHARACTER_IN_TRACK_NAME:
            col12.write(track_name)
        else:
            track_name_cut = track_name[0:MAX_CHARACTER_IN_TRACK_NAME-4] + '...'
            col12.write(track_name_cut)

            track_name = track_name[0:MAX_CHARACTER_IN_TRACK_NAME-10] + '...'

        col12.caption(artist_name)

        # Play button
        click_play_btn = col12.button("Play", key = count)
        count += 1

        # Set current song
        if click_play_btn:
            st.session_state.current_track_id = track_id
            st.session_state.current_track_name = track_name
            st.session_state.current_artist_name = artist_name
            st.session_state.current_track_image = album_image
            # st.session_state.current_artist_image = artist_image
            st.session_state.current_track_audio = track_audio
            # Set track for recommedation after search
            st.session_state.current_track_recommend = track_id
            # Save recently track
            if st.session_state['username']:
                user_favourite_tracks.add_user_favourite_tracks(st.session_state['username'], track_id, track_name, artist_name, row['album_image'], row['track_audio'])
             
else:
    col1.error("Bài hát không tồn tại trong dữ liệu")

# track_id = '6VXVYATpQXEIoZ97NnWCmn'
# Get recommendation tracks

if st.session_state.current_track_recommend is not None:
    tracks_df = recommend_tracks.get_recommendation_tracks(df, st.session_state.current_track_recommend)

# Playlist
MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST = 14
with col2:
    tab1, tab2 = st.tabs(['Danh sách phát', 'Bài hát yêu thích'])
    with tab1:
        if tracks_df is not None:
            count2 = 100000
            for n_row, row in tracks_df.iterrows():
                tab1_col1, tab1_col2, tab1_col3 = st.columns([8, 12, 4])
                album_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['album_image'])
                # artist_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['artist_image'])
                track_audio = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['track_audio'])
                track_id = row['track_id']
                track_name = row['track_name']
                artist_name = row['artist_name']
                
                with tab1_col1:
                    if album_image is not None:
                        st.image(album_image)
                with tab1_col2:
                    if len(track_name) >= MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST:
                        st.write(track_name[0:MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST-2] + '...')
                    else:
                        st.write(track_name)
                        
                    if len(artist_name) >= MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST:
                        st.caption(f'{artist_name[0:MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST-2]}' + '...')
                    else:
                        st.caption(artist_name)
                
                with tab1_col3:
                    play_in_playlist = st.button('p', key = count2)
                    # Set current song
                    if play_in_playlist:
                        st.session_state.current_track_id = track_id
                        st.session_state.current_track_name = track_name
                        st.session_state.current_artist_name = artist_name
                        st.session_state.current_track_image = album_image
                        # st.session_state.current_artist_image = artist_image
                        st.session_state.current_track_audio = track_audio
                count2 += 1

    # Listen recently
    with tab2:
        try:
            fav_track_df = user_favourite_tracks.get_user_favourite_tracks(st.session_state['username'])
            count3 = 200000
            for n_row, row in fav_track_df.iterrows():
                tab1_col1, tab1_col2, tab1_col3 = st.columns([8, 12, 4])
                album_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['album_image'])
                # artist_image = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['artist_image'])
                track_audio = get_data_from_minio.get_object_data(get_data_from_minio.minio_client, row['track_audio'])
                track_id = row['track_id']
                track_name = row['track_name']
                artist_name = row['artist_name']
                
                with tab1_col1:
                    if album_image is not None:
                        st.image(album_image)
                with tab1_col2:
                    if len(track_name) >= MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST:
                        st.write(track_name[0:MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST-2] + '...')
                    else:
                        st.write(track_name)
                        
                    if len(artist_name) >= MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST:
                        st.caption(f'{artist_name[0:MAX_CHARACTER_IN_TRACK_NAME_PLAYLIST-2]}' + '...')
                    else:
                        st.caption(artist_name)

                with tab1_col3:
                    play_in_playlist = st.button('p', key = count3)
                    # Set current song
                    if play_in_playlist:
                        st.session_state.current_track_id = track_id
                        st.session_state.current_track_name = track_name
                        st.session_state.current_artist_name = artist_name
                        st.session_state.current_track_image = album_image
                        # st.session_state.current_artist_image = artist_image
                        st.session_state.current_track_audio = track_audio
                count3 += 1
        except Exception as e:
            print(e)

# Player
sidebar_col1, sidebar_col2, sidebar_col3 = st.sidebar.columns([1,10,1])

if st.session_state.current_track_image is not None:
    sidebar_col2.image(st.session_state.current_track_image)

sidebar_col1.write(" ")
sidebar_col2.caption(st.session_state.current_artist_name)
sidebar_col2.write(st.session_state.current_track_name)
sidebar_col3.write(" ")
 
if st.session_state.current_track_audio is not None:
    st.sidebar.audio(st.session_state.current_track_audio, format='audio/mp3')