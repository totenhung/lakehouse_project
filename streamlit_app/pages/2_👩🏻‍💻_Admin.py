import streamlit as st
import pandas as pd
from my_modules import get_data_from_gold
from unidecode import unidecode

def search_music():
    df = get_data_from_gold.get_all_dataframe()

    # Nhập từ khóa tìm kiếm
    search_term = st.text_input("Tìm kiếm:", value='')
    search_term = unidecode(search_term)
    
    # Filter the dataframe using masks
    # Convert column of dataframe to string and remove accent 
    m1 = df["track_name"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.contains(search_term, case=False)
    # m2 = df["artist_name"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.contains(search_term, case=False)

    df_search_track = df[m1].drop_duplicates(subset= ['track_name']).sort_values(by=['popularity'], ascending=False).head(99)
    # df_search_artist = df[m2].drop_duplicates(subset='artist_name').sort_values(by=['popularity'], ascending=False)

    N_cards_per_row = 3
    if search_term:
        for n_row, row in df_search_track.reset_index().iterrows():
            i = n_row%N_cards_per_row
            if i==0:
                st.write("---")
                cols = st.columns(N_cards_per_row, gap="large")
            # draw the card
            with cols[n_row%N_cards_per_row]:
                st.markdown(f"{row['track_name'].strip()}")
                st.caption(f"**{row['artist_name'].strip()}**")
    else:
        st.write("Nhập từ khóa để bắt đầu.")

search_music()