import pickle
from pathlib import Path

import streamlit_authenticator as stauth

passwords = ["a"]

hashed_passwords = stauth.Hasher(passwords).generate()

print(hashed_passwords)