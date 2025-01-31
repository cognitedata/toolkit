import streamlit as st
from cognite.client import CogniteClient

st.title("An example app in CDF")
client = CogniteClient()


@st.cache_data
def get_assets():
    assets = client.assets.list(limit=1000).to_pandas()
    assets = assets.fillna(0)
    return assets


st.write(get_assets())
