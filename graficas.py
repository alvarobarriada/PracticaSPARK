# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np
import requests
import altair as alt


response = requests.get("http://127.0.0.1:5000/kpi1")
print(response.json())
datos = pd.read_json(response.json())

bars = alt.Chart(datos).mark_bar().encode(
    x = "NUMERO_OPERACIONES:Q",
    y = 'SECTOR:O'
).properties(
    width = 550,
    height = 300
)
st.write(bars)



