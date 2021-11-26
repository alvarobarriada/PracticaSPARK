# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np
import requests
import altair as alt

cp = st.text_input('Código Postal de Cliente', '04007')
st.write('Código Postal introducido', cp)

horas = st.select_slider(
    'Seleccione una franja horaria',
    options=['00-02', '02-04', '04-06', '06-08', '08-10', '10-12', '12-14', '14-16', '16-18', '18-20', '20-22', '22-24'])

st.write('La franja horaria seleccionada es', horas)

response = requests.get("http://127.0.0.1:5000/kpi7?horas=%s" % horas)
print(response.json())
kpi7 = pd.read_json(response.json())

bars = alt.Chart(kpi7).mark_bar().encode(
    x = "NUMERO_OPERACIONES:Q",
    y = 'SECTOR:O'
).properties(
    width = 550,
    height = 300
)
st.write(bars)


response = requests.get("http://127.0.0.1:5000/kpi8?horas=%s" % horas)
print(response.json())
kpi8 = pd.read_json(response.json())

bars = alt.Chart(kpi8).mark_bar().encode(
    x = "TOTAL:Q",
    y = 'MESES:O'
).properties(
    width = 550,
    height = 300
)
st.write(bars)


response = requests.get("http://127.0.0.1:5000/kpi1")
print(response.json())
kpi1 = pd.read_json(response.json())

bars = alt.Chart(kpi1).mark_bar().encode(
    x = "NUMERO_OPERACIONES:Q",
    y = 'CP_COMERCIO:O'
).properties(
    width = 550,
    height = 300
)
st.write(bars)



