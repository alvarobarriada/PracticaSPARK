# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np
import requests
import altair as alt

# Creación de un gráfico de prueba con Streamlit
chart_data = pd.DataFrame(
    np.random.randn(20,3),
    columns = ["a", "b", "c"]
)

st.line_chart(chart_data)
response = requests.get("http://127.0.0.1:5000/kpi1")
print(response.json())
datos = pd.read_json(response.json())
st.write(datos)
'''
datos = datos.rename({0: 'SECTOR'}, axis='columns')
datos = datos.rename(columns = {'index':'sum(NUM_OP)'})

p = alt.Chart(datos).mark_bar().encode(
    x="SECTOR",
    y="sum(NUM_OP)"
)
st.write(p)
'''
st.bar_chart(datos['sum(NUM_OP)'])
#st.json(response.json())

'''
# Gráfico con nuestro primer KPI. (De momento no funciona)

grafico1 = pd.DataFrame(kpis.kpi1, columns=["sector", "sum(num_op)"])

os.system("pip install streamlit")
import streamlit as st
st.hello

st.bar_chart(grafico1)
'''


