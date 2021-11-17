# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np

# Creación de un gráfico de prueba con Streamlit
chart_data = pd.DataFrame(
    np.random.randn(20,3),
    columns = ["a", "b", "c"]
)

st.line_chart(chart_data)

'''
# Gráfico con nuestro primer KPI. (De momento no funciona)

grafico1 = pd.DataFrame(kpis.kpi1, columns=["sector", "sum(num_op)"])

os.system("pip install streamlit")
import streamlit as st
st.hello

st.bar_chart(grafico1)
'''


