# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
from matplotlib.pyplot import margins
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np
import requests
import altair as alt


# Página Web - Cabecera
html_header="""
<head>
    <title> Billetajo - Análisis de datos </title>
    <meta charset="utf-8">
</head>

<h1 style="margin-left: 400px;"> TÍTULO PROVISIONAL </h1>
"""
st.set_page_config(page_title="Billetajo", page_icon="", layout="wide")
st.markdown(html_header, unsafe_allow_html=True)
# Estilos generales
st.markdown(""" <style>
/* Color del fondo */
.reportview-container {
        /*background-color: #5FA3C7;*/
        color: #C79A5F;
    }

/* Color del texto */
.st-ba {
    color: #C79A5F;
}
</style> """, unsafe_allow_html=True)

# HTML de cada recuadro
html_card_header1="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> Número de operaciones por sector </h3>
  </div>
</div>
"""

html_card_footer1="""
<div class="card">
  <div class="card-body" style="border-radius: 0px 0px 10px 10px; background: #eef9ea; padding-top: 1rem;; width: 600px;
   height: 25px;">
    <p class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> </p>
  </div>
</div>
"""

html_card_header2="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> KPI 2 </h3>
  </div>
</div>
"""

html_card_footer2="""
<div class="card">
  <div class="card-body" style="border-radius: 0px 0px 10px 10px; background: #eef9ea; padding-top: 1rem;; width: 600px;
   height: 25px;">
    <p class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> </p>
  </div>
</div>
"""

html_br=""" <br> """
html_blankspace=""" <br> <br><br><br><br><br><br><br><br> """

### Block 1#########################################################################################
with st.container():
    col1, col2, col3, col4, col5, = st.columns([1,15,1,15,1])
    with col1:
        st.write("")
    with col2:
        st.markdown(html_card_header1, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        horas = st.select_slider('Seleccione una franja horaria', options=['00-02', '02-04', '04-06', '06-08', '08-10', '10-12', '12-14', '14-16', '16-18', '18-20', '20-22', '22-24'])
        st.write('La franja horaria seleccionada es', horas)

        '''
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
        '''
        st.markdown(html_card_footer1, unsafe_allow_html=True)

    with col3:
        st.write("")

    with col4:
        st.markdown(html_card_header2, unsafe_allow_html=True)
        '''
        En pruebas para coordinarlo con horas
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
        '''
        st.markdown(html_card_footer2, unsafe_allow_html=True)
    with col5:
        st.write("")


st.markdown(html_br, unsafe_allow_html=True)
st.markdown(html_br, unsafe_allow_html=True)
st.markdown(html_br, unsafe_allow_html=True)


with st.container():
    col1, col2, col3, col4, col5, = st.columns([1,15,1,15,1])
    with col1:
        st.write("")
    with col2:
        st.markdown(html_card_header1, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        st.write('Hueco para otro KPI')

        st.markdown(html_card_footer1, unsafe_allow_html=True)

    with col3:
        st.write("")

    with col4:
        st.markdown(html_card_header2, unsafe_allow_html=True)
        st.markdown(html_blankspace, unsafe_allow_html=True)
        st.markdown(html_card_footer2, unsafe_allow_html=True)
    with col5:
        st.write("")



