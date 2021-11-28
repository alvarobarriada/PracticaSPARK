# En este script se generarán las gráficas a través de los KPIs obtenidos

import os
from matplotlib.pyplot import margins
#import pruebas as kpis
import pandas as pd
import streamlit as st
import numpy as np
import requests
import altair as alt
import plotly.express as px
import plotly.graph_objects as go



# Página Web - Cabecera
html_header="""
<head>
    <title> Billetajo - Análisis de datos </title>
    <meta charset="utf-8">
</head>

<h1 style="margin-left: 365px;"> BILLETAJO - ANÁLISIS DE DATOS </h1>
"""
st.set_page_config(page_title="Billetajo - Análisis de datos", page_icon="./media/statistics.png", layout="wide")
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
html_card_param1="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;">Franja Horaria</h3>
  </div>
</div>
"""
html_card_param2="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"></h3>
  </div>
</div>
"""


html_card_header1="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;">Nº Operaciones por Sector</h3>
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
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;">Monto consumido por meses</h3>
  </div>
</div>
"""

html_card_footer2="""
<div class="card">
  <div class="card-body" style="border-radius: 0px 0px 10px 10px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 25px;">
    <p class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> </p>
  </div>
</div>
"""

html_card_header3="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;">Código Postal Cliente</h3>
  </div>
</div>
"""

html_card_header4="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 600px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;">Nº Operaciones por Comercio</h3>
  </div>
</div>
"""

html_card_header5="""
<div class="card">
  <div class="card-body" style="border-radius: 10px 10px 0px 0px; background: #eef9ea; padding-top: 5px; width: 1200px;
   height: 50px;">
    <h3 class="card-title" style="background-color:#eef9ea; color:#008080; font-family:Montserrat; text-align: center; padding: 0px 0;"> Temperatura promedio por meses con más importe gastado </h3>
  </div>
</div>
"""

html_br=""" <br> """
html_blankspace=""" <br> <br><br><br><br><br><br><br><br> """

##Block
with st.container():
    col1, col2, col3, col4, col5, = st.columns([1,15,3,15,1])
    with col1:
        st.write("")
    
    with col2:
        st.markdown(html_card_param1, unsafe_allow_html=True)
        horas = st.select_slider('Resultados general: Marcar 00', 
        options=['00','00-02', '02-04', '04-06', '06-08', '08-10', '10-12', '12-14', '14-16', '16-18', '18-20', '20-22', '22-24'])
        st.write('La franja horaria seleccionada es', horas)

        #st.markdown(html_card_param1, unsafe_allow_html=True)
        #cp = st.text_input('','04007')
        #st.write('Código Postal introducido', cp)
    
    with col3:
        st.write("")

    with col4:
        st.markdown(html_card_param2, unsafe_allow_html=True)
    
    with col5:
        st.write("")
    

### Block 1#########################################################################################
with st.container():
    col1, col2, col3, col4, col5, = st.columns([1,15,3,15,1])
    with col1:
        st.write("")
        
    with col2:
        st.markdown(html_card_header1, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        if horas == '00':
            response = requests.get("http://127.0.0.1:5000/kpi2")
        else:
            response = requests.get("http://127.0.0.1:5000/kpi7?horas=%s" % horas)
        
        print(response.json())
        kpi7 = pd.read_json(response.json())

        bars = alt.Chart(kpi7).mark_bar().encode(
            x = "NUMERO_OPERACIONES:Q",
            y = 'SECTOR:O'
        ).properties(
            width = 600,
            height = 350
        )
        st.write(bars)
        st.markdown(html_br, unsafe_allow_html=True)
    with col3:
        st.write("")

    with col4:
        st.markdown(html_card_header2, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        if horas == '00':
            response = requests.get("http://127.0.0.1:5000/kpi6")
        else:
            response = requests.get("http://127.0.0.1:5000/kpi8?horas=%s" % horas)

        print(response.json())
        kpi8 = pd.read_json(response.json())

        #fig = go.Figure(data=[go.Pie(labels=labels, values=values)])

        fig = px.pie(kpi8, values='TOTAL', names='MESES', color='MESES', color_discrete_sequence=px.colors.sequential.RdBu)
        fig.update_layout(height=350, width=600)
        st.plotly_chart(fig)
        st.markdown(html_br, unsafe_allow_html=True)
    with col5:
        st.write("")


st.markdown(html_br, unsafe_allow_html=True)
st.markdown(html_br, unsafe_allow_html=True)
st.markdown(html_br, unsafe_allow_html=True)


with st.container():
    col1, col2, col3, col4, col5= st.columns([1,15,1,15,1])
    with col1:
        st.write("")
    with col2:
        st.markdown(html_card_header3, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        cp = st.text_input('Resultado general: Introducir 00000','00000')
        st.write('Código Postal introducido', cp)
        


    with col3:
        st.write("")

    with col4:
        st.markdown(html_card_header4, unsafe_allow_html=True)
        st.markdown(html_br, unsafe_allow_html=True)
        if cp == '00000':
            response = requests.get("http://127.0.0.1:5000/kpi1")
        else:
            response = requests.get("http://127.0.0.1:5000/kpi10?cp=%s" % cp)
        print(response.json())
        kpi1 = pd.read_json(response.json())

        bars = alt.Chart(kpi1).mark_bar().encode(
            x = "NUMERO_OPERACIONES:Q",
            y = 'CP_COMERCIO:O'
        ).properties(
            width = 600,
            height = 350
        )
        st.write(bars)
    with col5:
        st.write("")


##PRUEBAS
with st.container():
    col1, col2, col3, col4, col5= st.columns([1,15,1,15,1])
    with col1:
        st.write("")
    with col2:
        st.markdown(html_card_header5, unsafe_allow_html=True)
        response = requests.get("http://127.0.0.1:5000/kpi11")
        print(response.json())
        kpi11 = pd.read_json(response.json())

        fig = px.pie(kpi11, values='TOTAL', names='MESES', color='TEMPERATURA', color_discrete_sequence=px.colors.sequential.RdBu)
        fig.update_layout(height=350, width=600)
        st.plotly_chart(fig)

        barras = alt.Chart(kpi11).mark_bar().encode(
            x = "TEMPERATURA:Q",
            y = 'NUMERO_OPERACIONES:Q'
        ).properties(
            width = 1200,
            height = 400
        )
        st.markdown(html_br, unsafe_allow_html=True)
        st.write(barras)

        st.markdown(html_br, unsafe_allow_html=True)
    with col3:
        st.write("")

    with col4:
        st.write("")
    with col5:
        st.write("")