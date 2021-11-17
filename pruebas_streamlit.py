#import pruebas as kpis
import os

## Streamlit
import pandas as pd
import streamlit as st
import numpy as np
'''
grafico1 = pd.DataFrame(kpis.kpi1, columns=["sector", "sum(num_op)"])

os.system("pip install streamlit")
import streamlit as st
st.hello

st.bar_chart(grafico1)'''

chart_data = pd.DataFrame(
    np.random.randn(20,3),
    columns = ["a", "b", "c"]
)

st.line_chart(chart_data)