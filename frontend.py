import streamlit as st
import pandas as pd
import sqlCreation

st.title('TESTING')
df = sqlCreation.df_final
UBM = st.selectbox(label='listUBM',options=df)
st.dataframe(df.loc[df['UBM']==UBM])
st.dataframe(df)