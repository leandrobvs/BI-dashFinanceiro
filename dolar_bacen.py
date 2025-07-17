# %%
import requests
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt


# %%
# Configurações fixas
codigo_serie = 1 
data_inicial = "01/01/2023"
data_final = datetime.today().strftime('%d/%m/%Y')

# %%
url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados'
params = {
    'formato': 'json',
    'dataInicial': data_inicial,
    'dataFinal': data_final
}

response = requests.get(url, params=params)

# %%
dados = response.json()
df_dolar = pd.DataFrame(dados)

# %%
df_dolar['data'] = pd.to_datetime(df_dolar['data'], format='%d/%m/%Y')
df_dolar['valor'] = df_dolar['valor'].astype('float')

# %%
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy import text

# %%
load_dotenv()

sql_server = os.getenv("sql_server").replace('\\\\', '\\')
sql_database = os.getenv('sql_database')
sql_username = os.getenv('sql_username')
sql_password = os.getenv('sql_password')
sql_driver = os.getenv('sql_driver')



# %%


conn_str = f'mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver}&TrustServerCertificate=yes'

engine = create_engine(conn_str)


# %%
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dolar_bacen"))
    df_dolar.to_sql('dolar_bacen', con=conn, if_exists='append', index=False)



