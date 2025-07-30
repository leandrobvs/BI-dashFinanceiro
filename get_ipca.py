# %%
import requests
import pandas as pd
from datetime import datetime

# %%
# Configurações fixas
codigo_serie = 433
data_inicial = "01/01/2020"
data_final = datetime.today().strftime('%d/%m/%Y')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
}

# %%
url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados'
params = {
    'formato': 'json',
    'dataInicial': data_inicial,
    'dataFinal': data_final
}

response = requests.get(url, params=params, headers=headers)

# %%
dados = response.json()
df_ipca = pd.DataFrame(dados)

# %%
df_ipca['valor'] = pd.to_numeric(df_ipca['valor'], errors='coerce')
df_ipca['data'] = pd.to_datetime(df_ipca['data'], format='%d/%m/%Y')

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
    conn.execute(text("TRUNCATE TABLE ipca"))
    df_ipca.to_sql('ipca', con=conn, if_exists='append', index=False)



# %%
