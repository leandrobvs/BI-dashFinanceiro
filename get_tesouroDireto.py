# %%
import requests
import json
import pandas as pd
import ast

# %%
url = 'https://www.tesourodireto.com.br/json/br/com/b3/tesourodireto/service/api/treasurybondsinfo.json'


# %%
response = requests.get(url)

if response.status_code == 200:
    print('status: success')
    dados = response.json()
else :
    print(f'Erro na requisição: {response.status_code}')
    dados = None

# %%
lista_titulos = dados['response']['TrsrBdTradgList']

# %%
titulos = [item['TrsrBd'] for item in lista_titulos]

# %%
df = pd.DataFrame(titulos)

# %%
df = df.rename(columns={
    'cd': 'codigo_titulo',
    'nm': 'nome_titulo',
    'featrs': 'caracteristicas_titulo',
    'mtrtyDt': 'data_vencimento',
    'minInvstmtAmt': 'valor_minimo_investimento',
    'untrInvstmtVal': 'preco_unitario_compra',
    'invstmtStbl': 'descricao_estabilidade',
    'semiAnulIntrstInd': 'recebe_juros_semestrais',
    'rcvgIncm': 'perfil_investidor_recomendado',
    'anulInvstmtRate': 'taxa_juros_compra',
    'anulRedRate': 'taxa_recompra_tesouro',
    'minRedQty': 'quantidade_minima_recompra',
    'untrRedVal': 'valor_unitario_recompra',
    'minRedVal': 'valor_minimo_recompra',
    'isinCd': 'codigo_isin',
    'FinIndxs': 'indice_correcao',
    'wdwlDt': 'data_saque_permitido',
    'convDt': 'data_conversao',
    'BusSegmt': 'segmento_negocio',
    'amortQuotQty': 'quantidade_cotas_amortizadas'
})

# %%
df = df.drop(columns=[
    'caracteristicas_titulo',
    'descricao_estabilidade',
    'perfil_investidor_recomendado',
    'quantidade_minima_recompra',
    'data_saque_permitido',
    'data_conversao',
    'segmento_negocio',
    'quantidade_cotas_amortizadas'
])

# %%
df_titulos_disponiveis = df[df['preco_unitario_compra'] > 0].copy().reset_index(drop=True)

# %%
df_titulos_disponiveis['indice_correcao'] = df_titulos_disponiveis['indice_correcao'].apply(
    lambda x: x.get('nm', 'Não Informado') if isinstance(x, dict) else 'Não Informado'
)

# %%
df_titulos_disponiveis

# %%
df_titulos_disponiveis['data_vencimento'] = pd.to_datetime(df_titulos_disponiveis['data_vencimento'], errors='coerce')

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
    conn.execute(text("TRUNCATE TABLE titulos_tesouro"))
    df_titulos_disponiveis.to_sql('titulos_tesouro', con=conn, if_exists='append', index=False)


