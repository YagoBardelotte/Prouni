#%%
import pandas as pd
import os
from prouni_lib.db import utils

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DB_PATH = os.path.join(DATA_DIR, 'prouni.db')

# Abrindo conexão com o banco
connection = utils.connect_db_alchemy(DB_PATH)

#%%
'''
É necessário rodar o script tratamento.sql
para modificar algumas das informações da base de forma
a torná-la homogênea e a etapa seguinte vir com as
informações limpas
'''
# %%
query = utils.import_query(os.path.join(os.path.dirname(BASE_DIR),'queries\\aed.sql'))

colunas = ['CODIGO_EMEC_IES_BOLSA','TIPO_BOLSA','MODALIDADE_ENSINO_BOLSA','NOME_CURSO_BOLSA',
           'NOME_TURNO_CURSO_BOLSA','SEXO_BENEFICIARIO_BOLSA','RACA_BENEFICIARIO_BOLSA',
           'BENEFICIARIO_DEFICIENTE_FISICO','REGIAO_BENEFICIARIO_BOLSA','SIGLA_UF_BENEFICIARIO_BOLSA']

dfs = []
for col in colunas:
    query_aed = query.format(info = col)
    utils.execute_many_sql(query_aed, connection)
    dfs.append(pd.read_sql_query(query_aed, connection))

# %%
