#%%
# IMPORTAÇÕES
import os
import pandas as pd
import requests
import bs4
import sqlalchemy
from urllib.request import urlopen
from prouni_lib.db import utils
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

#%%
# Endereços do nosso projeto e subpastas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DB_PATH = os.path.join(DATA_DIR, 'prouni.db')

#%%
# Abrindo conexão com o banco
connection = utils.connect_db_alchemy(DB_PATH)

#%%
# Fazendo requisição para o site onde se encontram as tabelas
req = requests.get('https://data.amerigeoss.org/dataset/mec-prouni')

if req.status_code == 200:
    print('Requisição bem sucedida!')
    content = req.content

soup = bs4.BeautifulSoup(content, 'html.parser')

#%%
# Criando uma lista com os links para download das tabelas
links = []
for link in soup.findAll('a', target = '_blank', class_ = 'resource-url-analytics'):
    links.append(link.get('href'))

#%%
# Fazendo o download e guardando as tabelas por ano
resultados = {}
print('Fazendo o download dos arquivos...')
for i in tqdm(range(1, len(links))):
    if i <= 12 or i == 18:
        try:
            df_tmp = pd.read_csv(links[i], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
            if (df_tmp.columns[0] != "ANO_CONCESSAO_BOLSA"):
                df_tmp.rename(columns={df_tmp.columns[0]:"ANO_CONCESSAO_BOLSA"}, inplace=True)
            resultados[f'base_{i}'] = df_tmp
        except:
            pass
    else:
        try:
            df_tmp = pd.read_csv(links[i], error_bad_lines = False, encoding = 'UTF-8', sep = ';')
            if (df_tmp.columns[0] != "ANO_CONCESSAO_BOLSA"):
                df_tmp.rename(columns={df_tmp.columns[0]:"ANO_CONCESSAO_BOLSA"}, inplace=True)
            resultados[f'base_{i}'] = df_tmp
        except:
            pass
print("Download concluído!")
del([links,df_tmp])

#%%
# Tratamento de algumas tabelas
del(resultados['base_15']) # sem dados
del(resultados['base_17']) # sem dados

resultados['base_16'].isna().sum() # as ultimas linhas estão todas vazias
resultados['base_16'].dropna(inplace=True)
resultados['base_16']['ANO_CONCESSAO_BOLSA'] = resultados['base_16']['ANO_CONCESSAO_BOLSA'].astype('int')

# padronização do nome de algumas colunas
resultados['base_18'].rename(columns={'CPF_BENEFICIARIO':"CPF_BENEFICIARIO_BOLSA",
                                      'SEXO_BENEFICIARIO':'SEXO_BENEFICIARIO_BOLSA',
                                      'RACA_BENEFICIARIO':'RACA_BENEFICIARIO_BOLSA',
                                      'DATA_NASCIMENTO':'DT_NASCIMENTO_BENEFICIARIO',
                                      'REGIAO_BENEFICIARIO':'REGIAO_BENEFICIARIO_BOLSA',
                                      'UF_BENEFICIARIO':'SIGLA_UF_BENEFICIARIO_BOLSA',
                                      'MUNICIPIO_BENEFICIARIO':'MUNICIPIO_BENEFICIARIO_BOLSA'}, inplace=True)

resultados['base_18'].drop(['CAMPUS','MUNICIPIO'],axis = 1,inplace=True)

tb_final = pd.concat(resultados,ignore_index=True)
del(resultados)

#%%
# A tabela completa é inserida no db sqlite
print('Enviando para o banco de dados...')
table_name = f'tb_completa'
print(table_name)
tb_final.to_sql(table_name, 
                connection,
                if_exists = 'replace',
                index = False)
print('Processo terminado!\nHora de botar a mão na massa.')