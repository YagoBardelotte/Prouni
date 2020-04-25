# IMPORTAÇÕES
import os
import pandas as pd
import requests
import bs4
import sqlalchemy
from urllib.request import urlopen
from prouni_lib.db import utils
from tqdm import tqdm

# Endereços do nosso projeto e subpastas
BASE_DIR = os.path.dirname(os.path.abspath('data'))
DATA_DIR = os.path.join( BASE_DIR, 'data' )

# Abrindo conexão com o banco
connection = utils.connect_db( 'mariadb' )

# Fazendo requisição para o site onde se encontram as tabelas
req = requests.get('https://data.amerigeoss.org/dataset/mec-prouni')

if req.status_code == 200:
    print('Requisição bem sucedida!')
    content = req.content

soup = bs4.BeautifulSoup(content, 'html.parser')

# Criando uma lista com os links para download das tabelas
links = []
for link in soup.findAll('a', target = '_blank', class_ = 'resource-url-analytics'):
    links.append(link.get('href'))


# Fazendo o download e guardando as tabelas por ano
resultados = []
print('Iniciando o download dos arquivos...')
for i in tqdm(range(1, len(links))):
    try:
        df_tmp = pd.read_csv(links[i], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
        resultados.append(df_tmp)
    except:
        pass
print("Download concluído!")

# Para cada arquivo é realizada uma inserção no banco
print('Iniciando envio para o banco de dados...git')
for i in tqdm(resultados):

    table_name = f'tb_resultados_{i["ANO_CONCESSAO_BOLSA"][0]}'
    print(table_name)
    i.to_sql( table_name, 
                   connection,
                   schema = 'prouni',
                   if_exists = 'replace',
                   index = False )
print('Processo terminado!\nHora de botar a mão na massa.')