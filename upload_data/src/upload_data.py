# IMPORTAÇÕES
import os
import pandas as pd
import requests
import bs4
import sqlalchemy
from urllib.request import urlopen
from prouni_lib.db import utils

# Endereços do nosso projeto e subpastas
BASE_DIR = os.path.dirname(os.path.abspath('data'))
DATA_DIR = os.path.join( BASE_DIR, 'data' )

# Abrindo conexão com o banco
connection = utils.connect_db( 'mariadb', os.path.join(BASE_DIR, '.env') )

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


# Lendo e guardando as tabelas por ano
_2005 = pd.read_csv(links[1], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2006 = pd.read_csv(links[2], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2007 = pd.read_csv(links[3], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2008 = pd.read_csv(links[4], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2009 = pd.read_csv(links[5], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2010 = pd.read_csv(links[6], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2011 = pd.read_csv(links[7], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2012 = pd.read_csv(links[8], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2013 = pd.read_csv(links[9], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2014 = pd.read_csv(links[10], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2015 = pd.read_csv(links[11], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2016 = pd.read_csv(links[12], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2017 = pd.read_csv(links[13], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')
_2018 = pd.read_csv(links[14], error_bad_lines = False, encoding = 'ISO-8859-1', sep = ';')

resultados = [_2005,_2006,_2007,_2008,_2009,_2010,_2011,_2012,_2013,_2014,_2015,_2016,_2017,_2018]

# Para cada arquivo é realizada uma inserção no banco
for i in resultados:

    table_name = 'tb_resultados' + i
    print(table_name)
    df_tmp.to_sql( table_name, 
                   connection,
                   schema = 'prouni',
                   if_exists = 'replace',
                   index = False )