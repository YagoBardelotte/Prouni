#%%
import pandas as pd
import os
import sqlite3
import sqlalchemy
from tqdm import tqdm
import dotenv

def import_query(path, **kwargs):
    '''
    Essa função realiza o import de uma query, onde pode ser passado vários argumentos de import (read())
    '''
    with open(path, 'r', **kwargs) as query_file:
        query = query_file.read()

    return query

def connect_db_alchemy(db_path):
    '''
    Função para conectar ao banco de dados (sqlite3) usando SqlAlchemy
    '''
    str_connection = 'sqlite:///{path}'
    str_connection = str_connection.format(path = db_path)
    connection = sqlalchemy.create_engine( str_connection )
    return (connection)

def execute_many_sql( sql, conn, verbose=False ):
    if verbose:
        for i in tqdm(sql.split(";")[:-1]):
            conn.execute( i )
    else:
        for i in sql.split(";")[:-1]:
            conn.execute( i )