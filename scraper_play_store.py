# -*- coding: utf-8 -*-
"""
Created on Mon Oct  10 17:20:26 2019

@author: abonna
"""

import csv
from datetime import datetime
import os
import psycopg2
import credentials
import play_scraper
from subprocess import call

#### definicoes de variaveis
indir = '/home/ubuntu/scripts/crawler_play_store/'
outdir = '/home/ubuntu/scripts/crawler_play_store/csv/'
csvfile = 'details.csv'
tablename = 'google_play.details_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
query = 'SELECT app_id, empresa FROM google_play_dw.app'
cursor.execute(query)
banks = [item for item in cursor.fetchall()] ### pega todos os bancos e ids cadastrados no bd
cursor.close()
db_conn.close()

### funcao para parser a data no formato do banco de dados
def parse_data(data):
    return str(datetime.strptime(data, "%B %d, %Y").date())

### funcao para parser o numero de instalações
def parse_installs(installs):
    return int(installs.replace(',','').replace('+',''))

### funcao para parsear os detalhes do APP
def parse_csv(bank,appid):
    with open(outdir+csvfile, 'a', newline="\n") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        try:
            dict = play_scraper.details(appid)
            score =  dict['score']
            reviews =  dict['reviews']
            installs =  parse_installs(dict['installs'])
            updated = parse_data(dict['updated'])
            row = [appid,bank,score,reviews,installs,updated]
            writer.writerow(row)
        except:
            print('APP não encontrado!')
            pass

### itera sobre os apps cadastrados
for bank in banks:
    (appid,bank) = bank ### recebe o id do Google Play
    parse_csv(bank,appid)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
### copy
with open(outdir+csvfile, 'r') as ifile:
    SQL_STATEMENT = "COPY %s(app_id,empresa,score,reviews,instalacoes,ultima_atualizacao) FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()
os.remove(outdir+csvfile)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)