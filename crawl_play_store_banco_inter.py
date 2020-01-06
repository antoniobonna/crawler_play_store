# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 10:20:26 2019

@author: abonna
"""

from Naked.toolshed.shell import muterun_js
import csv
#import demjson
import sys
from datetime import datetime,date,timedelta
import os
import psycopg2
import credentials
from subprocess import call

#### definicoes de variaveis
indir = '/home/ubuntu/scripts/crawler_play_store/'
google_scraper_model = 'google_scraper_model_bancointer.js'
google_scraper = 'google_scraper_brokers.js'
outdir = '/home/ubuntu/scripts/crawler_play_store/csv/'
csvfile = 'reviews_brokers.csv'
tablename = 'google_play.reviews_stg'
current_date = '2019-12-20'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
query = """SELECT app_id, empresa FROM google_play_dw.app WHERE empresa in ('GENIAL','BTG PACTUAL','RICO','XP INVESTIMENTOS','ORAMA','EASYNVEST','CLEAR','PI INVESTIMENTOS')"""
cursor.execute(query)
banks = [item for item in cursor.fetchall()] ### pega todos os bancos e ids cadastrados no bd
cursor.close()
db_conn.close()

indexes = ('id','userName','date','score','text','replyDate','replyText')

### funcao para parser a data no formato do banco de dados
def parse_data(data):
    # year  = data[:10]
    # time = data[11:-1]
    # return year + ' ' + time
    return str(datetime.strptime(data.split(' GMT')[0], "%a %b %d %Y %H:%M:%S"))

### funcao para parsear os comentarios e coloca-los em um array
def parse_result(result):
    data = result.decode('utf8') ### coloca result em uma string
    ### coloca quotes nas keys
    data = data.replace("id: ","'id': ").replace("userName:","'userName':").replace("userImage:","'userImage':").replace("date:","'date':").replace("score:","'score':").replace("scoreText:","'scoreText':").replace("url:","'url':").replace("title:","'title':").replace("id:","id").replace("text:","'text':").replace("replyDate:","'replyDate':").replace("replyText:","'replyText':").replace("version:","'version':").replace("thumbsUp:","'thumbsUp':")
    data = data.replace('\n','').replace("null","'null'").replace(']','').replace('[','').encode('latin-1', 'ignore').decode('latin-1') ### limpa string
    data = data.replace('},','}|#|').split('|#|') ### coloca strings em arrays
    return data

def parse_csv(bank,appid):
    print('\nParsing {}...'.format(bank))
    new_date = date_aux = ''
    response = muterun_js(indir+google_scraper) ### executa o node.js
    if response.exitcode == 0:
        result = response.stdout ### pega saida do shell
        result_list = parse_result(result)
        with open(outdir+csvfile, 'a', newline="\n") as ofile:
            writer = csv.writer(ofile, delimiter=';')
            for row in result_list:
                try:
                    #print(row)
                    dict = eval(row) ### coloca row em um dicionario
                    try:
                        date = parse_data(dict['date'])
                    except:
                        pass
                    dict['date'] = date
                    new_date = dict['date'][:10]
                    if new_date >= current_date:
                        pass
                    else:
                        if new_date != date_aux:
                            date_aux = new_date
                            print('Current date: '+date_aux)
                        dict['replyDate'] = str(dict['replyDate']).replace('null','')
                        dict['text'] = dict['text'].encode('latin-1', 'ignore').decode('latin-1').strip()
                        if dict['replyDate']:
                            dict['replyDate'] = parse_data(dict['replyDate'])
                        line = [str(dict[k]).replace('null','') for k in list(dict.keys()) if k in indexes]
                        line.insert(0,bank)
                        line.insert(0,appid)
                        writer.writerow(line)
                except:
                    print(row)
                    pass
    else:
        #sys.stderr.write(response.stderr)
        print('Error')

### itera sobre os apps cadastrados
for bank in banks:
    appid,bank = bank ### recebe o id no Google Play
    with open(indir+google_scraper_model, 'r') as ifile:
        with open(indir+google_scraper, 'w') as ofile: ### carrega id no Node.js
            text = ifile.read()
            ofile.write(text.replace('{}',appid))
    parse_csv(bank,appid)
    os.remove(indir+google_scraper)

### conecta no banco de dados
# db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
# cursor = db_conn.cursor()
# print('Connected to the database')
# ### copy
# with open(outdir+csvfile, 'r') as ifile:
    # SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    # print("Executing Copy in "+tablename)
    # cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    # db_conn.commit()
# cursor.close()
# db_conn.close()
# os.remove(outdir+csvfile)

# ### VACUUM ANALYZE
# call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)
