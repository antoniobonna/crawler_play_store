# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 10:37:26 2019

@author: abonna
"""

import os
import re
import psycopg2
from collections import Counter
import credentials
import nltk
import csv
from gensim.models import Phrases
from gensim.models import Word2Vec
from nltk.corpus import stopwords
import psycopg2
import credentials
from subprocess import call
import enchant

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

stopwords = stopwords.words('portuguese')
p = enchant.Dict("pt_BR")
e = enchant.Dict()
nonstopwords = ['intermedium','isafe','sms','xiaomi','nfc','sicoob','wifi','cdb','redmi','ios','mei','crashar','nuconta','samsung','iphone','aff','ok','credicard','criptomoedas','cdbs','bugado','itoken','broker','fintech','fintechs','superapp','instagram','facebook','whatsapp']

def words(text):
    pattern = re.compile(r"[^\s]+")
    non_alpha = re.compile(r"[\W\d_]", re.IGNORECASE)
    for match in pattern.finditer(text):
        nxt = non_alpha.sub("", match.group()).lower()
        return nxt

### variaveis
outdir = '/home/ubuntu/scripts/crawler_play_store/csv/'
file = 'bigram.csv'
query_app = "SELECT app_id FROM google_play_dw.app"
query_company = 'SELECT empresa FROM google_play_dw.app'
query_data = "SELECT DISTINCT semana,ano FROM google_play_dw.vw_reviews WHERE app_id = '{}' AND (ano,semana) !=  (date_part('year',current_date),date_part('week',current_date)) ORDER BY 2,1"
query_comentario = "SELECT comentario FROM google_play_dw.vw_reviews WHERE app_id = '{}' AND ano = {} AND semana = {}"
query_delete = "DELETE FROM reclame_aqui_dw.bigrams_reclamacoes_avaliadas WHERE empresa_id = '{}' AND ano = {} AND semana = {}"
tablename = 'google_play_dw.bigrams'

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')

cursor.execute(query_company)
companies = [item[0].lower() for item in cursor.fetchall()]
nonstopwords = nonstopwords + companies

with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
    writer = csv.writer(ofile, delimiter=';')

    cursor.execute(query_app)
    apps = [item[0] for item in cursor.fetchall()]
    for app in apps:
        try:
            print('Parsing '+app+'...')
            cursor.execute(query_data.format(app))
            datas = [item for item in cursor.fetchall()]
            for semana,ano in [datas[-1]]:
                print('Ano: {} - Semana: {}'.format(ano,semana))
                cursor.execute(query_delete.format(app,ano,semana))
                db_conn.commit()
                cursor.execute(query_comentario.format(app,ano,semana))
                comments = [item[0] for item in cursor.fetchall()]

                bigram = Phrases(min_count=1, threshold=2)
                sentences = []
                for row in comments:
                    sentence = [words(word) for word in nltk.word_tokenize(row,language='portuguese')]
                    sentence = [x.replace('oq','que').replace('vcs','vocês').replace('vc','você').replace('funcao','função').replace('notificacoes','notificações').replace('hj','hoje').replace('pq','porque').replace('msm','mesmo').replace('td','tudo').replace('vzs','vezes').replace('vlw','valeu').replace('msg','mensagem').replace('mt','muito') for x in sentence if x]
                    sentence = [x for x in sentence if x not in stopwords]
                    sentence = [p.suggest(word)[0].lower() if word not in nonstopwords and (not p.check(word) and not e.check(word)) and p.suggest(word) else word for word in sentence]
                    sentences.append(sentence)
                    bigram.add_vocab([sentence])

                try:
                    bigram_model = Word2Vec(bigram[sentences])
                    bigram_model_counter = Counter()

                    for key in bigram_model.wv.vocab.keys():
                        if len(key.split("_")) > 1:
                            bigram_model_counter[key] += bigram_model.wv.vocab[key].count

                    most_commons =  bigram_model_counter.most_common()
                    for phrase,counts in most_commons:
                        writer.writerow([app,ano,semana,phrase.rstrip('_'),counts])
                except:
                    print('Error')
                    pass
        except:
            pass 
### copy
with open(outdir+file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()
os.remove(outdir+file)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)