# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 10:37:26 2019

@author: abonna
"""

import re
import psycopg2
import collections
import credentials

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

def words(text):
    pattern = re.compile(r"[^\s]+")
    non_alpha = re.compile(r"[\W\d_]", re.IGNORECASE)
    try:
        for match in pattern.finditer(text):
            nxt = non_alpha.sub("", match.group()).lower()
            if nxt and nxt not in ('o','a','do','e'):  # skip blank, non-alpha words
                yield nxt
    except:
        pass

def phrases(words):
    phrase = []
    for word in words:
        phrase.append(word)
        if len(phrase) > 3:
            phrase.remove(phrase[0])
        if len(phrase) == 3:
            yield tuple(phrase)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
query = "SELECT comentario from google_play.reviews_stg WHERE empresa = 'INTER'"
cursor.execute(query)
comments = [item[0] for item in cursor.fetchall()]
cursor.close()
db_conn.close()

counts = collections.defaultdict(int)
for comment in comments:
    for phrase in phrases(words(comment)):
        counts[phrase] += 1

top_phrases = [ (' '.join(phrase), count) for phrase, count in counts.items() if count > 50]
print (top_phrases)