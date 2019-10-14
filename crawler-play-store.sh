#!/bin/bash

### Definicao de variaveis
LOG="/var/log/scripts/scripts.log"
DIR="/home/ubuntu/scripts/crawler_play_store"
DUMP="/home/ubuntu/scripts/dump"
STARTDATE=$(date +'%F %T')
SCRIPTNAME="crawler-play-store.sh"

horario()
{
	date +%d/%m/%Y" - "%H:%M:%S
}
export -f horario

stagingDados()
{
	FILE=$1
	time python ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f stagingDados

LoadDW()
{
	FILE=$1
	time psql -d torkcapital -f ${DIR}/${FILE}
	echo -e "$(horario): Script $FILE executado.\n-\n"
}
export -f LoadDW

LoadHist()
{
	TABLE=$1
	time (psql -d torkcapital -c "COPY (table google_play.${TABLE}_stg) TO '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "COPY google_play.${TABLE}_hist FROM '/home/ubuntu/dump/${TABLE}.txt';"
	psql -d torkcapital -c "VACUUM ANALYZE google_play.${TABLE}_hist;"
	psql -d torkcapital -c "TRUNCATE google_play.${TABLE}_stg;")
	echo -e "$(horario): Tabela $TABLE transferida para dados historicos.\n-\n"
}
export -f LoadHist

### Carrega arquivos nas tabelas staging

echo -e "$(horario): Inicio do staging.\n-\n"

ListaArquivos="scraper_play_store.py crawl_play_store.py"
for FILE in $ListaArquivos; do
	stagingDados $FILE
done

### Carrega dados no DW

echo -e "$(horario): Inicio da carga no DW.\n-\n"

ListaArquivos="etl_visao_geral_dw.sql etl_reviews_dw.sql"
for FILE in $ListaArquivos; do
	LoadDW $FILE
done

### Limpa tabelas staging e carrega no historico

echo -e "$(horario): Inicio da limpeza do staging.\n-\n"

ListaTabelas="details reviews"
for TABLE in $ListaTabelas; do
	LoadHist $TABLE
done

### Remove arquivos temporarios e escreve no log

rm -f ${DUMP}/*.txt

ENDDATE=$(date +'%F %T')
echo "$SCRIPTNAME;$STARTDATE;$ENDDATE" >> $LOG

echo -e "$(horario):Fim da execucao.\n"

exit 0
