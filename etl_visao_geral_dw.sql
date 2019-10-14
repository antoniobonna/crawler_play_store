
-- dados_banco_central_dw.data

\! echo "Carregando dados na tabela data..."

INSERT INTO google_play_dw.data_visao_geral
SELECT DISTINCT data, date_part('week',data)::smallint, date_part('month',data)::smallint, date_part('year',data)::smallint
	FROM google_play.details_stg;

VACUUM ANALYZE google_play_dw.data_visao_geral;

----------------------------------------------------------------------------

-- google_play_dw.visao_geral

\! echo "Carregando dados na tabela fato visao_geral..."

INSERT INTO google_play_dw.visao_geral
SELECT d.dia, a.app_id, f.score, f.reviews, f.downloads, f.ultima_atualizacao
	FROM google_play.details_stg f
	JOIN google_play_dw.data_visao_geral d ON d.dia=f.data
	JOIN google_play_dw.app a ON a.app_id=f.app_id;

VACUUM ANALYZE google_play_dw.data_visao_geral;