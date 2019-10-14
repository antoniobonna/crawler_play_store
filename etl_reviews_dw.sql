
-- dados_banco_central_dw.comentario

\! echo "Carregando dados na tabela comentario..."

COPY(
SELECT DISTINCT comentario
	FROM google_play.reviews_stg WHERE not comentario is null
	EXCEPT
SELECT comentario FROM google_play_dw.comentario
) to '/home/ubuntu/dump/comentario.txt';
COPY google_play_dw.comentario(comentario) FROM '/home/ubuntu/dump/comentario.txt';

VACUUM ANALYZE google_play_dw.comentario;

----------------------------------------------------------------------------

-- google_play_dw.data_comentario

\! echo "Carregando dados na tabela data_comentario..."

INSERT INTO google_play_dw.data_comentario
SELECT distinct data, date(data), date_part('week', data),date_part('month', data),date_part('year', data)
	FROM google_play.reviews_stg WHERE not comentario is null
	ORDER BY 1;

VACUUM ANALYZE google_play_dw.data_comentario;

----------------------------------------------------------------------------

-- google_play_dw.data_resposta

\! echo "Carregando dados na tabela data_resposta..."

INSERT INTO google_play_dw.data_resposta
SELECT distinct data_resposta, date(data_resposta), date_part('week', data_resposta),date_part('month', data_resposta),date_part('year', data_resposta)
	FROM google_play.reviews_stg WHERE not resposta is null
	AND data_resposta NOT IN (SELECT DISTINCT datetime_resposta FROM google_play_dw.data_resposta)
	ORDER BY 1;

VACUUM ANALYZE google_play_dw.data_resposta;

----------------------------------------------------------------------------

-- dados_banco_central_dw.resposta

\! echo "Carregando dados na tabela resposta..."

COPY(
SELECT DISTINCT resposta
	FROM google_play.reviews_stg WHERE not resposta is null
	AND resposta NOT IN (SELECT DISTINCT resposta FROM google_play_dw.resposta)
) to '/home/ubuntu/dump/resposta.txt';
COPY google_play_dw.resposta(resposta) FROM '/home/ubuntu/dump/resposta.txt';

VACUUM ANALYZE google_play_dw.resposta;

----------------------------------------------------------------------------

-- dados_banco_central_dw.usuario

\! echo "Carregando dados na tabela usuario..."

COPY(
SELECT DISTINCT user_id, usuario
	FROM google_play.reviews_stg WHERE not usuario is null
	AND user_id NOT IN (SELECT DISTINCT user_id FROM google_play_dw.usuario)
) to '/home/ubuntu/dump/usuario.txt';
COPY google_play_dw.usuario FROM '/home/ubuntu/dump/usuario.txt';

VACUUM ANALYZE google_play_dw.usuario;

----------------------------------------------------------------------------

-- dados_banco_central_dw.reviews

\! echo "Carregando dados na tabela fato reviews..."

COPY(
SELECT d.datetime, a.app_id, u.user_id, c.comentario_id, r.resposta_id, f.score
  FROM google_play.reviews_stg f
  JOIN google_play_dw.data_comentario d ON d.datetime = f.data
  JOIN google_play_dw.app a ON a.empresa = f.empresa
  JOIN google_play_dw.usuario u ON u.user_id = f.user_id
  JOIN google_play_dw.comentario c ON c.comentario = f.comentario
  LEFT JOIN google_play_dw.resposta r ON r.resposta = f.resposta AND r.datetime_resposta = f.data_resposta
) to '/home/ubuntu/dump/reviews.txt';
COPY google_play_dw.reviews FROM '/home/ubuntu/dump/reviews.txt';

VACUUM ANALYZE google_play_dw.reviews;
