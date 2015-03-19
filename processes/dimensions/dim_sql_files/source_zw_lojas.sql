select
  l.idloja
  , l.nome
  , l.estrutura
  , l.estado
  , provi.nome as nome_provincia
  , mun.nome as nome_municipio
  , p.codigo
  , p.nome as parceiro
from
  zapweb.loja l
  left join zapweb.morada mor on (mor.idmorada = l.idmorada)
  left join zapweb.comuna com on (com.idcomuna = mor.idcomuna)
  left join zapweb.municipio mun on (mun.idmunicipio = com.idmunicipio)
  left join zapweb.provincia provi on (provi.idprovincia = mun.idprovincia)
  inner join zapweb.parceiro p on (p.idparceiro = l.idparceiro)