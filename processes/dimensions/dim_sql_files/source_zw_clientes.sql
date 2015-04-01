select
	distinct(cli.codigocliente)
	, cli.nome
	, cli.sexo
	, cli.datanascimento
	, cli.estadocivil
	, cli.telefone
	, cli.telemovel
	, cli.email
	, prov.nome as provincia
	, mun.nome as municipio
from zapweb.cliente cli
	inner join zapweb.conta c on (c.idcliente = cli.idcliente)
	left join zapweb.morada mor on (mor.idmorada = cli.idmorada)
	left join zapweb.comuna com on (com.idcomuna = mor.idcomuna)
	left join zapweb.municipio mun on (mun.idmunicipio = com.idmunicipio)
	left join zapweb.provincia prov on (prov.idprovincia = mun.idprovincia)
	order by cli.codigocliente desc limit 100