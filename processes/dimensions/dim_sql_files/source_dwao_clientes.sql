select
	cli.iddim_cliente
	, cli.codigocliente
	, cli.hash
	, cli.version
	, cli.source
from dim_cliente cli
where cli.source = %(source)s
