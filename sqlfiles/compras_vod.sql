select
  cs.codigocontaservico
  , cli.codigocliente
  , upper(pdi.codigoproduto) as filme
  , pdi.preco
  from zapweb.consumo
  LEFT JOIN zapweb.produtoinstantaneo pdi ON pdi.idprodutoinstantaneo = consumo.idprodutoinstantaneo
  LEFT JOIN zapweb.produtotipo pt ON pt.idprodutotipo = pdi.idprodutotipo
  LEFT JOIN zapweb.contaservico cs ON cs.idcontaservico = consumo.idcontaservico
  inner join zapweb.conta c on c.idconta = cs.idconta
  inner join zapweb.cliente cli on cli.idcliente = c.idcliente
  WHERE
    pt.idbolsa = 5
    AND pdi.preco > 0
    and consumo.datainicio = %(day)s