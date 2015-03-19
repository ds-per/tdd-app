select
  if(mslv.datavenda is not null, 'vendida', 'distribuida') as tipo
  , time_to_sec(timediff(mslv.datavenda, mslv.dataentradastock)) / 3600 as tempo_abastecimento
  , time_to_sec(timediff(mslv.datavenda, timestamp(ar.dataentrega,ar.horaentrega))) / 3600 as tempo_activacao
  , if(ar.viareclamacao is not null, 'RECLAMACAO',
      if(ar.viaregisto, 'REGISTO', 'DATAENTRY')) as via
  , p.nome as supervisor
  , mslv.idloja
  , e.numeroserie
from
	zapweb.movimentostocklojavirtual mslv
  left join zapweb.aquisicao a on mslv.idfactura = a.idfactura
  left join zapweb.aquisicaoregisto ar on a.idaquisicao = ar.idaquisicao
  inner join zapweb.loja_lojavirtual lv on mslv.idlojavirtual = lv.idlojavirtual
  inner join zapweb.loja l on l.idloja = lv.idloja
  inner join zapweb.equipamento e on mslv.numeroserie = e.numeroserie
  inner join zapweb.promotores p on p.id = mslv.idpromotorpai
  inner join zapweb.facturaitem fi on fi.idfactura = mslv.idfactura
  left join zapweb.notacreditoitem nci on nci.idfacturaitem = fi.iditem
  left join zapweb.notacredito nc on nc.idnotacredito = nci.idnotacredito
where
  mslv.dataentradastock between %(day)s and DATE_ADD(%(day)s, INTERVAL 1 DAY)
	or mslv.datavenda between %(day)s and DATE_ADD(%(day)s, INTERVAL 1 DAY)
	or mslv.dataanulacao between %(day)s and DATE_ADD(%(day)s, INTERVAL 1 DAY)
  group by mslv.idloja, e.numeroserie, p.nome