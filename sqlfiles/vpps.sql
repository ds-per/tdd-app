select
  if(mslv.datavenda is null, 'distribuida',
    if (mslv.datavenda between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59'), 'vendida',
      if (mslv.dataanulacao between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59'), 'anulada', 'distribuida'))) as tipo
  , time_to_sec(timediff(if (mslv.datavenda between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59'), mslv.datavenda, null), mslv.dataentradastock)) / 3600 as tempo_abastecimento
  , time_to_sec(timediff(if (mslv.datavenda between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59'), mslv.datavenda, null), timestamp(ar.dataentrega,ar.horaentrega))) / 3600 as tempo_activacao
  , if(ar.viareclamacao is not null, 'RECLAMACAO',
      if(ar.viaregisto, 'REGISTO', 'DATAENTRY')) as via
  , p.nome as supervisor
  , mslv.idlojavirtual as idloja
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
  mslv.dataentradastock between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59')
	or mslv.datavenda between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59')
	or mslv.dataanulacao between timestamp(%(day)s, '00:00:00') and timestamp(%(day)s, '23:59:59')
  group by mslv.idloja, e.numeroserie, p.nome