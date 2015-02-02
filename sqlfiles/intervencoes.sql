select ot.idintervencao
	, ot.idtipointervencao
	, "criadas" as "estado"
	, e.descricao as "estado_actual"
	, idparceiro
	, idagente
	, 0 as "demora"
	, 0 as "demora_exec"
from acontecimento as a
inner join intervencao as ot on ot.idintervencao = a.idintervencao
inner join causa as c on c.idcausa = a.idcausa
inner join estado as e on e.idestado = c.idestadoseguinte
where Date(ot.dataabertura) = %(day)s
and ot.idtipointervencao in (1,2,3,4,5,7)
and a.dataacontecimento = (select MAX(aaux.dataacontecimento) from acontecimento aaux where aaux.idintervencao = a.idintervencao)

UNION ALL

select ot.idintervencao
	, ot.idtipointervencao
	, "distribuidas" as "estado"
	, e.descricao as "estado_actual"
	, a.idparceiro
	, a.idagente
	, (timestampdiff(HOUR, ot.dataabertura, (select Max(aaux.dataacontecimento) data from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where Date(aaux.dataacontecimento) = %(day)s and eaux.idestado = 3
                    and aaux.idintervencao = a.idintervencao))) as "demora"
	, 0 as "demora_exec"
from acontecimento as a
inner join intervencao as ot on ot.idintervencao = a.idintervencao
inner join causa as c on c.idcausa = a.idcausa
inner join estado as e on e.idestado = c.idestadoseguinte
where a.dataacontecimento = (select MAX(aaux.dataacontecimento) from acontecimento aaux where aaux.idintervencao = a.idintervencao)
and ot.idtipointervencao in (1,2,3,4,5,7)
and a.idintervencao in (select aaux.idintervencao from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where Date(aaux.dataacontecimento) = %(day)s and eaux.idestado = 3
                    and aaux.idintervencao = a.idintervencao)

UNION ALL

select ot.idintervencao
	, ot.idtipointervencao
	, if(e.descricao="Anulada", "anuladas", "concluidas") as estado
	, e.descricao as "estado_actual"
	, a.idparceiro
	, a.idagente
	, (timestampdiff(HOUR, ot.dataabertura, (select Max(aaux.dataacontecimento) data from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where Date(aaux.dataacontecimento) = %(day)s and (eaux.idestado = 8 or eaux.idestado = 4)
                    and aaux.idintervencao = a.idintervencao))) as "demora"
	, (timestampdiff(HOUR, (select Max(aaux.dataacontecimento) data from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where eaux.idestado = 3
                    and aaux.idintervencao = a.idintervencao),(select Max(aaux.dataacontecimento) data from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where Date(aaux.dataacontecimento) = %(day)s and (eaux.idestado = 8 or eaux.idestado = 4)
                    and aaux.idintervencao = a.idintervencao))) as "demora_exec"
from acontecimento as a
inner join intervencao as ot on ot.idintervencao = a.idintervencao
inner join causa as c on c.idcausa = a.idcausa
inner join estado as e on e.idestado = c.idestadoseguinte
where a.dataacontecimento = (select MAX(aaux.dataacontecimento) from acontecimento aaux where aaux.idintervencao = a.idintervencao)
and ot.idtipointervencao in (1,2,3,4,5,7)
and a.idintervencao in (select aaux.idintervencao from acontecimento as aaux
                    inner join causa as caux on caux.idcausa = aaux.idcausa
                    inner join estado as eaux on eaux.idestado = caux.idestadoseguinte
                    where Date(aaux.dataacontecimento) = %(day)s and (eaux.idestado = 8 or eaux.idestado = 4)
                    and aaux.idintervencao = a.idintervencao)
;
