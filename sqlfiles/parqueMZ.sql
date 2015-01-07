SELECT
    date_format(:day, "%Y%m%d") as idtempo
    , equip.idmodelomaterial AS modelomaterial
    , s.idproduto as produto
    , idprovincia as provincia
    , mp.idmunicipio as municipio
    , if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as fact_count
        FROM consumo c
        INNER JOIN subscricao s ON c.idsubscricao = s.idsubscricao
        inner JOIN contaservico ct on ct.idcontaservico = s.idcontaservico
        INNER JOIN conta using(idconta)
        INNER JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico
        INNER JOIN morada m on m.idmorada = ct.idmorada
        INNER JOIN comuna cm on cm.idcomuna = m.idcomuna
        INNER JOIN municipio mp on mp.idmunicipio = cm.idmunicipio
        WHERE
            c.datainicio <= :day
            AND c.datafim >= :day
            AND s.idproduto != 27
            and equip.idtipoequipamento = 1
    		and (equip.dataanulacao is null or equip.dataanulacao > :day )
        GROUP BY idmodelomaterial, s.idproduto, idprovincia, mp.idmunicipio

