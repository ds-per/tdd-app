
parque = """
SELECT
    date_format(:day, "%Y%m%d") as idtempo
    , IF(stb.idstb IS NOT NULL, stb.idmodelomaterialstb, equip.idmodelomaterial) AS modelomaterial
    , s.idproduto as produto
    , idprovincia as provincia
    , mp.idmunicipio as municipio
    , if(conta.contahotel = 1, COUNT(*), count(ct.idcontaservico)) as fact_count
        FROM consumo c
        INNER JOIN subscricao s ON c.idsubscricao = s.idsubscricao
        left JOIN stb on stb.idstb = s.idstb
        inner JOIN contaservico ct on ct.idcontaservico = if(s.`idcontaservico` is null, stb.idcontaservico, s.idcontaservico)
        INNER JOIN conta using(idconta)
        LEFT JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico
        INNER JOIN morada m on m.idmorada = ct.idmorada
        INNER JOIN comuna cm on cm.idcomuna = m.idcomuna
        INNER JOIN municipio mp on mp.idmunicipio = cm.idmunicipio
        WHERE
            c.datainicio <= :day
            AND c.datafim >= :day
            and s.idproduto != 27
        GROUP BY idmodelomaterial, s.idproduto, idprovincia, mp.idmunicipio
"""
