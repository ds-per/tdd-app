SELECT   
  date_format(:day, "%Y%m%d") as idtempo
, stb.idmodelomaterialstb AS modelomaterial
, idprovincia as provincia, s1.idproduto as produto
, if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as fact_count
                FROM
                (
                    select f.idsubscricao, f.idconsumo, f.datainicio, f.datafim, f.valor, f.estado
                    from (
                        select subscricao.idsubscricao, max(idconsumo) as maxidconsumo
                        from consumo cc
                        inner join subscricao on subscricao.idsubscricao = cc.idsubscricao
                        where cc.datafim < :day
                        and idproduto != 27
                    ) as x inner join consumo as f on f.idsubscricao = x.idsubscricao and f.idconsumo = x.maxidconsumo
                ) as xx
                INNER JOIN subscricao s1 ON ( s1.idsubscricao = xx.idsubscricao )
                INNER JOIN contaservico ct ON ct.idcontaservico = s1.idcontaservico
                INNER JOIN conta using(idconta)
                INNER JOIN stb  ON stb.idcontaservico = ct.idcontaservico
                INNER JOIN morada m on m.idmorada = ct.idmorada
                INNER JOIN comuna cm on cm.idcomuna = m.idcomuna
                INNER JOIN municipio mp on mp.idmunicipio = cm.idmunicipio
       WHERE
        s1.idproduto != 27
        AND (stb.dataanulacao is null or stb.dataanulacao > :day)
        AND ct.idcontaservico NOT IN (SELECT
            s2.idcontaservico
            FROM consumo c2
            INNER JOIN subscricao s2 using(idsubscricao)
            WHERE
            s2.idcontaservico = s1.idcontaservico
            AND s2.idproduto != 27
            AND c2.idconsumo > xx.idconsumo
            AND c2.datainicio <= :day)
    GROUP BY idmodelomaterial, idprovincia, s1.idproduto

