SELECT   
  date_format(:day, "%Y%m%d") as idtempo
, stb.idmodelomaterialstb AS modelomaterial
, idprovincia as provincia, s1.idproduto as produto
, if(DATEDIFF(:day, xx.datafim)<=7, 7,
    if(DATEDIFF(:day, xx.datafim)<=15, 15,
    if(DATEDIFF(:day, xx.datafim)<=30, 30,
    if(DATEDIFF(:day, xx.datafim)<=60, 60,
    if(DATEDIFF(:day, xx.datafim)<=90, 90,
    if(DATEDIFF(:day, xx.datafim)<=120, 120,
    if(DATEDIFF(:day, xx.datafim)<=180, 180,
    if(DATEDIFF(:day, xx.datafim)<=270, 270,
    if(DATEDIFF(:day, xx.datafim)<=360, 360,
    if(DATEDIFF(:day, xx.datafim)>360, 361, NULL)))))))))) as 'tempoexp'
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
                        group by subscricao.idsubscricao
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
    GROUP BY idmodelomaterial, idprovincia, s1.idproduto, tempoexp

