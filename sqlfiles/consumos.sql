SELECT
 equip.idmodelomaterial as modelomaterial
, idprovincia as provincia
, mp.idmunicipio as municipio
, s2.idproduto AS from_produto
, s1.idproduto AS to_produto
, IF(s2.idproduto is null
      , 'NOVO'
      , IF(s1.idproduto = s2.idproduto
      , 'MANTEM'
      , IF((s2.idproduto = 23 AND s1.idproduto in (22, 24, 28))
           OR
           (s2.idproduto = 22 AND s1.idproduto in (24, 28))
           OR
           (s2.idproduto = 24 AND s1.idproduto = 28)
      , 'DOWNGRADE'
      , IF((s2.idproduto = 28 AND s1.idproduto in (22, 23, 24))
           OR
           (s2.idproduto = 24 AND s1.idproduto in (22, 23))
           OR
           (s2.idproduto = 22 AND s1.idproduto = 23)
      , 'UPGRADE'
      , 'BUG'
    )))) AS tipo
, DATEDIFF(c1.datainicio, c2.datafim) AS 'demora'
, if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as `fact_count`
        FROM consumo c1
        INNER JOIN subscricao s1 ON ( s1.idsubscricao = c1.idsubscricao )
        LEFT JOIN consumo c2 ON ( c2.idconsumo = (
                SELECT max(idconsumo) from consumo c4
                inner join subscricao s4 using(idsubscricao)
                where c4.datafim = (select max(c3.datafim)
                                    FROM consumo c3 inner JOIN subscricao s3 using(idsubscricao)
                                     WHERE c3.datafim < c1.datainicio
                                      AND s3.idcontaservico = s1.idcontaservico
                                      AND s3.idproduto != 27
                                    )
                and s4.idcontaservico = s1.idcontaservico
        ))
    LEFT JOIN subscricao s2 ON  s2.idsubscricao = c2.idsubscricao
    INNER JOIN contaservico ct ON ct.idcontaservico =  s1.idcontaservico
    INNER JOIN conta using(idconta)
    INNER JOIN subscricao_equipamento on s1.idsubscricao = subscricao_equipamento.idsubscricao
    INNER JOIN equipamento equip ON equip.idequipamento = subscricao_equipamento.idequipamento
    INNER JOIN morada m ON m.idmorada = ct.idmorada
    INNER JOIN comuna cm ON cm.idcomuna = m.idcomuna
    INNER JOIN municipio mp ON mp.idmunicipio = cm.idmunicipio
    WHERE
    c1.datainicio = %(day)s
    and s1.idproduto != 27
    and ct.idtipocontaservico = 1
    GROUP BY demora, idmodelomaterial, from_produto, to_produto, idprovincia, municipio, tipo
