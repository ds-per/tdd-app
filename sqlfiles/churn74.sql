SELECT
  date_format(:day, "%Y%m%d") as idtempo
, equip.idmodelomaterial as modelomaterial
, idprovincia as provincia
, s1.idproduto as from_produto
, "EXPIRADO" as tipo
, if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as `fact_count`
FROM
    consumo c1
    INNER JOIN subscricao s1 ON s1.idsubscricao = c1.idsubscricao
    inner join contaservico ct ON ct.idcontaservico = s1.idcontaservico
   	Inner join conta using(idconta)
    INNER JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico
    INNER JOIN morada m ON m.idmorada = ct.idmorada
    INNER JOIN comuna cm ON cm.idcomuna = m.idcomuna
    INNER JOIN municipio mp ON mp.idmunicipio = cm.idmunicipio
WHERE
    c1.datafim = :yest
    and s1.idproduto != 27
    and equip.idtipoequipamento = 1
    and (equip.dataanulacao is null or equip.dataanulacao >= :yest )
    AND ct.idcontaservico NOT IN (
        SELECT s2.idcontaservico
           FROM consumo c2
           INNER JOIN
            subscricao s2 using(idsubscricao)
            WHERE
                s2.idcontaservico = s1.idcontaservico
            AND c2.datainicio  in (:yest, :day)
            AND c2.idconsumo > c1.idconsumo)
GROUP BY idmodelomaterial, idprovincia , s1.idproduto
