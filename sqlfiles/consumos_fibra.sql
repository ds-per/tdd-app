SELECT
 equip.idmodelomaterial as modelomaterial
, idprovincia as provincia
, mp.idmunicipio as municipio
, s2.idproduto AS from_produto
, s1.idproduto AS to_produto
, IF(s2.idproduto = s1.idproduto, 'MANTEM',
    IF((s2.idproduto > s1.idproduto), 'DOWNGRADE',
        IF((s2.idproduto < s1.idproduto), 'UPGRADE', 'NOVO'))) AS tipo
, DATEDIFF(c1.datainicio, c2.datafim) AS 'demora'
, if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as `fact_count`
        FROM consumo c1
        INNER JOIN subscricao s1 ON ( s1.idsubscricao = c1.idsubscricao )
        LEFT JOIN consumo c2 ON ( c2.idconsumo = (
                SELECT MAX(idconsumo)
                FROM consumo c3 inner JOIN subscricao s3 using(idsubscricao)
                WHERE c3.idconsumo < c1.idconsumo
                AND s3.idcontaservico = s1.idcontaservico
                AND s3.idproduto  between 50 and 53
               ))
    LEFT JOIN subscricao s2 ON  s2.idsubscricao = c2.idsubscricao
    INNER JOIN contaservico ct ON ct.idcontaservico =  s1.idcontaservico
    INNER join conta using(idconta)
    left JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico
    INNER JOIN morada m ON m.idmorada = ct.idmorada
    INNER JOIN comuna cm ON cm.idcomuna = m.idcomuna
    INNER JOIN municipio mp ON mp.idmunicipio = cm.idmunicipio
    WHERE
    c1.datainicio = %(day)s
    and s1.idproduto between 50 and 53
    and equip.idtipoequipamento = 4
    and (equip.dataanulacao is null or equip.dataanulacao > %(day)s )
    GROUP BY demora, idmodelomaterial, s1.idproduto, idprovincia, municipio, tipo

