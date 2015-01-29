SELECT
 idprovincia as provincia
, mp.idmunicipio as municipio
, s1.idproduto as produto
, ct.idcontaservico
, conta.contahotel
, c1.idconsumo, c1.datainicio, c1.datafim
, equip.idequipamento
, equip.numeroserie
, equip.dataanulacao
, equip.idmodelomaterial AS modelomaterial
FROM
    ( select max(cc.idconsumo) as maxidconsumo
            from consumo cc
            inner join subscricao on subscricao.idsubscricao = cc.idsubscricao
            where cc.datainicio <= %(day)s
            and idproduto > 27
            group by subscricao.idcontaservico
    ) as xx
    inner join consumo c1 on c1.idconsumo = xx.maxidconsumo
    INNER JOIN subscricao s1 ON ( s1.idsubscricao = c1.idsubscricao )
    INNER JOIN contaservico ct ON ct.idcontaservico = s1.idcontaservico
    INNER JOIN conta using(idconta)
    LEFT JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico and equip.idtipoequipamento > 1
    INNER JOIN morada m on m.idmorada = ct.idmorada
    INNER JOIN comuna cm on cm.idcomuna = m.idcomuna
    INNER JOIN municipio mp on mp.idmunicipio = cm.idmunicipio
       WHERE
        s1.idproduto > 27
