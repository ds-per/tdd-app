SELECT
 equip.idmodelomaterial as idmodelomaterialstb
, idprovincia
, mp.idmunicipio as idmunicipio
, s2.idproduto AS 'from'
, s1.idproduto AS 'to'
, IF(s1.idproduto = s2.idproduto, 'MANTEM',
    IF((s2.idproduto = 23 AND s1.idproduto = 22) OR
        (s2.idproduto = 23 AND s1.idproduto = 24) OR
        (s2.idproduto = 22 AND s1.idproduto = 24), 'DOWNGRADE',
        IF((s2.idproduto = 24 AND s1.idproduto = 22) OR
            (s2.idproduto = 24 AND s1.idproduto = 23) OR
            (s2.idproduto = 22 AND s1.idproduto = 23), 'UPGRADE', 'NOVO'))) AS tipo
, DATEDIFF(c1.datainicio, c2.datafim) AS demora
, if(crs.idvoucher is not null, itd.idloja,
        if(crs.idfacturaitem is not null, aquisicao.idloja,
        if(crs.idcarregamentodirecto is not null, ctd.idloja, 8))) as loja
, caixa_itd.idloja as loja_caixa
, if(crs.idvoucher is not null, voucher.activacao,
        if(crs.idfacturaitem is not null, 'LOJA',
        if(crs.idcarregamentodirecto is not null, 'SIRE', 'INTERNO'))) as canal
, if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) as `fact_count`
        FROM consumo c1
        INNER JOIN subscricao s1 ON ( s1.idsubscricao = c1.idsubscricao )
        LEFT JOIN consumo c2 ON ( c2.idconsumo = (
                SELECT MAX(idconsumo)
                FROM consumo c3 inner JOIN subscricao s3 using(idsubscricao)
                WHERE c3.idconsumo < c1.idconsumo
                AND s3.idcontaservico = s1.idcontaservico
                AND s3.idproduto != 27
        ))
    LEFT JOIN subscricao s2 ON  s2.idsubscricao = c2.idsubscricao
    INNER JOIN contaservico ct ON ct.idcontaservico =  s1.idcontaservico
    INNER join conta using(idconta)
    INNER JOIN equipamento equip ON equip.idcontaservico = ct.idcontaservico
    INNER JOIN morada m ON m.idmorada = ct.idmorada
    INNER JOIN comuna cm ON cm.idcomuna = m.idcomuna
    INNER JOIN municipio mp ON mp.idmunicipio = cm.idmunicipio
    LEFT JOIN creditoservico crs ON crs.idcreditoservico = (
                SELECT max(idcreditoservico) FROM creditoservico
                    WHERE ct.idconta = idconta
                        AND DATE(DATA) <= c1.datainicio
                        AND idbolsa IS NULL
                        AND idcontaadiantamento IS NULL
                        AND pi >= 0
                        AND (pi >= 150 OR idtransferencia IS NOT NULL)
            )
    LEFT JOIN voucher ON voucher.idvoucher = crs.idvoucher
    LEFT JOIN itemstock it ON it.iditemstock = (select iditemstock from itemstock where numeroserie = cast(voucher.numeroserie as char(12)) limit 1)
    LEFT JOIN itemdisponivel itd ON itd.iditemdisponivel = it.iditemdisponivel
    LEFT JOIN itemstock caixa_it ON caixa_it.iditemstock = (
                  select iditemstock from itemstock where numeroserie = equip.numeroserie limit 1)
    LEFT JOIN itemdisponivel caixa_itd ON caixa_itd.iditemdisponivel = caixa_it.iditemdisponivel
    LEFT JOIN facturaitem fi ON fi.iditem = crs.idfacturaitem
    LEFT JOIN aquisicao ON aquisicao.idfactura = fi.idfactura
    LEFT JOIN carregamentodirecto ctd ON ctd.idcarregamentodirecto = crs.idcarregamentodirecto
    WHERE
    c1.datainicio = %(day)s
    and s1.idproduto != 27
    and equip.idtipoequipamento = 1
    and (equip.dataanulacao is null or equip.dataanulacao > %(day)s )
    GROUP BY demora, idmodelomaterialstb, s1.idproduto, idprovincia, idmunicipio, tipo
