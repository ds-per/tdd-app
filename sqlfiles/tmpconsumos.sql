SELECT
  equip.idmodelomaterial AS idmodelomaterialstb,
  mp.idprovincia,
  mp.idmunicipio AS idmunicipio,
  s2.idproduto AS 'from',
  s1.idproduto AS 'to',
  IF(s2.idproduto IS NULL
  , 'NOVO'
  , IF(s1.idproduto = s2.idproduto
  , 'MANTEM'
  , IF((s2.idproduto = 23 AND s1.idproduto IN (22, 24, 28))
       OR
       (s2.idproduto = 22 AND s1.idproduto IN (24, 28))
       OR
       (s2.idproduto = 24 AND s1.idproduto = 28)
  , 'DOWNGRADE'
  , IF((s2.idproduto = 28 AND s1.idproduto IN (22, 23, 24))
       OR
       (s2.idproduto = 24 AND s1.idproduto IN (22, 23))
       OR
       (s2.idproduto = 22 AND s1.idproduto = 23)
  , 'UPGRADE'
  , 'BUG'
)))) AS tipo,
  DATEDIFF(c1.datainicio, c2.datafim) AS demora,
  if(crs.idvoucher IS NOT NULL, itd.idloja,
     if(crs.idfacturaitem IS NOT NULL, aquisicao.idloja,
        if(crs.idcarregamentodirecto IS NOT NULL, ctd.idloja, 8))) AS loja,

  if(caixa_itd.idloja IS NULL, NULL,
     if(caixa_itd.idloja = mov.idloja, mov.idlojavirtual, caixa_itd.idloja)) AS loja_caixa,

  if(crs.idvoucher IS NOT NULL, voucher.activacao,
     if(crs.idfacturaitem IS NOT NULL, 'LOJA',
        if(crs.idcarregamentodirecto IS NOT NULL, 'SIRE', 'INTERNO'))) AS canal,
  if(conta.contahotel = 1, COUNT(*), count(DISTINCT ct.idcontaservico)) AS `fact_count`
FROM consumo c1
  INNER JOIN subscricao s1 ON (s1.idsubscricao = c1.idsubscricao)
  LEFT JOIN consumo c2 ON ( c2.idconsumo = (
                SELECT max(idconsumo) from consumo c4
                inner join subscricao s4 using(idsubscricao)
                where c4.datafim = (select max(c3.datafim)
                                    FROM consumo c3 inner JOIN subscricao s3 using(idsubscricao)
                                     WHERE c3.datafim < %(day)s
                                      AND s3.idcontaservico = s1.idcontaservico
                                      AND s3.idproduto != 27
                                    )
                and s4.idcontaservico = s1.idcontaservico))
  LEFT JOIN subscricao s2 ON s2.idsubscricao = c2.idsubscricao
    left join consumo c3 ON c3.idconsumo = (select max(idconsumo)
                                                    from consumo c5
                                                    inner join subscricao s5 using(idsubscricao)
                                                    where
                                                    s5.idcontaservico = s1.idcontaservico
                                                    and c5.datainicio < %(day)s
                                                    and c5.datafim >= %(day)s
                                                    and c5.idconsumo != c1.idconsumo
                                                    and s5.idproduto != 27)
  INNER JOIN contaservico ct ON ct.idcontaservico = s1.idcontaservico
  INNER JOIN conta USING (idconta)
  INNER JOIN subscricao_equipamento ON s1.idsubscricao = subscricao_equipamento.idsubscricao
  INNER JOIN equipamento equip ON equip.idequipamento = subscricao_equipamento.idequipamento
  INNER JOIN morada m ON m.idmorada = ct.idmorada
  INNER JOIN comuna cm ON cm.idcomuna = m.idcomuna
  INNER JOIN municipio mp ON mp.idmunicipio = cm.idmunicipio
  LEFT JOIN creditoservico crs ON crs.idcreditoservico = (
    SELECT max(idcreditoservico)
    FROM creditoservico
    WHERE ct.idconta = idconta
          AND DATE(data) <= c1.datainicio
          AND idbolsa IS NULL
          AND idcontaadiantamento IS NULL
          AND pi >= 0
          AND (pi >= 150 OR idtransferencia IS NOT NULL)
  )
  LEFT JOIN voucher ON voucher.idvoucher = crs.idvoucher
  LEFT JOIN itemstock it ON it.iditemstock =
                            (SELECT iditemstock
                             FROM itemstock
                             WHERE numeroserie = cast(voucher.numeroserie AS CHAR(12))
                             LIMIT 1)
  LEFT JOIN itemdisponivel itd ON itd.iditemdisponivel = it.iditemdisponivel
  LEFT JOIN itemstock caixa_it ON caixa_it.iditemstock = (
    SELECT iditemstock
    FROM itemstock
    WHERE numeroserie = equip.numeroserie
    LIMIT 1)
    LEFT JOIN itemdisponivel caixa_itd ON caixa_itd.iditemdisponivel = caixa_it.iditemdisponivel
    LEFT JOIN movimentostocklojavirtual mov ON equip.numeroserie = mov.numeroserie
    LEFT JOIN facturaitem fi ON fi.iditem = crs.idfacturaitem
  LEFT JOIN aquisicao ON aquisicao.idfactura = fi.idfactura
  LEFT JOIN carregamentodirecto ctd ON ctd.idcarregamentodirecto = crs.idcarregamentodirecto
WHERE
    c1.datainicio = %(day)s
    and c3.idconsumo is null
    and s1.idproduto != 27
    and ct.idtipocontaservico = 1
GROUP BY demora, idmodelomaterialstb, s1.idproduto, mp.idprovincia, mp.idmunicipio, tipo, loja, loja_caixa, canal
