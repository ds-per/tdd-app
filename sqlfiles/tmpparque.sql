SELECT 
p.idprovincia AS provincia
, m.idmunicipio AS municipio
      , sum(IF(fc.tipo='EXPIRADO', fc.fact_count, 0)) AS Churn
      , sum(IF(fc.tipo='NOVO', fc.fact_count, 0)) AS GrossAdds
      , sum(IF(fc.demora>1, fc.fact_count, 0)) AS Reactivacoes
      , (SELECT sum(fp.fact_count) FROM fct_parque fp WHERE fp.idtempo = %(idtempo)s AND fp.municipio = m.idmunicipio) 
          - (SELECT sum(fp.fact_count) FROM fct_parque fp WHERE fp.idtempo = %(idtempo)s  AND fp.municipio = m.idmunicipio) AS NetAdds
      , (SELECT sum(fp.fact_count) FROM fct_parque fp WHERE fp.idtempo = %(idtempo)s AND fp.municipio = m.idmunicipio) AS Parque
            FROM
                municipio m
                INNER JOIN provincia p ON p.idprovincia = m.idprovincia
                LEFT JOIN tmp_fct_consumos fc ON fc.idtempo = %(idtempo)s AND fc.idmunicipio = m.idmunicipio
            GROUP BY p.idprovincia, m.idmunicipio
