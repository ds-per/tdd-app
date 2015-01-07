    select b3.data, stb.numerostb, c.numerocartao, b3.nome from btmpws4 b4, btmpws3 b3, wsstb stb, wscartao c
    where b3.idwscomando=b4.idwscomando and b3.idwsstb=stb.idwsstb and b3.idwscartao=c.idwscartao and b3.nome!="None"
