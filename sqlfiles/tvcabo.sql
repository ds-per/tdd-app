drop table if exists btmpws1;
drop table if exists btmpws2;
drop table if exists btmpws21;
drop table if exists btmpws22;
drop table if exists btmpws3;
drop table if exists btmpws4;
drop table if exists btmpws41;
drop table if exists btmpws5;

set @data= %(day)s;

create  table btmpws1 select c.idwscomando as idwscomando, c.idwsstb as idwsstb, c.idwscartao as idwscartao,
c.idwstipocomando as idwstipocomando, datacriado as data,
if(c.idwstipocomando=3 or c.idwstipocomando=6,null,group_concat(p.nome)) as nome from wscomando c
    left join wscomando_produto cp on (c.idwscomando=cp.idwscomando)
    left join wsproduto p on cp.idwsproduto=p.idwsproduto where c.datacriado<@data and c.idwstipocomando!=7
    group by c.idwscomando;
     
    alter table btmpws1 add index (idwsstb);
    alter table btmpws1 add index (idwscartao);
    alter table btmpws1 add index (idwscomando);
     
    create  table btmpws21 select t1.idwscomando, ifnull(max(t2.idwscomando),0) as idwscomando2
    from btmpws1 t1
    left join btmpws1 t2 on (t1.idwsstb=t2.idwsstb and t2.idwscomando<t1.idwscomando)
    group by t1.idwscomando;
    alter table btmpws21 add index (idwscomando);
     
    create  table btmpws22 select t1.idwscomando, ifnull(max(t2.idwscomando),0) as idwscomando2
    from btmpws1 t1
    left join btmpws1 t2 on (t1.idwscartao=t2.idwscartao and t2.idwscomando<t1.idwscomando)
    group by t1.idwscomando;
    alter table btmpws22 add index (idwscomando);
     
    create  table btmpws2 select t21.idwscomando,
    t21.idwscomando2 as idwscomando21 ,t22.idwscomando2 as idwscomando22
    from btmpws21 t21, btmpws22 t22 where t21.idwscomando=t22.idwscomando;
    alter table btmpws2 add index (idwscomando);
    alter table btmpws2 add index (idwscomando21);
    alter table btmpws2 add index (idwscomando22);
     
    create  table btmpws3 select t2.idwscomando, t11.idwsstb, t11.idwscartao, t11.data, t11.nome,
    t12.idwscomando as idwscomando21, t12.nome as nome21, t13.idwscomando as idwscomando22, t13.nome as nome22 from btmpws1 t11, btmpws2 t2
    left join btmpws1 t12 on t2.idwscomando21=t12.idwscomando
    left join btmpws1 t13 on t2.idwscomando22=t13.idwscomando
    where t2.idwscomando=t11.idwscomando;
    alter table btmpws3 add index (idwscomando);
    alter table btmpws3 add index (idwscomando21);
    alter table btmpws3 add index (idwscomando22);
     
    update btmpws3 set nome21="None" where nome21 is null;
    update btmpws3 set nome22="None" where nome22 is null;
    update btmpws3 set nome="None" where nome is null;
     
     
    create  table btmpws41 select b3.idwscomando from btmpws3 b3
    left join btmpws3 b32 on b3.idwscomando=b32.idwscomando21
    where b32.idwscomando is null;
     
    create  table btmpws4 select b3.idwscomando from btmpws41 b41, btmpws3 b3
    left join btmpws3 b32 on b3.idwscomando=b32.idwscomando22
    where b32.idwscomando is null and b41.idwscomando=b3.idwscomando;

