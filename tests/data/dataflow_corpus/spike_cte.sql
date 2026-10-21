with c as (select * from db.a), d as (select * from c) select * from d join db.b on 1=1
