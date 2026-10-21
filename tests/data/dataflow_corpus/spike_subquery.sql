select * from (select * from db.a) s join (select * from db.b) t on s.x=t.x
