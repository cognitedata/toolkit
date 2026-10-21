select a, b from db.t lateral view explode(arr) x as b
