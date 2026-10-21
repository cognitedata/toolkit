select * from db.t where is_new('tag', cast(lastUpdatedTime as long))
