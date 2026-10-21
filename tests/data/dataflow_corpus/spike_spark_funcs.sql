select to_json(struct(a, b)) j, map_from_arrays(k, v) m, cast(a as string) s from db.t
