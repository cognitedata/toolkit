def diff_list_str(local: list[str], cdf: list[str]) -> tuple[dict[int, int], list[int]]:
    local_by_cdf: dict[int, int] = {}
    added: list[int] = []
    index_by_local = {item: i for i, item in enumerate(local)}
    for index, item in enumerate(cdf):
        if item in index_by_local:
            local_by_cdf[index_by_local[item]] = index
        else:
            added.append(index)
    return local_by_cdf, added
