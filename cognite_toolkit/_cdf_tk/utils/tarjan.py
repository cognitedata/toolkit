from cognite.client.data_classes.data_modeling import ViewId


def tarjan(dependencies_by_id: dict[ViewId, set[ViewId]]) -> list[set[ViewId]]:
    """Returns the strongly connected components of the dependency graph
     in topological order.

    Args:
        dependencies_by_id: A dictionary where the keys are the view ids and the values are the set of view ids
            that the key view depends on.

    Returns:
        A list of sets of view ids, where each set is a strongly connected component.
    """

    S = []
    S_set = set()
    index: dict[ViewId, int] = {}
    lowlink = {}
    ret = []

    def visit(v: ViewId) -> None:
        index[v] = len(index)
        lowlink[v] = index[v]
        S.append(v)
        S_set.add(v)
        for w in dependencies_by_id.get(v, []):
            if w not in index:
                visit(w)
                lowlink[v] = min(lowlink[w], lowlink[v])
            elif w in S_set:
                lowlink[v] = min(lowlink[v], index[w])
        if lowlink[v] == index[v]:
            scc = set()
            dependency: ViewId | None = None
            while v != dependency:
                dependency = S.pop()
                scc.add(dependency)
                S_set.remove(dependency)
            ret.append(scc)

    for view_id in dependencies_by_id.keys():
        if view_id not in index:
            visit(view_id)
    return ret
