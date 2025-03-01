from typing import TypeVar

T = TypeVar("T")


def tarjan(dependencies_by_id: dict[T, set[T]]) -> list[set[T]]:
    """Returns the strongly connected components of the dependency graph
     in topological order.

    Args:
        dependencies_by_id: A dictionary where the keys are ids and the values are sets of ids that the key depends on.

    Returns:
        A list of sets of ids that are strongly connected components in the dependency graph.
    """

    stack = []
    stack_set = set()
    index: dict[T, int] = {}
    lowlink = {}
    result = []

    def visit(v: T) -> None:
        index[v] = len(index)
        lowlink[v] = index[v]
        stack.append(v)
        stack_set.add(v)
        for w in dependencies_by_id.get(v, []):
            if w not in index:
                visit(w)
                lowlink[v] = min(lowlink[w], lowlink[v])
            elif w in stack_set:
                lowlink[v] = min(lowlink[v], index[w])
        if lowlink[v] == index[v]:
            scc = set()
            dependency: T | None = None
            while v != dependency:
                dependency = stack.pop()
                scc.add(dependency)
                stack_set.remove(dependency)
            result.append(scc)

    for view_id in dependencies_by_id.keys():
        if view_id not in index:
            visit(view_id)
    return result
