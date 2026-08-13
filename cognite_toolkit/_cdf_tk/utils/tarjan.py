from typing import TypeVar

T = TypeVar("T")
V = TypeVar("V")


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


def pack_into_batches(
    dependencies_by_id: dict[T, set[T]], items_by_id: dict[T, V], batch_limit: int
) -> tuple[list[list[V]], list[set[T]]]:
    """Packs items into batches respecting their topological order, up to a maximum batch size.

    Computes the strongly connected components (SCCs) of the dependency graph in topological order,
    then packs consecutive SCCs into batches up to batch_limit. An SCC is never split across batches,
    even if it exceeds batch_limit on its own, since its items are interdependent.

    Args:
        dependencies_by_id: A dictionary where the keys are ids and the values are sets of ids that the key
            depends on. Every id that should be included in the output must be present as a key.
        items_by_id: A mapping from id to the item to include in the output batches.
        batch_limit: The maximum number of items allowed in a single batch.

    Returns:
        A tuple of (batches, oversized_sccs), where batches are the items packed into topologically
        ordered batches, and oversized_sccs are the strongly connected components that exceeded
        batch_limit on their own.
    """
    batches: list[list[V]] = []
    oversized_sccs: list[set[T]] = []
    current_batch: list[V] = []
    for strongly_connected in tarjan(dependencies_by_id):
        scc_items = [items_by_id[item_id] for item_id in strongly_connected]
        if len(current_batch) + len(scc_items) > batch_limit and len(current_batch) > 0:
            batches.append(current_batch)
            current_batch = []
        current_batch.extend(scc_items)
        if len(scc_items) > batch_limit:
            oversized_sccs.append(strongly_connected)
    if len(current_batch) > 0:
        batches.append(current_batch)
    return batches, oversized_sccs
