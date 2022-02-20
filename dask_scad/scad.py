"""
SCAD shared-memory scheduler

See local.py and multithreading.py from dask
"""
from dask.core import flatten, get_dependencies, has_tasks, reverse_dict
from dask.optimization import cull, fuse
from dask.utils import ensure_dict


def get(
    dsk,
    keys,
    cache=None,
    optimize_graph=True,
    **kwargs
):
    """
    Parameters
    ----------

    dsk : dict
        A dask dictionary specifying a workflow
    keys: object or list
        Keys corresponding to desired data
    cache : dict-like, optional
        Temporary storage of results
    optimize_graph: bool
        If true [default], `fuse` is applied to the graph before computation
    """

    # Optimize Dask
    dsk = ensure_dict(dsk)
    dsk2, dependencies = cull(dsk, keys)
    if optimize_graph:
        dsk3, dependencies = fuse(dsk2, keys, dependencies)
    else:
        dsk3 = dsk2

    # flattened set of unique keys
    if isinstance(keys, list):
        keys_flat = set(flatten(keys))
    else:
        keys_flat = {keys}
    keys = set(keys_flat)

    compute, memory = process(dsk3, keys, cache)


class Element:
    """
    Holds generic information relating to any Scad element

    key: str
        key that this element is associated with
    corunning: list[Element]
        list of corunning elements
    """
    def __init__(
        self,
        key,
        **kwargs
    ):
        self.key = key
        self.corunning = []

    def get_id(self):
        pass

    def generate(self, dst):
        pass


class ComputeElement(Element):
    """
    Holds information relating to an individual Scad compute element

    key: str
        name of key associated with the computation
    corunning: list[Element]
        list of corunning elements
    parents: list[Element]
        list of parent elements
    dependents: list[Element]
        list of dependent elements
    computation: computation
        dask computation
    """
    def __init__(
        self,
        key,
        task,
        **kwargs
    ):
        super().__init__(key, **kwargs)
        self.task = task
        self.parents = []
        self.dependents = []

    def get_id(self):
        pass

    def generate(self, dst):
        pass


class MemoryElement(Element):
    """
    Holds information relating to an individual Scad memory element

    key: str
        name of key associated with the computation that outputs this element
    corunning: list[Element]
        list of corunning elements
    limits: dict
        dictionary of limits
    """
    def __init__(
        self,
        key,
        limits=None,
        **kwargs
    ):
        super().__init__(key, **kwargs)
        self.limits = dict() if limits is None else limits

    def get_id(self):
        pass

    def generate(self, dst):
        pass


def _construct(
    key,
    dsk,
    dependencies,
    dependents,
    compute,
    memory
):
    if key in compute or key in memory:
        return

    compute[key] = ComputeElement(key, dsk[key])
    memory[key] = MemoryElement(key)

    def get_compute(k):
        if k not in compute:
            _construct(k, dsk, dependencies, dependents, compute, memory)
        return compute[k]

    def get_memory(k):
        if k not in memory:
            _construct(k, dsk, dependencies, dependents, compute, memory)
        return memory[k]

    # compute adjacent nodes
    pars = [get_compute(k) for k in dependencies[key]]
    deps = [get_compute(k) for k in dependents[key]]
    corun = [get_memory(k) for k in dependencies[key]]

    # set edges
    compute[key].parents.extend(pars)
    compute[key].dependents.extend(deps)
    compute[key].corunning.append(memory[key])
    compute[key].corunning.extend(corun)

    memory[key].corunning.append(compute[key])
    memory[key].corunning.extend(deps)


def process(
    dsk,
    keys,
    cache=None,
    **kwargs
):
    """
    Processes a task graph and outputs:
        - a list of compute elements
        - a list of memory elements

    Parameters
    ----------

    dsk : dict
        A dask dictionary specifying a workflow
    keys: object or list
        Keys corresponding to desired data
    cache : dict-like, optional
        Temporary storage of results
    """

    # setup
    dsk = dict(dsk)
    if cache is None:
        cache = dict()

    # extract keys with no tasks (e.g, {'x': 1})
    data_keys = set()
    for k, v in dsk.items():
        if not has_tasks(dsk, v):
            cache[k] = v
            data_keys.add(k)

    # create task graph with cache included
    dsk2 = dsk.copy()
    dsk2.update(cache)

    # compute dependencies and dependents
    dependencies = {k: get_dependencies(dsk2, k) for k in dsk}
    dependents = reverse_dict(dependencies)
    waiting_data = {k: v.copy() for k, v in dependents.items() if v}

    # prune unnecessary cache values
    for a in list(cache):
        if a not in waiting_data:
            del cache[a]

    # TODO: Add output task here

    compute = {}
    memory = {}

    # construct Scad elements
    for a in dsk2:
        _construct(a, dsk2, dependencies, dependents, compute, memory)

    return compute.values(), memory.values()
