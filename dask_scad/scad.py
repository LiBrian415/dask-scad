"""
SCAD shared-memory scheduler

See local.py and multithreading.py from dask
"""
import base64
import cloudpickle
import os.path
import redis
import sys
import shutil

from dask import config
from dask.core import flatten, get_dependencies, has_tasks, reverse_dict
from dask.local import nested_get
from dask.optimization import cull, fuse
from dask.utils import ensure_dict

from jinja2 import Environment, FileSystemLoader

from tempfile import TemporaryDirectory
from utils import wskgen, wskish, run_disagg

def get(
    dsk,
    result,
    cache=None,
    optimize_graph=True,
    scad_output=None,
    obj_dir='temp',
    comp_config=None,
    **kwargs
):
    """
    How to use:
    get(dsk, key(s), scad_output={...})

    compute(scheduler=get, scad_output={...})

    with dask.config.set(scheduler=get, scad_output={...})
        ...

    Parameters
    ----------

    dsk : dict
        A dask dictionary specifying a workflow
    result: object or list
        Keys corresponding to desired data
    cache : dict-like, optional
        Temporary storage of results
    optimize_graph: bool
        If true [default], `fuse` is applied to the graph before computation
    scad_output : dict
        A dictionary specifying storage for output
        {
            'type': redis,
            'meta': dict() [type-specific metadata]
        }
    obj_dir : string
        The relative or absolute path to the directory storing generated Scad object files
    comp_config: dict
        A dictionary specifying the computation engine: openwhisk or run_disagg from Scad
        {
            'kind': openwhisk | run_disagg, [required]
            'mpath': str() [the absolute path to the memory_server binary in Scad, optional,
                            required if comp_config['kind'] == 'run_disagg'],
        }
    """

    scad_output = scad_output or config.get('scad_output')
    comp_config = comp_config or config.get('comp_config')

    # Optimize Dask
    dsk = ensure_dict(dsk)
    dsk2, dependencies = cull(dsk, result)
    if optimize_graph:
        dsk3, dependencies = fuse(dsk2, result, dependencies)
    else:
        dsk3 = dsk2

    # flattened set of unique keys
    if isinstance(result, list):
        keys_flat = set(flatten(result))
    else:
        keys_flat = {result}
    keys = set(keys_flat)

    compute, memory = process(dsk3, cache)

    # construct output element
    output = OutputElement(scad_output)
    output.parents = [compute[k] for k in keys]
    output.corunning = [memory[k] for k in keys]
    for p in output.parents:
        p.dependents.append(output)
    for c in output.corunning:
        c.corunning.append(output)

    # Note: use os.mkdir() for local testing
    # use temp directory to handle cleanup
    os.makedirs(obj_dir, exist_ok = True)
    generate(list(compute.values()) + [output] + list(memory.values()), obj_dir)

    if comp_config['kind'] == 'openwhisk':
        wskgen.generate(obj_dir, 'action.json')

        json_content = wskish.read_example('action.json')
        wsk_config = wskish.load_wskprops()
    
        host = wsk_config['APIHOST']
        if not host.startswith('http'):
            host = 'https://' + host

        auth = wsk_config['AUTH']
        wskprops = wskish.WskProps(host, auth)
        app_name = "app-test"

        resp = wskish.do_action_update(wskprops.host, "PUT", json_content, app_name, wskprops.auth)
    
    elif comp_config['kind'] == 'rundisagg':
        result_meta = run_disagg.run(obj_dir, comp_config['mpath'])
    else:
        sys.exit("wrong computation engine")

    # Comment line below if you need to debug simple elements
    shutil.rmtree(obj_dir)
    cache = load(result_meta, scad_output) # {'output': ans}
    return nested_get(result, cache)


class Element:
    def get_id(self):
        pass


class ComputeElement(Element):
    """
    Holds information relating to an individual Scad compute element

    name: str
        'func_' + name
    key: hashable
        name of key associated with the computation
        (None used to identify output node)
    corunning: list[MemoryElement]
        list of corunning elements
    parents: list[ComputeElement]
        list of parent elements
    dependents: list[ComputeElement/OutputElement]
        list of dependent elements
    computation: computation
        dask computation
    output: MemoryElement
        memory element used to store output of task
    """
    def __init__(
        self,
        name,
        key,
        computation,
        output=None,
        **kwargs
    ):
        self.name = name
        self.key = key
        self.computation = computation
        self.output = output
        self.parents = []
        self.dependents = []
        self.corunning = []

    def get_id(self):
        return 'func_' + self.name

    def get_fname(self):
        return self.get_id() + '.o.py'


class OutputElement(Element):
    """
        Holds information relating to an individual Scad compute element

        parents: list[ComputeElement]
            list of parent elements
        corunning: list[MemoryElement]
            list of corunning elements
        output: dict
            A dictionary specifying storage for output
        """

    def __init__(
            self,
            output,
            **kwargs
    ):
        self.output = output
        self.parents = []
        self.corunning = []

    def get_id(self):
        return 'output'

    def get_fname(self):
        return self.get_id() + '.o.py'


class MemoryElement(Element):
    """
    Holds information relating to an individual Scad memory element

    name: str
        'mem_' + name
    key: hashable
        name of key associated with the computation that outputs this element
    corunning: list[ComputeElement/OutputElement]
        list of corunning elements
    limits: dict
        dictionary of limits
    """
    def __init__(
        self,
        name,
        key,
        limits=None,
        **kwargs
    ):
        self.name = name
        self.key = key
        self.corunning = []
        self.limits = {'mem': '512 MB'} if limits is None else limits

    def get_id(self):
        return 'mem_' + self.name

    def get_fname(self):
        return self.get_id() + '.o.yaml'


def _construct(
    key,
    dsk,
    name,
    dependencies,
    dependents,
    compute,
    memory
):
    if key in compute or key in memory:
        return

    computation = dsk[key]
    # Give data_keys the identity function
    if not has_tasks(dsk, computation):
        computation = (lambda x: x, computation)

    memory[key] = MemoryElement(name[key], key)
    compute[key] = ComputeElement(name[key], key, computation, memory[key])

    def get_compute(k):
        _construct(k, dsk, name, dependencies, dependents, compute, memory)
        return compute[k]

    def get_memory(k):
        _construct(k, dsk, name, dependencies, dependents, compute, memory)
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
    # this also replaces values we already have in the dsk with the cached values
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

    compute = {}
    memory = {}
    name = {}
    for idx, key in enumerate(dsk2.keys()):
        name[key] = str(idx)

    # construct Scad elements
    for a in dsk2:
        _construct(a, dsk2, name, dependencies, dependents, compute, memory)

    return compute, memory


def generate(elements, dir):
    def generate_comp(el):
        template = get_template('function.j2')
        return template.render(
            key=cloudpickle.dumps(el.key),
            name=el.name,
            computation=cloudpickle.dumps(el.computation),
            parents=el.parents,
            dependents=el.dependents,
            corunning=el.corunning,
            output=el.output.get_id() if el.output is not None else None
        )

    def generate_output(el):
        template = get_template('output.j2')
        return template.render(
            output=el.output,
            parents=el.parents,
            corunning=el.corunning
        )

    def generate_mem(el):
        template = get_template('memory.j2')
        return template.render(corunning=el.corunning, limits=el.limits)

    for e in elements:
        generated = None
        if isinstance(e, ComputeElement):
            generated = generate_comp(e)
        elif isinstance(e, OutputElement):
            generated = generate_output(e)
        elif isinstance(e, MemoryElement):
            generated = generate_mem(e)

        if generated is not None:
            with open(os.path.join(dir, e.get_fname()), 'w+') as f:
                f.write(generated)


def load(meta, scad_output):
    def read_redis(output, cache):
        r = redis.Redis(host=scad_output['meta']['host'], port=scad_output['meta']['port'])
        for k, v in output.items():
            val = cloudpickle.loads(r.get(v))
            cache[k] = val

    output = cloudpickle.loads(base64.b64decode(meta['output']))
    cache = dict()

    if scad_output['type'] == 'redis':
        read_redis(output, cache)
    else:
        raise

    return cache


# Jinja2 stuff
TEMPLATE_PATHS = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")]


def get_environment():
    loader = FileSystemLoader(TEMPLATE_PATHS)
    environment = Environment(loader=loader)
    return environment


def get_template(name):
    return get_environment().get_template(name)

