#!/usr/bin/env python3.9

import time
import atexit
import threading
import importlib.util
import requests
import argparse
import json
import subprocess
import random
import os
import sys
import yaml
import subprocess
import multiprocessing.pool

from os import listdir
from os.path import isfile, join
import dask.local as dask_local
from dask.local import MultiprocessingPoolExecutor, get_async
from concurrent.futures import Executor, ThreadPoolExecutor
from threading import Lock, current_thread
from collections import defaultdict
from dask.system import CPU_COUNT

from disagg import LibdAction

from glob import glob

dir_path = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.join(dir_path, '../lib')

default_pool: Executor or None = None
main_thread = current_thread()
pools: defaultdict[threading.Thread, dict[int, Executor]] = defaultdict(dict)
pools_lock = Lock()

# Function for extracting the meta info
def metaline(line, filetype):
    if filetype == '.py':
        if line.startswith('#@ '):
            return True, line[3:]
    elif filetype == '.yaml':
        return True, line
    return False, None

def filemeta(f):
    name = os.path.basename(f).split('.')[0]
    filetype = os.path.splitext(f)[1]

    metaphase = True
    metalist = [
        "name: {}\n".format(name),
        "filetype: {}\n".format(filetype)
    ]
    sourcelist = []

    print("processing object file", f, "type", filetype)
    with open(f) as source:
        for line in source.readlines():
            is_meta, meta = metaline(line, filetype)
            if metaphase and is_meta:
                metalist.append(meta)
            else:
                is_meta = False
                sourcelist.append(line)
    return metaphase, yaml.load(''.join(metalist), Loader = yaml.Loader), ''.join(sourcelist)
        
class ParamLoader:
    pass

class FileParamLoader(ParamLoader):
    def __init__(self, infiles = {}, outfile = None):
        print('build with infiles', infiles, 'outfiles', outfile)
        self.infiles = infiles
        self.outfile = outfile
        self.rv = None

    def get(self):
        for obj in self.infiles:
            filename = "rundisagg.{}.json".format(obj)
            self.infiles[obj] = glob(os.path.join(self.path, filename))

        params = {} 
        for obj in self.infiles:
            params[obj] = []
            for filename in self.infiles[obj]:
                with open(filename) as f:  
                    params[obj].append(json.load(f))

        return params

    def put(self, rv):
        self.rv = rv
        if self.outfile is not None:
            with open(self.outfile, 'w') as f:  
                json.dump(rv, f)

class MetaFileParamLoader(FileParamLoader):
    def __init__(self, meta, parIndex, path="/tmp"):
        self.path = path
        self.parIndex = parIndex
        infiles = {
            o:self.ioInFile(o)
            for o in meta.get('parents', []) }
        outfile = self.ioOutFile(meta['name'])
        super().__init__(infiles, outfile)

    def ioOutFile(self, obj):
        filename = "rundisagg.{}.json".format(obj)
        if self.parIndex is not None:
            filename = "rundisagg.{}@{}.json".format(obj, self.parIndex)
        return os.path.join(self.path, filename)

    def ioInFile(self, obj):
        filename = "rundisagg.{}.json".format(obj)
        files = glob(os.path.join(self.path, filename))
        return files

def execute(filename, action, loader):
    spec = importlib.util.spec_from_file_location("module.name", filename)
    __func = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(__func)

    params = loader.get()
    rv = __func.main(params, action)
    loader.put(rv)

def build_action(activation_id, transports, **kwargs):
    cv = threading.Condition()
    action = LibdAction(cv, activation_id, **kwargs)
    for transport in transports:
        action.add_transport(transport)
    return action

def parse_memory_transports(mems):
    mems = mems or []
    template = "{0};rdma_{2};url,tcp://localhost:{1};"

    def build(args):
        # no implementation
        if len(args) == 2:
            args.append('tcp')

        # set server
        if 'server' in args[2]:
            args[1] = 'tcp://*:' + args[1]
        else:
            args[1] = 'tcp://localhost:' + args[1]
        
        # see other parameters as python
        extra_params = ';'.join(args[3:])
        if len(extra_params) > 0:
            extra_params = extra_params + ';'
        args = args[:3]
        args.append(extra_params)
        return "{0};rdma_{2};url,{1};{3}".format(*args)

    def parse(mem):
        args = mem
        parNum = '0'
        if '@' in mem:
            parNum, args = mem.split('@')

        args = args.split(':') 

        if int(parNum) <= 1:
            return [build(args)]
        else:
            transports = []
            for i in range(int(parNum)):
                _args = args.copy()
                _args[0] = '{}@{}'.format(args[0], i)
                _args[1] = str(int(args[1]) + i)
                transports.append(build(_args))
            return transports

    return [t for m in mems for t in parse(m)]

def pack_exception(e, dumps):
    return e, sys.exc_info()[2]

def _thread_get_id():
    return current_thread().ident


def run(input, memory_server_path):
    objfiles = [join(input, f) for f in listdir(input) if isfile(join(input, f)) and join(input, f).endswith('.o.py')]
    memfiles = [f for f in listdir(input) if isfile(join(input, f)) and join(input, f).endswith('.o.yaml')]
    
    port = 30442
    mem_to_port = dict()
    mem_procs = []
    # run each memory element by 'memory_server' in separated processes
    for mem in memfiles:
        mem_to_port[mem.rsplit(".o", 1)[0]] = port
        mem_procs.append(subprocess.Popen([memory_server_path, str(port)]))
        port = port + 1

    print("wait 5 seconds to ensure memory elements are up")
    time.sleep(5)
    # build action profile for execution
    action_profile = {}
    for file in objfiles:
        _, meta, _ = filemeta(file)
        params = MetaFileParamLoader(meta, None)

        mem_list = []
        if 'corunning' in meta:
            for mem in meta['corunning']:
                mem_list.append(mem + ':' + str(mem_to_port[mem]))
        else:
            mem_list = None

        transports = parse_memory_transports(mem_list)

        activation_id = meta['name']
        action_args = {
            # 'post_url': 'http://localhost:2400/',
            # 'plugins': 'monitor'
        }
        
        print('building action with transports', transports, action_args)
        action = build_action(activation_id, transports, **action_args)

        if 'parents' in meta:
            parents = meta['parents']
        else:
            parents = None
        
        if 'dependents' in meta:
            dependents = meta['dependents']
        else:
            dependents = None

        action_profile[meta['name']] = {'file': file, 'action': action, 'params': params, \
                                    'parents': parents, 'dependents': dependents}
     
    # construct task graphs by action_profile
    simple_task_graph = dict()

    def dummy_func(file, action, params, *args):
        execute(file, action, params)
        return 0

    for func in action_profile:
        file = action_profile[func]['file']
        action = action_profile[func]['action']
        params = action_profile[func]['params']
        if action_profile[func]['parents'] is None:
            simple_task_graph[func] = (dummy_func, file, action, params)
        else:
            simple_task_graph[func] = (dummy_func, file, action, params, *action_profile[func]['parents'])


    # adapt the code from dask/local.py/start_state_from_dask
    result = [['output']]

    # TODO might have bug from here
    default_pool = None
    pool = None
    num_workers = None
    pool = pool or None
    num_workers = num_workers or None
    thread = current_thread()

    with pools_lock:
        if pool is None:
            if num_workers is None and thread is main_thread:
                if default_pool is None:
                    default_pool = ThreadPoolExecutor(CPU_COUNT)
                    atexit.register(default_pool.shutdown)
                pool = default_pool
            elif thread in pools and num_workers in pools[thread]:
                pool = pools[thread][num_workers]
            else:
                pool = ThreadPoolExecutor(num_workers)
                atexit.register(pool.shutdown)
                pools[thread][num_workers] = pool
        elif isinstance(pool, multiprocessing.pool.Pool):
            pool = MultiprocessingPoolExecutor(pool)

    results = get_async(
        pool.submit,
        pool._max_workers,
        simple_task_graph,
        result,
        cache=None,
        get_id=_thread_get_id,
        pack_exception=pack_exception,
    )

    # Cleanup pools associated to dead threads
    with pools_lock:
        active_threads = set(threading.enumerate())
        if thread is not main_thread:
            for t in list(pools):
                if t not in active_threads:
                    for p in pools.pop(t).values():
                        p.shutdown()

    for proc in mem_procs:
        proc.terminate()
    
    return simple_task_graph['output'][3].rv 
