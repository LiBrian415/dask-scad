import requests
import argparse
import json
import subprocess
import random
import os
import yaml

from glob import glob

dir_path = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.join(dir_path, '../lib')

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

def gen_obj(meta, source):
    # process memory objects
    memory_source = "def main(_, action):\n    t = action.get_transport('memory', 'rdma_server')\n    t.serve()"

    kind = 'python:3'
    annotations = {}
    annotations['exec'] = 'python'
    if meta['type'] == 'memory':
        source = memory_source

    name = meta.get('name')

    # process limits
    limits = {
        "concurrency": 1,
        "logs": 10,
        "resources": {
            "cpu": meta.get('limits', {}).get('cpu', 1.0),
            "mem": meta.get('limits', {}).get('mem', '128 MB'),
            "storage": meta.get('limits', {}).get('storage', '512 MB')
        },
        "timeout": 60000
    }

    # change corunning dict to list
    corunning = [k for k in meta.get('corunning', {})]

    # import
    for filename in meta.get('import', []):
        fullname = os.path.join(dir_path, "../lib/", filename)
        print('import', fullname)
        with open(fullname) as f:
            source = f.read() + '\n' + source

    obj = {
        'annotations': [{'key': k, 'value': v} for k, v in annotations.items()],
        'exec': {
            'kind': kind,
            'code': source,
            'binary': True,
            'main': 'main'
        },
        'limits': limits,
        'name': name,
        'parameters': [],
        'publish': False,
        'porusParams': {
            'relationships': {
                'corunning': corunning,
                'dependents': meta.get('dependents', []),
                'parents': meta.get('parents', [])
            },
            'runtimeType': meta['type'],
            'withMerged': meta.get('withMerged', []),
        },
        'version': "0.0.1"
    }

    if 'parallelism' in meta:
        obj['porusParams']['parallelism'] = meta['parallelism']

    return obj

# l : meta, l
def gen_json(funcname, l):
    func = {
        'name': funcname,
        'objects': [],
        "publish": True,
    }
    for _, meta, source in l:
        func['objects'].append(gen_obj(meta, source))
    return {'functions': [func], "publish": True}


def generate(path, targetFile):
    if path.endswith('/'):
        path = path[:-1]
    dirname = os.path.basename(path)
    objects = glob(os.path.join(path, "*.o.*"))
    config = gen_json(dirname, [filemeta(f) for f in objects])

    print('generating config file:', targetFile, 'from folder', dirname)

    with open(targetFile, 'w') as outfile:
        json.dump(config, outfile, indent = 2)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Generate disaggregated json from folder')
    parser.add_argument('-o', '--output', type=str, default='action.json',
                        help='Json file with disaggregated openwhisk type')
    parser.add_argument('path', type=str,
                        help='path of the object files')
    args = parser.parse_args()

    generate(args.path, args.output)
