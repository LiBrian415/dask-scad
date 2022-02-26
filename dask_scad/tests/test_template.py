import base64
import cloudpickle
import json
import os.path
import subprocess
import tempfile
import redis

from jinja2 import Environment, FileSystemLoader
from operator import add


def test_exec():
    def _test_exec(task, data, expected, is_error=False):
        template = get_template('exec.j2')
        with tempfile.NamedTemporaryFile(suffix='.py') as file:
            with open(file.name, 'w') as f:
                f.write(template.render(computation=cloudpickle.dumps(task)))
            res = json.loads(subprocess.run(['python3', file.name, json.dumps(data)],
                                            stdout=subprocess.PIPE).stdout.decode('utf-8'))
            assert res['res'] == expected
            assert res['error'] == is_error

    cache = {'x': 1, 'y': 2}
    identity_task = (lambda x: x, 'x')
    add_task = (add, 'x', 'y')
    add2_task = (add, (add, 'x', 5), 'y')

    _test_exec(identity_task, cache, cache['x'])
    _test_exec(add_task, cache, cache['x'] + cache['y'])
    _test_exec(add2_task, cache, cache['x'] + cache['y'] + 5)


def test_output():
    with tempfile.TemporaryDirectory() as td:
        output = {'type': 'lfs', 'meta': {'dir': td}}
        exp_cache = {'x': 1, 'y': 2, 'z': 3, ('a', 1): [1, 2, 3]}

        template = get_template('output.j2')
        with tempfile.NamedTemporaryFile(suffix='.py') as file:
            with open(file.name, 'w') as f:
                f.write(template.render(output=output))
            res = json.loads(subprocess.run(['python3', file.name, base64.b64encode(cloudpickle.dumps(exp_cache)).decode('ascii')],
                                            stdout=subprocess.PIPE).stdout.decode('utf-8'))
            assert not res['error']
            out = cloudpickle.loads(base64.b64decode(res['output']))
            cache = dict()
            for k, v in out.items():
                with open(v, 'rb') as f:
                    val = cloudpickle.loads(f.read())
                cache[k] = val
            assert len(cache) == len(exp_cache)
            assert len(set(cache.keys()).difference(set(exp_cache.keys()))) == 0
            assert len(set(exp_cache.keys()).difference(set(cache.keys()))) == 0
            for k, v in exp_cache.items():
                assert v == cache[k]


def test_output_redis():
    output = {'type': 'redis', 'meta': {'host': 'localhost', 'port': 6379}}
    exp_cache = {'x': 1, 'y': 2, 'z': 3, ('a', 1): [1, 2, 3]}

    template = get_template('output.j2')
    with tempfile.NamedTemporaryFile(suffix='.py') as file:
        with open(file.name, 'w') as f:
            f.write(template.render(output=output))
        res = json.loads(
            subprocess.run(['python3', file.name, base64.b64encode(cloudpickle.dumps(exp_cache)).decode('ascii')],
                           stdout=subprocess.PIPE).stdout.decode('utf-8'))
        assert not res['error']
        out = cloudpickle.loads(base64.b64decode(res['output']))
        cache = dict()
        r = redis.Redis(host=output['meta']['host'], port=output['meta']['port'])
        for k, v in out.items():
            val = cloudpickle.loads(r.get(v))
            cache[k] = val
            r.delete(v)
        assert len(cache) == len(exp_cache)
        assert len(set(cache.keys()).difference(set(exp_cache.keys()))) == 0
        assert len(set(exp_cache.keys()).difference(set(cache.keys()))) == 0
        for k, v in exp_cache.items():
            assert v == cache[k]


# Jinja2 stuff
TEMPLATE_PATHS = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")]


def get_environment():
    loader = FileSystemLoader(TEMPLATE_PATHS)
    environment = Environment(loader=loader)
    return environment


def get_template(name):
    return get_environment().get_template(name)


# Run tests
test_exec()
test_output()

# Redis Tests (Run redis and uncomment to run)
# test_output_redis()
