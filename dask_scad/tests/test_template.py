import cloudpickle
import json
import os.path
import subprocess
import tempfile

from jinja2 import Environment, FileSystemLoader
from operator import add


def test_exec():
    def _test_exec(task, data, expected, is_error=False):
        template = get_template('exec.j2')
        with tempfile.NamedTemporaryFile(suffix='.py') as file:
            print(file.name)
            with open(file.name, 'w') as f:
                f.write(template.render(computation=cloudpickle.dumps(task)))
            res = json.loads(subprocess.run(['python3', file.name, json.dumps(data)],
                                            stdout=subprocess.PIPE).stdout.decode('utf-8'))
            assert res['res'] == expected
            assert res['error'] == is_error

    cache = {'x': 1, 'y': 2}
    identity_task = (lambda x: x, 'x')
    add_task = (add, 'x', 'y')

    _test_exec(identity_task, cache, cache['x'])
    _test_exec(add_task, cache, cache['x'] + cache['y'])


# Jinja2 stuff
TEMPLATE_PATHS = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")]


def get_environment():
    loader = FileSystemLoader(TEMPLATE_PATHS)
    environment = Environment(loader=loader)
    return environment


def get_template(name):
    return get_environment().get_template(name)


# Run test
test_exec()
