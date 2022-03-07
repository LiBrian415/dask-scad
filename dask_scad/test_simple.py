from dask import delayed
from time import sleep
import scad
import dask.config

def inc(x):
    sleep(1)
    return x + 1

def add(x, y):
    sleep(1)
    return x + y


if __name__ == '__main__':
    dask.config.set({'scheduler': scad.get})
    x = delayed(inc)(1)
    y = delayed(inc)(2)
    z = delayed(add)(x, y)

    print(z)
    print(z.compute(scad_output = {'type': 'redis', 'meta': {'host':'127.0.0.1', 'port':6379}},
                    comp_config = {'kind': 'rundisagg', 'mpath': '/home/cse291/Scad/runtime/lib/memory_server'}))
