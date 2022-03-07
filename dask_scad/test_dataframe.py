from dask import delayed
from time import sleep
import scad
import dask.config
import pandas as pd
import dask.dataframe as dd

def inc(x):
    sleep(1)
    return x + 1

def add(x, y):
    sleep(1)
    return pd.concat([x, y])


if __name__ == '__main__':
    dask.config.set({'scheduler': scad.get})
    d = {"a": [1,2,3,4], "b": [2,4,6,8]}
    df = pd.DataFrame(d)

    x = delayed(inc)(df)
    y = delayed(inc)(df)
    z = delayed(add)(x, y)

    print(z)
    print(z.compute(scad_output = {'type': 'redis', 'meta': {'host':'127.0.0.1', 'port':6379}},
                    comp_config = {'kind': 'rundisagg', 'mpath': '/home/cse291/Scad/runtime/lib/memory_server'}))
