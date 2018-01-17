"""
Example::

    from luigi import Target

    a = ... # dask object with name 'a'

    tgt = LocalTarget("some/file/path")
    b = targeted(a, tgt, reader=..., writer=...)

    with TargetedCallback():
        b.compute()
"""
from functools import partial

import luigi
import xarray as xr
from dask.callbacks import Callback
from dask.delayed import delayed

from .graph_manipulations import unfuse_match


class TargetedCallback(Callback):
    def __init__(self, *args, **kwargs):
        super(TargetedCallback, self).__init__(*args, **kwargs)
        self._targeted_keys = {}

    def _start(self, dsk):

        new_dsk = unfuse_match(dsk, lambda x: x == read_or_compute)

        for key, val in new_dsk.items():
            if val[0] == read_or_compute:
                reader, writer, tgt, x = val[1:]
                if tgt.exists():
                    new_dsk[key] = (reader, tgt)
                    self._targeted_keys[key] = ("reading from", tgt)
                else:
                    new_dsk[key] = (write_and_return, writer, tgt, x)
                    self._targeted_keys[key] = ("writing to", tgt)

        dsk.update(new_dsk)

    def _pretask(self, key, dsk, state):
        if key in self._targeted_keys:
            action, tgt = self._targeted_keys[key]
            print("%s %s" % (action, tgt))


def read_or_compute():
    """This is a placeholder which is searched for by TargetedCallback.

    if (read_or_compute, reader, writer, tgt, x) is found in the dask graph it will be replaced by either
    (reader, tgt)
    if tgt.exists() == True
    Otherwise it will be replaced by
    (write_and_return, writer, 'b')
    """
    pass


def write_and_return(writer, target, x):
    writer(target, x)
    return x


def identity(x):
    return x


def targeted(target, obj, reader=identity, writer=identity):
    return delayed(read_or_compute)(reader, writer, target, obj)


def string_read(obj):
    return obj.open("r").read()


def string_write(target, obj):
    with target.open("w") as f:
        f.write(obj)


def xarray_read(f):
    return xr.open_dataset(f.path)


def xarray_write(f, obj):
    return obj.to_netcdf(f.path)


def xr_local_targeted(path):
    return targeted(
        luigi.LocalTarget(path), reader=xarray_read, writer=xarray_write)


string_targeted = partial(targeted, reader=string_read, writer=string_write)
