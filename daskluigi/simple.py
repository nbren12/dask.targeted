"""
Example::

    from luigi import Target

    a = ... # dask object with name 'a'

    tgt = LocalTarget("some/file/path")
    b = targeted(a, tgt, reader=..., writer=...)

    with TargetedCallback():
        b.compute()
"""
import luigi
import xarray as xr
from dask.callbacks import Callback
from dask.delayed import delayed
from toolz import curry


class TargetedCallback(Callback):
    def _start(self, dsk):
        new_dsk = {}
        for key, val in dsk.items():
            if val[0] == read_or_compute:
                reader, writer, tgt, x = val[1:]
                if tgt.exists():
                    new_dsk[key] = (reader, tgt)
                else:
                    new_dsk[key] = (write_and_return, writer, x)

        dsk.update(new_dsk)


def read_or_compute():
    pass


def write_and_return(writer, x):
    writer(x)
    return x


def identity(x):
    return x


@curry
def targeted(target, obj, reader=identity, writer=identity):
    """Return targeted object
    """
    if target.exists():
        return delayed(reader)(target)

    def _write_and_return(obj):
        writer(target, obj)
        return obj

    return delayed(_write_and_return)(obj)


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


string_targeted = targeted(reader=string_read, writer=string_write)
