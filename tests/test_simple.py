import luigi
import mock
import pytest
from dask import delayed

from targeted.simple import (TargetedCallback, read_or_compute,
                             string_targeted, targeted, xarray_read,
                             xarray_write)


def test_string_targeted(tmpdir):
    def fun():
        return "hello world"

    m = mock.Mock(wraps=fun)
    fun = delayed(m)

    path = str(tmpdir.join("a_file.txt"))
    tgt = luigi.LocalTarget(path)

    with TargetedCallback():

        a = fun()
        b = string_targeted(tgt, a)
        assert b.compute() == "hello world"
        assert b.compute() == "hello world"

    m.assert_called_once()


def test_xr_targeted(tmpdir):
    import xarray as xr

    def inc(x):
        return x + 1

    m = mock.Mock(wraps=inc)
    fun = delayed(m)

    air = xr.tutorial.load_dataset("air_temperature")
    a = fun(air)

    path = str(tmpdir.join("test.nc"))

    tgt = luigi.LocalTarget(path)
    air_t = targeted(tgt, a, reader=xarray_read, writer=xarray_write)

    with TargetedCallback():
        air_t.compute()
        assert tgt.exists()

        air_t.compute()

    m.assert_called_once()


def targeted_callback_generator():
    tgt = mock.Mock(name="target")

    fun = mock.Mock(name="function")
    fun.return_value = "hello world"

    reader = mock.Mock(name="reader")
    writer = mock.Mock(name="writer")

    # dask
    dsk = {'a': (read_or_compute, reader, writer, tgt, 'b'), 'b': (fun, )}
    yield fun, tgt, dsk

    dsk = {
        'a': (lambda x: x, (read_or_compute, reader, writer, tgt, 'b')),
        'b': (fun, )
    }

    yield fun, tgt, dsk


@pytest.mark.parametrize("fun,tgt,dsk", targeted_callback_generator())
def test_targeted_callback(fun, tgt, dsk):
    from dask import get

    with TargetedCallback():
        tgt.exists.return_value = False
        get(dsk, 'a')
        fun.assert_called_once()

        fun.reset_mock()
        tgt.exists.return_value = True
        get(dsk, 'a')
        fun.assert_not_called()
