import luigi
from dask import delayed
from daskluigi.simple import string_targeted, targeted, xarray_read, xarray_write, targeted
import mock


def test_string_targeted(tmpdir):
    def fun():
        return "hello world"

    m = mock.Mock(wraps=fun)
    fun = delayed(m)

    path = str(tmpdir.join("a_file.txt"))
    tgt = luigi.LocalTarget(path)

    a = fun()
    b = string_targeted(tgt)(a)
    assert b.compute() == "hello world"

    # test that second call does not call the function again
    b = string_targeted(tgt)(a)
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
    air_t = targeted(tgt, reader=xarray_read, writer=xarray_write)(a)
    air_t.compute()

    assert tgt.exists()

    # second compute
    air_t = targeted(tgt, reader=xarray_read, writer=xarray_write)(a)
    air_t.compute()

    m.assert_called_once()
