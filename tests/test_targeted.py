from luigi import LocalTarget
from targeted.targeted import (Targeted, target_compute_dict)
import mock
import pytest


def f(tgt, s):
    tgt.open("w").write(s)
    with tgt.open("w") as flocal:
        flocal.write(s)


def f2(tgt1, tgt2):
    tgt1.open("w").write("first output")
    tgt2.open("w").write("second output")


@pytest.mark.xfail()
def test_called_once(tmpdir):

    txtfile = str(tmpdir.join("hello.txt"))
    # use this mock object to keep track of the number of times f is called
    m = mock.Mock(wraps=f)
    val = Targeted(m, [LocalTarget(txtfile)])("hello")
    val.persist()
    val.compute()
    # m.assert_called_once()

    # multiple outputs
    m = mock.Mock(wraps=f2)
    tgts = [LocalTarget(str(tmpdir.join(name))) for name in ["1.txt", "2.txt"]]

    val1, val2 = Targeted(m, tgts)()
    val1.persist()
    val2.compute()
    m.assert_called_once()


def test_target_compute_dict(tmpdir):
    # open some files

    files = [tmpdir.join(str(i)) for i in range(3)]

    tgts = [LocalTarget(str(s)) for s in files]

    targ = Targeted(lambda x: x, tgts)
    tgt_graph = {targ: []}

    compute = target_compute_dict(tgt_graph)
    assert compute[targ] == True

    # touch the files
    for f in files:
        f.open("w").write("")
    compute = target_compute_dict(tgt_graph)
    assert compute[targ] == False


