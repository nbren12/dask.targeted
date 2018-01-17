from luigi import LocalTarget
from daskluigi.targeted import (Targeted, filter_tree, target_compute_dict,
                                unfuse_match)
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


def test_filter_tree():
    from operator import add, mul

    targ = lambda x: Targeted(x, [])

    dsk1 = {'a': (targ(add),
                  (add, (targ(add), -1, 1), (targ(mul), 10, 10)), 1)} # yapf: disable

    dsk2 = {
        'a': (targ(add), 'b', 1),
        'b': (add, 'c', 'd'),
        'c': (targ(add), -1, 1),
        'd': (targ(mul), 10, 10)
    }

    # count the number of targets
    tree = filter_tree(dsk2, 'a')
    assert len(tree) == 3

    tree = filter_tree(dsk1, 'a')
    assert len(tree) == 3

    tree = filter_tree(dsk2, 'b')
    assert len(tree) == 2


def test_filter_tree():
    from operator import add, mul

    targ = lambda x: Targeted(x, [])

    dsk1 = {'a': (targ(add),
                  (add, (targ(add), -1, 1), (targ(mul), 10, 10)), 1)} # yapf: disable

    dsk2 = {
        'a': (targ(add), 'b', 1),
        'b': (add, 'c', 'd'),
        'c': (targ(add), -1, 1),
        'd': (targ(mul), 10, 10)
    }

    # count the number of targets
    tree = filter_tree(dsk2, 'a')
    assert len(tree) == 3

    tree = filter_tree(dsk1, 'a')
    assert len(tree) == 3

    tree = filter_tree(dsk2, 'b')
    assert len(tree) == 2


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


def test_unfuse_match():
    """
    #TODO add assert statements to this function. the print statements make sense, but i have to figure out how to process the keys. This might require mocking tokenize.
    """

    def fun(*args):
        pass

    dsk1 = {'a': ('b-1', ('b-2', -1, 1), 1)} # yapf: disable
    ans = unfuse_match(dsk1, lambda x: x[0] == 'b')
    print(ans)

    dsk1 = {'a': ('b-1', 1, ('a-1', ('b-2', -1, ('b-3', 1, 1))))} # yapf: disable
    ans = unfuse_match(dsk1, lambda x: x[0] == 'b')
    print(ans)
