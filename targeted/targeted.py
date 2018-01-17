"""Interface which allows dask to easily interact with Luigi Target objects

# TODO

 - add asserts to test_unfuse_match. I might need to mock tokenize somehow to achieve this
"""
from toolz import curry
from dask.delayed import delayed


def target_or_compute(tgt, output):
    """Function which will be used to match for targets in graph
    """
    return tgt


class Targeted(object):
    def __init__(self, fun, tgts):
        "docstring"
        self.fun = fun
        self.tgts = tgts

        import uuid
        self.key = str(uuid.uuid4())

    def __call__(self, *args, **kwargs):
        new_args = tuple(self.tgts) + args
        output = delayed(self.fun)(*new_args, **kwargs)

        out = [delayed(target_or_compute)(tgt, output) for tgt in self.tgts]

        if len(out) == 1:
            out = out[0]

        return out

    def exists(self):
        return all(tgt.exists() for tgt in self.tgts)

    def compute(self):
        return not self.exists()

    def __repr__(self):
        return "Targeted(%s)" % self.fun


def prune_tree(dsk):

    # get dask tree with Targeted objects as top level keys
    dsk_flat = unfuse_match(dsk, lambda x: isinstance(x, Targeted))

    for key, val in dsk_flat.items():
        if isinstance(val[0], Targeted):
            if val[0].exists():
                dsk_flat[key] = val[0].tgts


def target_compute_dict(tgt_graph):
    compute = {}
    for targ in tgt_graph:
        compute[targ] = targ.compute()
    return compute


@curry
def targeted(tgts, fun):
    f = Targeted(fun, tgts)
    return delayed(f)
