from functools import wraps
import luigi
from typing import Dict
from dask.delayed import delayed, Delayed
import inspect


def targeted(func):

    inspect.signature(func)

    sig = inspect.signature(func)
    targets = {key: val.default
               for key, val in sig.parameters.items()
               if isinstance(val.default, luigi.Target)}

    @wraps(func)
    def f(*args, **kwargs):
        # construct list of targets to pass
        func(*args, **targets, **kwargs)
        return targets

    return Targeted(delayed(f), targets=targets)


class Targeted(object):
    """
    Attributes
    -----------
    depend_targets : Dict[str, luigi.Target]
        dict of (dask key name, luigi.Target). Used to look up the target
    _delayed : dask.Delayed

    Properties
    ----------
    post processor :
         I am not sure what this thing is called, but it defines pre_task, post_task,
         etc methods which go through and edit the dask frame. It will just check for
         a key by looking at target_dict, and test it's existence by calling
         luigi.Target.exists. If the key exists, we can just replace it by the target.

    ('a', 'b', 'b')
    ('b',

    """

    def __init__(self, my_delayed: Delayed,
                 targets:Dict =None,
                 depend_targets:Dict=None):
        self._delayed = my_delayed

        if not depend_targets:
            self.depend_targets = {}
        else:
            self.depend_targets = depend_targets

        self.targets = targets

    def __getattr__(self, item):
        return getattr(self._delayed, item)

    def __call__(self, *args, **kwargs):
        new_target_dict = self.depend_targets.copy()
        new_target_dict.update(self.targets)
        for arg in list(args) + list(kwargs.values()):
            if isinstance(arg, Targeted):
                new_target_dict.update(arg.depend_targets)

        return Targeted(self._delayed(*args, **kwargs), targets=None,
                        depend_targets=new_target_dict)

