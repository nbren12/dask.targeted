import os
from luigi import LocalTarget
from .target import targeted, delayed
import json


def test_target(tmpdir):

    path = os.path.join(tmpdir, "a.txt")
    test_list = list(range(10))

    @targeted
    def init_file(input=LocalTarget(path)):
        with input.open("w") as f:
            json.dump(test_list, f)


    @targeted
    def sort_file(tgt: LocalTarget):
        with tgt.open("r") as f:
            out = json.load(f)
        return out

    file = init_file()
    contents = sort_file(init_file())

    assert contents.compute() == test_list
