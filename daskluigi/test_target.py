import os
from luigi import LocalTarget
from .target import targeted, delayed


def test_target(tmpdir):

    path = os.path.join(tmpdir, "a.txt")

    @targeted
    def init_file(input=LocalTarget(path)):
        with input.open("w") as f:
            for i in range(10,0,-1):
                f.write("%d\n"%i)


    @targeted
    def sort_file(tgt: LocalTarget):
        with tgt.open("r") as f:
            dat = f.readlines()
        return dat


    file = init_file()
    contents = sort_file(init_file())

    # contents.compute()
    file.compute()
