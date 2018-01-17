===============================
dask.targeted
===============================

Using dask with luigi targets.

There are many tools out there which are used for automatic scientific workflows
by having the users specify some sort of dependency graph between files. These
tools are especially popular in bioinformatics, but are broadly applicable in
many disciplines. Some popular tools for this include make, luigi, Snakemake,
nextflow, and many others. I like these tools a lot, but it can be tedious
breaking up a lengthy python script into pieces and manually specifing the
dependency graph by hand. This effort seems especially redudant because dask
already builds a computational graph behind the scenes.

The basic idea of this package is to use dask to build a graph between
persistent objects using an interface similar to ``dask.delayed``. Luigi already
has wide-array of targets that represent different types of persistent data
stores (e.g. database, file system, etc), that we can leverage to make this work
easier.

This package provides the following syntax for building a graph between persistent objects::


  from dask.delayed import delayed
  from targeted.simple import TargetedCallback, targeted
  import luigi

  @delayed
  def fun():
      print("Calling fun")
      return "hello world"


  def string_read(obj):
      return obj.open("r").read()


  def string_write(target, obj):
      with target.open("w") as f:
          f.write(obj)



  tgt = luigi.LocalTarget("hello.txt")

  with TargetedCallback():

      a = fun()
      b = targeted(tgt, reader=string_read, writer=string_write)(a)

      print(b.compute()) # calls fun and saves to hello.txt
      print(b.compute()) # loads text from hello.txt
