===============================
daskluigi
===============================


.. image:: https://img.shields.io/travis/nbren12/daskluigi.svg
        :target: https://travis-ci.org/nbren12/daskluigi


Using dask delayed with luigi targets


There are some difficulties with this project, and I have not yet resolved on a suitable syntax. The basic requirments are 

1. The inputs and outputs which are luigi targets must be known when dasks constructs its graph.

Ideally, we could use a function decorator syntax like dask.delayed for this, but luigi's object oriented syntax is probably the best possible way to specify inputs and outputs.

One potential solution is to inspect the arguments of target functions for the names `in` and `out`.
