.. PyDAG documentation master file

PyDAG
=====

PyDAG allows you to create `HTCondor`_ submit descriptions, `DAGMan`_ nodes,
and DAGs via Python. Any Python object that has a string representation that
corresponds to a valid HTCondor command or DAGMan keyword can be added to the
submit description. You can easily extend PyDAG by defining your own
command/keyword classes; classes for pre/post-scripts and macros are already
provided.


.. automodule:: pydag.htcondor
    :members:

.. automodule:: pydag.dagman
    :members:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _HTCondor:
    https://research.cs.wisc.edu/htcondor/

.. _DAGMan:
    https://research.cs.wisc.edu/htcondor/dagman/dagman.html
