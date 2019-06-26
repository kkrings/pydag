# -*- coding: utf-8 -*-


class DAGManJob(object):
    r"""DAGMan job

    Specify a DAG's input.

    Parameters
    ----------
    filename : str
        DAGMan input file
    nodes : list(DAGManNode)
        Sequence of DAGMan nodes

    Examples
    --------
    Create a DAG with one node; a macro ``inputfile`` is defined; the
    macro's value is passed as an argument to the Python script that is
    specified as the executable in the node's HTCondor submit
    description.

    >>> import pydag
    >>> job = pydag.htcondor.HTCondorSubmit("example.submit", "example.py")
    >>> job.commands["arguments"] = "$(inputfile)"
    >>> node = pydag.dagman.DAGManNode("example", job)
    >>> node["VARS"] = pydag.dagman.Macros(inputfile="example.txt")
    >>> dag = pydag.dagman.DAGManJob("example.dag", [node])
    >>> print(dag)
    JOB example example.submit
    VARS example inputfile="example.txt"
    >>> dag.dump()
    >>> print(dag.written_to_disk)
    True

    """
    def __init__(self, filename, nodes):
        self.filename = filename
        self.nodes = nodes
        self._dependencies = {}
        self._written_to_disk = False

    def __str__(self):
        job = [str(node) for node in self.nodes]

        job.extend("PARENT {0} CHILD {1}".format(parents, children)
                   for parents, children in self.dependencies)

        return "\n".join(job)

    def dump(self):
        r"""Write DAGMan input file to `filename`.

        """
        for node in self.nodes:
            if not isinstance(node.submit_description, str):
                node.submit_description.dump()

        with open(self.filename, "w") as ostream:
            ostream.write(str(self))

        self._written_to_disk = True

    def add_dependency(self, parents, children):
        r"""Add dependency within the DAG.

        Nodes are parents and/or children within the DAG. A parent node
        must be completed successfully before any of its children may be
        started. A child node may only be started once all its parents
        have successfully completed.

        Parameters
        ----------
        parents : tuple(str)
            Parent node names
        children : tuple(str)
            Children node names

        Raises
        ------
        ValueError
            If `parents` or `children` contains an unknown DAGMan node.

        """
        nodes = [node.name for node in self.nodes]

        if not all(node in nodes for node in parents):
            raise ValueError("Unknown DAGMan node.")

        if not all(node in nodes for node in children):
            raise ValueError("Unknown DAGMan node.")

        self._dependencies[parents] = children

    @property
    def dependencies(self):
        r"""str: Dependencies within the DAG
        """
        return [(" ".join(parents), " ".join(children))
                for parents, children in self._dependencies.items()]

    @property
    def written_to_disk(self):
        r"""bool: If `True` DAGMan input file was written to disk.
        """
        return self._written_to_disk


class DAGManNode(object):
    r"""DAGMan node

    Specify a DAG's node.

    Parameters
    ----------
    name : str
        Uniquely identifies nodes within the DAGMan input file and in
        output messages
    submit_description : HTCondorSubmit, str
        HTCondor submit description; either a string specifying a submit
        description file or an `HTCondorSubmit` instance
    \*\*kwargs
        Node keywords

    Attributes
    ----------
    keywords : dict(str, object)
        Mapping of node keywords to objects representing values

    """
    def __init__(self, name, submit_description, **kwargs):
        self.name = name
        self.submit_description = submit_description
        self.keywords = dict(kwargs)

    def __str__(self):
        if isinstance(self.submit_description, str):
            filename = self.submit_description
        else:
            filename = self.submit_description.filename

        node = ["JOB {job} {name}".format(job=self.name, name=filename)]

        node.extend("{key} {job} {value}".format(
            key=key, job=self.name, value=value)
            for key, value in self.keywords.items())

        return "\n".join(node)


class DAGManScript(object):
    r"""Pre-processing or post-processing

    Specify a shell script/batch file to be executed either before a
    job within a node is submitted or after a job within a node
    completes its execution.

    Parameters
    ----------
    executable : str
        Specify the shell script/batch file to be executed.
    \*args
        Script/batch file arguments

    Attributes
    ----------
    arguments : list(object)
        Sequence of objects representing script/batch file arguments

    """
    def __init__(self, executable, *args):
        self.executable = executable
        self.arguments = list(args)

    def __str__(self):
        if len(self.arguments) > 0:
            arguments = " ".join("{0}".format(arg) for arg in self.arguments)
            return "{name} {args}".format(name=self.executable, args=arguments)
        else:
            return self.executable


class Macros(dict):
    r"""Define macros for DAGMan node.

    Derived from `dict`; only `__str__` is re-implemented to meet the
    DAGMan macro syntax.

    """
    def __str__(self):
        return " ".join('{key}="{val}"'.format(key=key, val=val)
                        for key, val in self.items())
