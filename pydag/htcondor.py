class HTCondorSubmit(object):
    """HTCondor submit description

    Represents a submit description of a HTCondor job.

    Parameters
    ----------
    executable : str
        HTCondor executable file

    Attributes
    ----------
    filename : str
        HTCondor submit description file
    commands : Dict[str, object]
        Mapping of objects describing submit description file commands
    queue : int
        Number of jobs to queue

    Examples
    --------
    Create HTCondor submit description for a Python script.

    >>> import pydag
    >>> job = pydag.htcondor.HTCondorSubmit("example.submit", "example.py")
    >>> job.commands["initialdir"] = "$ENV(HOME)"
    >>> print(job)
    universe = vanilla
    executable = example.py
    initialdir = $ENV(HOME)
    queue 1
    >>> job.dump()
    >>> print(job.written_to_disk)
    True

    """
    def __init__(self, filename, executable, queue=1, **kwargs):
        self.filename = filename
        self.queue = queue

        self.commands = {
            "universe": "vanilla",
            "executable": executable,
            }

        self.commands.update(kwargs)
        self._written_to_disk = False

    def __str__(self):
        return ("\n".join("{key} = {value}".format(key=key, value=value)
                for key, value in self.commands.iteritems()) +
                "\nqueue {0}".format(self.queue))

    def dump(self):
        """Writes HTCondor submit description to `filename`.

        """
        with open(self.filename, "w") as ostream:
            ostream.write(str(self))

        self._written_to_disk = True

    @property
    def written_to_disk(self):
        """Bool: If `True` submit description was written to disk.
        """
        return self._written_to_disk
