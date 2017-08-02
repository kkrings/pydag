import os


class SBatchSubmit(object):
    r"""SLURM sbatch submit description

    Represents a sbatch submit description of a single task SLURM job.

    Parameters
    ----------
    filename : str
        SLURM sbatch submit description file
    executable : str
        Path to executable
    arguments : str, optional
        Arguments that will be passed to `executable`
    \*\*kwargs
        SLURM sbatch options

    Attributes
    ----------
    options : dict(str, object)
        Mapping of SLURM sbatch options to objects representing values
    transfer_executable : bool
        Transfer `executable` to node (default: `False`).
    transfer_input_files : list(str)
        Sequence of input files that are copied to the node before
        executing `executable`
    transfer_output_files : list(str)
        Sequence of output files that are moved after
        executing `executable`

    """
    def __init__(self, filename, executable, arguments='', **kwargs):
        self.filename = filename
        self.executable = executable
        self.arguments = arguments
        self.options = kwargs
        self.options['ntasks'] = 1
        self.transfer_executable = False
        self.transfer_input_files = []
        self.transfer_output_files = []

    def __str__(self):
        return ("\n".join(("#SBATCH --{key}={val}".format(key=key, val=val)
                for key, val in self.options.iteritems())))

    def dump(self):
        """Write SLURM sbatch submit description to `filename`.

        """
        transfer_input_files = " ".join(
            "'{}'".format(os.path.abspath(f))
            for f in self.transfer_input_files)

        transfer_output_files = " ".join(
            "'{}'".format(os.path.abspath(f))
            for f in self.transfer_output_files)

        description = _skeleton.format(
            slurm_options=str(self),
            executable="'{}'".format(os.path.abspath(self.executable)),
            arguments=self.arguments,
            transfer_executable=str(self.transfer_executable).lower(),
            transfer_input_files=transfer_input_files,
            transfer_output_files=transfer_output_files)

        with open(self.filename, 'w') as ostream:
            ostream.write(description)


_skeleton = """#!/usr/bin/env bash

{slurm_options}

echo "Working on node `hostname`."

echo 'Create working directory:'
workdir=$SLURM_JOB_NAME
mkdir -v $workdir
cd $workdir

executable={executable}
transfer_executable={transfer_executable}

if [ "$transfer_executable" = "true" ]
then
    echo 'Transfer executable to node:'
    cp -v $executable .
    executable=$workdir/`basename $executable`
fi

inputfiles=({transfer_input_files})

echo 'Transfer input files to node:'
for inputfile in ${{inputfiles[*]}}
do
    cp -v $inputfile .
done

echo 'Execute...'
$executable {arguments}

outputfiles=({transfer_output_files})

echo 'Transfer output files:'
for outputfile in ${{outputfiles[*]}}
do
    mv -v `basename $outputfile` $outputfile
done

echo 'Remove working directory:'
cd ..
rm -rv $workdir
"""
