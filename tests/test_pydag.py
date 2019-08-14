#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Unit tests for `pydag`

"""
import os
import tempfile
import unittest

import pkg_resources
import pydag


class TestPyDAG(unittest.TestCase):
    """Test cases for `pydag`

    """
    def runTest(self):
        test_data = pkg_resources.resource_filename(__name__, "test_data")

        job = pydag.htcondor.HTCondorSubmit("test.submit", "test.py")
        job.commands["args"] = '"$(name)"'

        with open(os.path.join(test_data, "test.submit")) as stream:
            self.assertEqual(str(job), stream.read()[:-1])

        nodes = [
            pydag.dagman.DAGManNode("A", job),
            pydag.dagman.DAGManNode("B", job)
            ]

        nodes[0].keywords["VARS"] = pydag.dagman.Macros(name="value")
        self.assertEqual(str(nodes[0].keywords["VARS"]), 'name="value"')

        nodes[0].keywords["SCRIPT PRE"] = pydag.dagman.DAGManScript("test.py")
        self.assertEqual(str(nodes[0].keywords["SCRIPT PRE"]), "test.py")

        dag = pydag.dagman.DAGManJob("test.dag", nodes)
        dag.nodes[0].keywords["SCRIPT PRE"].arguments.append("$JOB")
        dag.add_dependency(parents=("A"), children=("B"))

        with open(os.path.join(test_data, "test.dag")) as stream:
            self.assertEqual(str(dag), stream.read()[:-1])

        dag.nodes[0].submit_description = "test.submit"

        with open(os.path.join(test_data, "test.dag")) as stream:
            self.assertEqual(str(dag), stream.read()[:-1])

        with self.assertRaises(ValueError):
            dag.add_dependency(parents=("C"), children=("B"))

        with self.assertRaises(ValueError):
            dag.add_dependency(parents=("A"), children=("C"))

        with tempfile.TemporaryDirectory(prefix="pydag_") as dirname:
            job.filename = os.path.join(dirname, job.filename)
            dag.filename = os.path.join(dirname, dag.filename)

            dag.dump()
            self.assertTrue(job.written_to_disk)
            self.assertTrue(dag.written_to_disk)
            self.assertTrue(os.path.exists(job.filename))
            self.assertTrue(os.path.exists(dag.filename))


if __name__ == "__main__":
    # Execute unit tests.
    unittest.main()
