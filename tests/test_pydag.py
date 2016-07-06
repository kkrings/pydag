import os
import unittest

import pydag


class TestPyDAG(unittest.TestCase):
    def runTest(self):
        job = pydag.htcondor.HTCondorSubmit("test.submit", "test.py")
        job.commands["args"] = '"$(name)"'

        with open("test_data/test.submit") as stream:
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

        with open("test_data/test.dag") as stream:
            self.assertEqual(str(dag), stream.read()[:-1])

        dag.nodes[0].submit_description = "test.submit"

        with open("test_data/test.dag") as stream:
            self.assertEqual(str(dag), stream.read()[:-1])

        with self.assertRaises(ValueError):
            dag.add_dependency(parents=("C"), children=("B"))

        with self.assertRaises(ValueError):
            dag.add_dependency(parents=("A"), children=("C"))

        dag.dump()
        self.assertTrue(job.written_to_disk)
        self.assertTrue(dag.written_to_disk)
        self.assertTrue(os.path.exists(job.filename))
        self.assertTrue(os.path.exists(dag.filename))

        os.remove(job.filename)
        os.remove(dag.filename)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPyDAG)
    unittest.TextTestRunner(verbosity=2).run(suite)
