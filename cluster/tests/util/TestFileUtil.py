import unittest

from sys import path
from os.path import  dirname, abspath, realpath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from util.FileUtil import FileUtil

class TestFileUtil(unittest.TestCase):
  def testUpdateWithProperties(self):
    workingDir = dirname(realpath(__file__)) + "/working";
    FileUtil.mkdirs(workingDir)
    properties = { "account.dev": "dev" }
    source      = workingDir + "/test-properties.txt"
    FileUtil.writeText(source, "account.dev=${account.dev}")
    destination = workingDir + "/test-properties-update.txt"
    FileUtil.updateWithProperties(source, destination, properties)
    content = FileUtil.readAsText(destination)
    self.assertEqual("account.dev=dev", content)
    FileUtil.rmtree(workingDir)