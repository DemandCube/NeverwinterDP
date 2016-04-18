from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))

class BaseObject:
  def getWorkingDir(self): 
    return "working"
  
  def title(self, message):
    name = self.__class__.__name__
    print "***********************************************************************"
    print '[' + name + '] ' + message 
    print "***********************************************************************"  

  def info(self, message):
    name = self.__class__.__name__
    print '[' + name + '] ' + message 