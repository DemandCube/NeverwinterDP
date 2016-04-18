import sys, subprocess, traceback
from subprocess import PIPE

class ShellProcess(object):
  def __init__(self, cmd):
    self.cmd = cmd;
  
  def run(self):
    self.process = subprocess.Popen(self.cmd, shell=True)
    self.process.communicate()
    return self
  
  def runAndGetStdoutAsText(self):
    self.process = subprocess.Popen(self.cmd, shell=True, stdout=PIPE)
    (stdout, stderr) = self.process.communicate();
    self.assertSuccess()
    return stdout.rstrip();
  
  def runAndGetStdoutAsLines(self):
    self.process = subprocess.Popen(self.cmd, shell=True, stdout=PIPE)
    (stdout, stderr) = self.process.communicate();
    self.assertSuccess()
    lines = stdout.splitlines()
    return lines
    
  def runInBackground(self):
    self.process = subprocess.Popen(self.cmd, shell=True)
    return self
  
  def assertSuccess(self):
    if self.process.returncode != 0:
      raise Exception("The command " + self.cmd + " failed!!!" ) 
    return self
  
  def exitIfFail(self):
    try:
      self.assertSuccess()
      return self
    except:
      traceback.print_exc(limit=5, file=sys.stdout)
      sys.exit(1)