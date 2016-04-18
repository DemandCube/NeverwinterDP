from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from util.ShellProcess import ShellProcess

class IPlatform:
  def imageClean(self, cluster): raise NotImplementedError
  def imageBuild(self, cluster): raise NotImplementedError
  
  def serverStart(self, server): raise NotImplementedError
  def serverStop(self, server): raise NotImplementedError
  def serverKill(self, server): raise NotImplementedError
  def serverDestroy(self, server): raise NotImplementedError
  def serverDestroyAll(self): raise NotImplementedError
  def serverResolveIp(self, server): raise NotImplementedError
  
  def sshExec(self, account, server, cmd):
    remotehost = account + "@" + self.serverResolveIp(server)
    ShellProcess('ssh -o StrictHostKeyChecking=no %s \"%s\"' % (remotehost, cmd)).run(). exitIfFail()
  
  def sshExecAndGetStdout(self, account, server, cmd):
    remotehost = account + "@" + self.serverResolveIp(server)
    return ShellProcess('ssh -o StrictHostKeyChecking=no %s \"%s\"' % (remotehost, cmd)).runAndGetStdoutAsText()
  
  def scp(self, account, server, source, dest):
    remoteHost = account + "@" + self.serverResolveIp(server)
    cmd = 'scp -rC %s %s:%s' % (source, remoteHost, dest)
    ShellProcess(cmd).run(). exitIfFail()
    
  def rsync(self, account, server, localDir, dest):
    if not localDir.endswith('/'): localDir += '/'
    remotehost = account + "@" + self.serverResolveIp(server)
    ShellProcess('rsync -avz "%s" "%s:%s"' % (localDir, remotehost, dest)).run(). exitIfFail()