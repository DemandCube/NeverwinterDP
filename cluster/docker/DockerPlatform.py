import os, shutil

from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))

from IPlatform import IPlatform
from util.ShellProcess import ShellProcess
from util.FileUtil import FileUtil

class DockerPlatform(IPlatform):
  def title(self, message):
    print "***********************************************************************"
    print 'DockerPlatform ' +message 
    print "***********************************************************************"  

  def info(self, message):
    print "[DockerPlatform] " + message 
  
  def imageBuild(self, cluster):
    self.title("Build images")
    self.info("Copy ssh resources");
    home = os.path.expanduser("~");
    currentDir = os.path.dirname(os.path.realpath(__file__));
    ubuntuDir = currentDir + "/ubuntu"
    workingDir = ubuntuDir + "/working"
    workingSSHDir = workingDir + "/ssh";
    
    FileUtil.mkdirs(workingSSHDir)
    
    FileUtil.updateWithProperties(ubuntuDir + "/Dockerfile", workingDir + "/Dockerfile", {"account.dev": 'dev'})
    shutil.copy2(home + "/.ssh/id_rsa.pub", workingSSHDir + "/authorized_keys")
    
    images = cluster.getImages()
    for name, imageConfig in images.iteritems():
      imageTag = imageConfig['repository'] + ':' + name
      imageId = self.imageFindIdByTag(imageTag)
      if not imageId:
        cmd = 'docker build -t ' + imageTag + ' ' + workingDir;
        ShellProcess(cmd).run().exitIfFail()

    shutil.rmtree(workingDir)
    
  def imageClean(self, cluster):
    self.info("Remove all the images")
    images = cluster.getImages()
    for name, imageConfig in images.iteritems():
      imageTag = imageConfig['repository'] + ':' + name
      imageId = self.imageFindIdByTag(imageTag)
      if imageId is not None:
        self.info("Remove imageConfig " + imageTag + ", imageConfig id = " + imageId)
        ShellProcess("docker rmi -f " + imageTag).run().exitIfFail()
      self.info('Remove the image ' + imageTag)
      
  def imageFindIdByTag(self, imgTag):
    listImagesCmd = "docker images --format '{{.ID}}' " + imgTag
    lines = ShellProcess(listImagesCmd).runAndGetStdoutAsLines()
    if len(lines) == 0:
      return None
    elif len(lines) == 1:
      return lines[0];
    else:
      raise Exception("Found more than 1 dockerImage with " + imgTag)
  
      
  def serverStart(self, server):
    hostname = server.hostname
    cmd = 'docker run -d -p 22  --privileged -h ' + hostname + ' --name ' + hostname + ' ubuntu:base'
    ShellProcess(cmd).run().exitIfFail()
        
  def serverStop(self, server): 
    ShellProcess("docker stop " + server.hostanme).run().exitIfFail()
  
  def serverKill(self, server, signal): 
    if signal is None:
      ShellProcess("docker kill --signal='KILL' " + server.hostanme).run().exitIfFail()
    else:
      ShellProcess("docker kill --signal='" + signal + "' " + server.hostanme).run().exitIfFail()
  
  def serverDestroy(self, server): 
    ShellProcess('docker rm --force ' + server.hostname).run()
  
  def serverDestroyAll(self): 
    ShellProcess("docker rm --force  `docker ps --no-trunc -aq`").run()
  
  def serverResolveIp(self, server): 
    if server.ip is not None: return server.ip
    cmd = "docker inspect -f '{{ .NetworkSettings.IPAddress }}' " + server.hostname
    try:
      server.ip = ShellProcess(cmd).runAndGetStdoutAsText()
      return server.ip
    except:
      return ""