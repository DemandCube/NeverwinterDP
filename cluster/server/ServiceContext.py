from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from BaseObject import BaseObject
from server.ServiceInstaller import ServiceInstaller

class ServiceContext(BaseObject):
  def __init__(self, account, cluster, platform, server, service):
    self.account  = account
    self.cluster  = cluster
    self.platform = platform
    self.server   = server
    self.service  = service
  
  def install(self): 
    self.info("Install The service " + self.service.name + ' on ' + self.server.hostname)
    ServiceInstaller().install(self)
    self.configure()
    
  def configure(self): 
    self.info("Configure The service " + self.service.name + ' on ' + self.server.hostname)
    ServiceInstaller().configure(self)
  
  def start(self): 
    processes = self.getServiceProcessesByServer()
    for process in processes:
      cmdStart = process['cmdStart']
      self.platform.sshExec(self.account, self.server, cmdStart)
      self.info("Start " + self.service.name + " service with command " + cmdStart +" on " + self.server.hostname)
    
  def stop(self):  
    processes = self.service.processes
    for (name, process) in processes.iteritems():
      print str(process)
      self.platform.sshExec(self.account, self.server, process['cmdStop'])
      self.info("Stop " + self.service.name + " service, process " + name +" on " + self.server.hostname)  
      
  def kill(self):  
    processes = self.service.processes
    for (processName, process) in processes.iteritems():
      pid = self.server.getProcessPid(self.account, self.platform, self.service.name, processName, process)
      if pid == "":
        self.info("Cannot find the " + self.service.name + " service " + "on " + self.server.hostname)
      elif pid is not None:
        cmd = "kill -9 " + pid
        self.platform.sshExec(self.account, self.server, cmd)
        self.info("Kill the " + self.service.name + " Service on " + self.server.hostname + ", pid = " + pid)
  
  def clean(self):  
    self.platform.sshExec(self.account, self.server, self.service.cmdClean)
    self.info("Clean the data for the " + self.service.name + " service on " + self.server.hostname)
  
  def resolveContextExpression(self, stringObj):
    if not isinstance(stringObj, basestring): return stringObj
    if stringObj.find('@context:getServerId()')  >= 0 :
      value = str(self.server.serverId)
      stringObj = stringObj.replace("@context:getServerId()", value)
    if stringObj.find("@context:getZookeepConnect()")  >= 0 :
      value = self.cluster.getZookeeperConnect()
      stringObj = stringObj.replace("@context:getZookeepConnect()", value)
    if stringObj.find("@context.getZookeeperClusterConfig()") >= 0:
      value = self.cluster.getZookeeperClusterConfig()
      stringObj = stringObj.replace("@context.getZookeeperClusterConfig()", value)
    if stringObj.find("@context.getHadoopMasterConfig()") >= 0:
      value = self.cluster.getHadoopMasterConfig()
      stringObj = stringObj.replace("@context.getHadoopMasterConfig()", value)
    if stringObj.find("@context.getHadoopSlaveConfig()") >= 0:
      value = self.cluster.getHadoopSlaveConfig()
      stringObj = stringObj.replace("@context.getHadoopSlaveConfig()", value)
    return stringObj
  
  def getServiceProcessesByServer(self) :
    holder = []
    availServiceProcesses = self.service.processes
    customServicePerServer = getattr(self.server.services, self.service.name)
    if hasattr(customServicePerServer, 'processes'):
      processes = customServicePerServer.processes
      for name in processes:
        holder.append(availServiceProcesses[name])    
    else:
      for (name, process) in availServiceProcesses.iteritems():
        holder.append(process)
    return holder
