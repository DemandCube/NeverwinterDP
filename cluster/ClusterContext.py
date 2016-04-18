import os, json, re, operator

from sys import path
from os.path import  dirname, abspath
from argparse import Namespace
from tabulate import tabulate
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from BaseObject import BaseObject
from docker.DockerPlatform import DockerPlatform
from server.ServerSet import ServerSet
from server.Service import Service
from server.ServiceContext import ServiceContext
from util.PropertyUtil import PropertyUtil

class ClusterContext(BaseObject):
  def __init__(self, configFile, platformType):
    baseDir = dirname(os.path.realpath(__file__));
    if not configFile.startswith('/'):
      configFile = baseDir + "/" + configFile;
    configData = open(configFile, 'r').read()
    config = json.loads(configData, object_hook=lambda d: Namespace(**d))
    setattr(config.properties, 'base_dir', baseDir)
    self.config = config
    PropertyUtil.traverseAndResolvePropertyExpression(config, config.properties)
    
    self.images = {}
    for name in config.images.__dict__.keys():
      imgObj = getattr(config.images, name)
      image = { "name": name, "repository": imgObj.repository }
      self.images[name] = image

    self.serverSets = {}
    for name in config.servers.__dict__.keys():
      serverConfig = getattr(config.servers, name)
      self.serverSets[name] = ServerSet(name, serverConfig)
    
    self.services = {}
    for name in config.services.__dict__.keys():
      serviceConfig = getattr(config.services, name)
      self.services[name] = Service.createService(name, serviceConfig)
    self.platform = DockerPlatform()
  
  def getImages(self): return self.images
  
  def getServerSet(self, name): 
    if self.serverSets[name] is not None:
      return self.serverSets[name]
    return None
  
  def getServerSets(self): return self.serverSets
  
  def getServices(self): return self.serverSets

  def getZookeeperConnect(self):
    serverSet = self.getServerSet('zookeeper')
    if serverSet is None: return "localhost:2181"
    zkConnect = ""
    first = True
    for (serverName, server) in serverSet.getServers().iteritems():
      if not first: zkConnect += ","
      zkConnect += server.hostname + ':2181'
      first = False
    return zkConnect
  
  def getZookeeperClusterConfig(self):
    serverSet = self.getServerSet('zookeeper')
    if serverSet is None: return ""
    config = "\n\n#Zookeeper cluster configuration\n"
    for (serverName, server) in serverSet.getServers().iteritems():
      config += "server." + str(server.serverId) + "=" + server.hostname + ":2888:3888\n"
    return config

  def getHadoopMasterConfig(self):
    serverSet = self.getServerSet('hadoop_master')
    if serverSet is None: return ""
    config = ""
    for (serverName, server) in serverSet.getServers().iteritems():
      config += server.hostname + "\n"
    return config

  def getHadoopSlaveConfig(self):
    serverSet = self.getServerSet('hadoop_worker')
    if serverSet is None: return ""
    config = ""
    for (serverName, server) in serverSet.getServers().iteritems():
      config += server.hostname + "\n"
    return config
  
  def status(self):
    self.title("List the servers and the service status")
    self.serverResolveIp()
    account = self.config.properties.account_dev
    headers = ["Host", "IP", "Service", "Process", "PID"]
    cells  = []
    for serverSet in sorted(self.serverSets.itervalues(), key=operator.attrgetter("priority")):
      servers = serverSet.getServers()
      for (severName, server) in servers.iteritems():
        cells.append([server.hostname, server.ip, "", "", ""])
        for serviceName in server.services.__dict__.keys():
          service = self.services[serviceName]
          cells.append(["", "", serviceName, "", ""])
          processes = service.processes
          for (processName, process) in processes.iteritems():
            pid = server.getProcessPid(account, self.platform, serviceName, processName, process)
            if pid is not None:
              cells.append(["", "", "", processName, pid])
          
    print tabulate(cells, headers)
  
  def imageBuild(self): return self.platform.imageBuild(self)
  
  def imageClean(self): return self.platform.imageClean(self)
  
  def serverResolveIp(self): 
    for (serverSetName, serverSet) in self.serverSets.iteritems():
      servers = serverSet.getServers()
      for (serverName, server) in servers.iteritems():
        if server.ip is None: self.platform.serverResolveIp(server)
          
  def server(self, option): 
    self.title("Server " + option)
    #sortedServerSet = sorted(self.serverSets.items(), key=lambda x : MyDict[x]) 
    #for (serverSetName, serverSet) in self.serverSets.iteritems():
    for serverSet in sorted(self.serverSets.itervalues(), key=operator.attrgetter("priority")):
      servers = serverSet.getServers()
      for (serverName, server) in servers.iteritems():
        if option == 'start': self.platform.serverStart(server)
        if option == 'stop': self.platform.serverStop(server)
        if option == 'kill': self.platform.serverKill(server)
        if option == 'destroy': self.platform.serverDestroy(server)
        
    if option == 'start':
      hostIpMap = self.generateHostsMap()
      for (serverSetName, serverSet) in self.serverSets.iteritems():
        servers = serverSet.getServers()
        for (serverName, server) in servers.iteritems():
          cmd = "echo '%s' >> /etc/hosts" % (hostIpMap)
          self.platform.sshExec("root", server, cmd)
  
  def serverDestroyAll(self): 
    self.title("Destroy All The Server In The Cluster ")
    self.platform.serverDestroyAll()
    
  def generateHostsMap(self):
    BEGIN = "##CLUSTER START##"
    END   = "##CLUSTER END##"
    hostIpMap  = BEGIN + '\n'
    self.serverResolveIp()
    for (name, serverSet) in self.serverSets.iteritems():
      servers = serverSet.getServers()
      for (hostname, server) in servers.iteritems():
        hostIpMap += server.ip + '    ' + hostname + '\n'
    hostIpMap  += END
    return hostIpMap
  
  
  def updateHostsFile(self):
    self.info("Generate /etc/hosts content")
    hostString  = self.generateHostsMap()
    hostFile = open('/etc/hosts', 'r')
    hostFileContent = hostFile.read()
    hostFile.close();
    
    regex = re.compile(r"##CLUSTER START##.*##CLUSTER END##", re.DOTALL)
    hostFileContent = regex.sub('', hostFileContent)
    hostFileContent += '\n' + hostString;
    print hostFileContent
  
  def service(self, option):
    self.title("Service " + option)
    account = self.config.properties.account_dev
    for serverSet in sorted(self.serverSets.itervalues(), key=operator.attrgetter("priority")):
      servers = serverSet.getServers()
      for (serverName, server) in servers.iteritems():
        for serviceName in server.services.__dict__.keys():
          service = self.services[serviceName]
          context = ServiceContext(account, self, self.platform, server, service)
          if option   == 'install':   context.install()
          elif option == 'configure': context.configure()
          elif option == 'start':     context.start()
          elif option == 'stop':      context.stop()
          elif option == 'kill':      context.kill()
          elif option == 'clean':     context.clean()
