from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))

class Server(object):
  def __init__(self, serverType, config, serverId):
    hostnameBase = serverType
    if hasattr(config, 'hostname'):
      hostnameBase = config.hostname
    self.serverType = serverType
    self.services   = config.services;
    self.serverId   = serverId
    self.hostname   = hostnameBase + '-' + str(serverId)
    self.ip = None
  
  def getProcessPid(self, account, platform, serviceName, processName, process):
    try:
      customService = getattr(self.services, serviceName)
      if hasattr(customService, "processes"):
        if processName not in customService.processes:
          return None
      return platform.sshExecAndGetStdout(account, self, process['cmdFindPid'])
    except Exception as ex:
      return ""
