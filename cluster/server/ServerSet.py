from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from BaseObject import BaseObject
from server.Server import Server

class ServerSet(BaseObject):
  def __init__(self, name, config):
    self.name     = name;
    self.priority = config.priority;
    self.config   = config;
    self.servers  = None
    
  def getServers(self):
    if self.servers is None:
      self.servers = {};
      for i in range(self.config.instance):
        server = Server(self.name, self.config, i + 1)
        self.servers[server.hostname] = server
    return self.servers;
