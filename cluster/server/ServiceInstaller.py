import os
from sys import path
from os.path import  dirname, abspath
#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from BaseObject import BaseObject
from util.FileUtil import FileUtil

class ServiceInstaller(BaseObject):
    
  def install(self, ctx):
    if ctx.service.config.install.type == 'rsync':
      self.installRsync(ctx)
    elif ctx.service.config.install.type == 'scptgz':
      self.installScpTgz(ctx)
  
  def configure(self, ctx):
    config = ctx.service.config
    if not hasattr(config, 'configure'): 
      return
    configure = config.configure;
    if hasattr(configure, 'patch'): 
      patchConfig = configure.patch
      destination = ctx.service.config.install.destination
      #resourceDir = tempfile.gettempdir() + "/" + server.hostname + "_patch"
      resourceDir = self.getWorkingDir() + "/" + ctx.server.hostname + "_patch"
      try:
        FileUtil.cptree(patchConfig.resourceDir, resourceDir)
        self.modifyResources(ctx, patchConfig, resourceDir)
        self.info("Patch the configuration from " + patchConfig.resourceDir)
        ctx.platform.scp(ctx.account, ctx.server, resourceDir + "/*", destination)
        self.execConfigureExecute(ctx, ctx.service.config.configure)
        customServiceConfigPerServer = getattr(ctx.server.services, ctx.service.name)
        if hasattr(customServiceConfigPerServer, "configure"):
          self.execConfigureExecute(ctx, customServiceConfigPerServer.configure)
      finally:
        FileUtil.rmtree(resourceDir)
  
  def execConfigureExecute(self, ctx, configure):
    if not hasattr(configure, 'execute'): return
    batchCmd = ""
    for selCmd in configure.execute:
      selCmd = ctx.resolveContextExpression(selCmd)
      if len(batchCmd) > 0: batchCmd += " && "
      batchCmd += selCmd
    ctx.platform.sshExec(ctx.account, ctx.server, batchCmd)
  
      
  def installRsync(self, ctx):
    localDir  = ctx.service.config.install.source
    ctx.platform.rsync(ctx.account, ctx.server, localDir, ctx.service.homeDir)
    
  def installScpTgz(self, ctx):
    source = ctx.service.config.install.source
    dest = ctx.service.config.install.destination
    self.info('copy the file ' + source)
    ctx.platform.scp(ctx.account, ctx.server, source, "/tmp")

    fileName = os.path.basename(source)    
    self.info('Extract the file /tmp/' + fileName +' to ' + dest)
    cmd = 'mkdir -p ' + dest + ' && tar -zxf /tmp/' + fileName + ' --strip 1 -C ' + dest + ' && rm -f /tmp/' + fileName 
    ctx.platform.sshExec(ctx.account, ctx.server, cmd)
        
  def modifyResources(self, ctx, patchConfig, resourceDir):
    if not hasattr(patchConfig, 'resources'):
      return
    for resource in patchConfig.resources:
      self.info("Modify resource " + resource.file)
      resourcePath = resourceDir + "/" + resource.file
      resourceFile = open(resourcePath, 'r')
      resourceContent = resourceFile.read()
      resourceFile.close()
      resourceContent = self.modifyResourceProperties(ctx, resource, resourceContent)
      resourceContent = self.modifyResourceAppend(ctx, resource, resourceContent)
      FileUtil.writeText(resourcePath, resourceContent)
      
  def modifyResourceProperties(self, ctx, resource, resourceContent):
    if not hasattr(resource, "properties"): return resourceContent
    text = ""
    for line in resourceContent.split('\n'):
      for (key, value) in resource.properties.__dict__.iteritems():
        if line.startswith(key + '='):
          value = ctx.resolveContextExpression(value)
          line = key + '=' + value
          break
      text += line + '\n'
    return text
  
  def modifyResourceAppend(self, ctx, resource, resourceContent):
    if not hasattr(resource, "append"): return resourceContent
    for entry in resource.append:
      entry = ctx.resolveContextExpression(entry)
      resourceContent += "\n" + entry
    return resourceContent