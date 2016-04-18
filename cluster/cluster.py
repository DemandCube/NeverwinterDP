#! /bin/sh
""":"
exec python $0 ${1+"$@"}
"""
import click

from sys import path
from os.path import  dirname, abspath

#Make sure the package is computed from cluster directory
path.insert(0, dirname(dirname(abspath(__file__))))
from ClusterContext import ClusterContext
from util.ShellProcess import ShellProcess

platformType   = None
configFile     = None
clusterContext = None

def checkInitClusterContext():
  global clusterConfig, clusterContext, platformType
  if clusterContext is not None:
    return
  if configFile is not None:
    clusterContext = ClusterContext(configFile, platformType)

@click.group(invoke_without_command=True, chain=True)
@click.pass_context
@click.option('--platform', help="The platform type")
@click.option('--config', help="The cluster configuration file")
def cli(ctx, platform, config):
  global platformType, configFile
  platformType = platform
  if platformType is None: 
    platformType = 'docker'
  configFile = config
  if ctx.invoked_subcommand is not None:
    ctx.invoked_subcommand
  else:
    click.echo('I was invoked without subcommand')

@cli.command()
def status():
  global clusterContext
  checkInitClusterContext()
  clusterContext.status()

@cli.command()
def destroy():
  global clusterContext
  checkInitClusterContext()
  clusterContext.serverDestroyAll()
  
@cli.command('image')
@click.option('--clean', is_flag=True, help="Remove all the images")
@click.option('--build', is_flag=True, help="Build all the images")
def image(clean, build):
  global clusterContext
  checkInitClusterContext()
  if clean: clusterContext.imageClean()
  if build: clusterContext.imageBuild()

@cli.command('server')
@click.option('--destroy', is_flag=True, help="destroy the servers and release the resources")
@click.option('--kill', is_flag=True, help="Kill the running server or poweroff")
@click.option('--stop', is_flag=True, help="Stop the running server or poweroff")
@click.option('--start', is_flag=True, help="Launch the server")
@click.option('--hosts-map', is_flag=True, help="Generate /etc/hosts")
def server(destroy, kill, stop, start, hosts_map):
  global clusterContext
  checkInitClusterContext()
  if destroy:   clusterContext.server('destroy')
  if kill:      clusterContext.server('kill')
  if stop:      clusterContext.server('stop')
  if start:     clusterContext.server('start')
  if hosts_map: clusterContext.updateHostsFile()

@cli.command(help='Service command')
@click.option('--stop', is_flag=True, help="Stop the services")
@click.option('--clean', is_flag=True, help="Kill the services")
@click.option('--install',  is_flag=True, help="Install the services")
@click.option('--configure', is_flag=True, help="Configure the services")
@click.option('--kill', is_flag=True, help="Kill the services")
@click.option('--start', is_flag=True, help="Start the services")
def service(stop, kill, clean, install, configure, start):
  global clusterContext
  checkInitClusterContext()
  if stop:      clusterContext.service('stop')
  if kill:      clusterContext.service('kill')
  if clean:     clusterContext.service('clean')
  if install:   clusterContext.service('install')
  if configure: clusterContext.service('configure')
  if start:     clusterContext.service('start')
  
@cli.command('docker:setup', help='Setup docker environment, route ip...')
def dockerSetup():
  hostIp = ShellProcess('docker-machine ip default').runAndGetStdoutAsText()
  ShellProcess('sudo route -n add 172.17.0.0/16 ' + hostIp).run().exitIfFail()
  
if __name__ == '__main__':
  cli(obj={})
