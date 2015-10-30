package com.neverwinterdp.vm.environment.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.vm.VMConfig;

abstract public class YarnManager {
  protected Logger logger = LoggerFactory.getLogger(YarnManager.class.getName());

  protected VMConfig vmConfig ;
  protected Map<String, String> yarnConfig;

  protected Configuration conf ;
  protected NMClient nmClient;
  
  public Map<String, String> getYarnConfig() { return this.yarnConfig ; }
  
  public NMClient getNMClient() { return this.nmClient ; }
  
  public void onInit(VMConfig vmConfig) throws Exception {
    this.vmConfig = vmConfig;
    this.yarnConfig = vmConfig.getHadoopProperties();
    conf = new YarnConfiguration() ;
    vmConfig.overrideHadoopConfiguration(conf);

    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();
  }

  public void onDestroy() {
    try {
      if(nmClient != null) {
        nmClient.stop();
        nmClient.close();
      }
    } catch (IOException e) {
      logger.error("Destroy error:", e);
    }
  }
  
  public VMRequest createContainerRequest(int priority, int numOfCores, int memory) {
    //Priority for worker containers - priorities are intra-application
    Priority containerPriority = Priority.newInstance(priority);
    //Resource requirements for worker containers
    Resource resource = Resource.newInstance(memory, numOfCores);
    VMRequest containerReq =  
        new VMRequest(resource, null /* hosts*/, null /*racks*/, containerPriority);
    return containerReq;
  }
  
  abstract public void allocate(VMRequest vmReq, ContainerRequestCallback cb) throws Exception ;
  
  public void startContainer(Container container, VMConfig appVMConfig) throws YarnException, IOException {
    String command = appVMConfig.buildCommand();
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    if(vmConfig.getVmResources().size() > 0) {
      appVMConfig.getVmResources().putAll(vmConfig.getVmResources());
      VMResources vmResources = new VMResources(conf, appVMConfig);
      ctx.setLocalResources(vmResources);
    }
    Map<String, String> appEnv = new HashMap<String, String>();
    boolean miniClusterEnv = vmConfig.getClusterEnvironment() == VMConfig.ClusterEnvironment.YARN_MINICLUSTER;
    setupAppClasspath(miniClusterEnv , conf, appEnv);
    ctx.setEnvironment(appEnv);
    
    StringBuilder sb = new StringBuilder();
    List<String> commands = Collections.singletonList(
        sb.append(command).
        append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").
        append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr").toString()
    );
    ctx.setCommands(commands);
    nmClient.startContainer(container, ctx);
  }
  
  protected void setupAppClasspath(boolean miniClusterEnv, Configuration conf, Map<String, String> appMasterEnv) {
    if(miniClusterEnv) {
      String cps = System.getProperty("java.class.path") ;
      String[] cp = cps.split(":") ;
      for(String selCp : cp) {
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), selCp, ":");
      }
    } else {
      StringBuilder classPathEnv = new StringBuilder();
      classPathEnv.append(Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
      classPathEnv.append("./*");

      String[] classpath = conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH
      ) ;
      for (String selClasspath : classpath) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(selClasspath.trim());
      }
      appMasterEnv.put(Environment.CLASSPATH.name(), classPathEnv.toString());
      System.err.println("CLASSPATH: " + classPathEnv);
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*", ":");
  }
  
  static public interface ContainerRequestCallback {
    public void onAllocate(YarnManager manager, VMRequest request, Container container) ;
  }
}