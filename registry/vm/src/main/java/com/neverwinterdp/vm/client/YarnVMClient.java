package com.neverwinterdp.vm.client;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.vm.HadoopConfigurationUtil;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.environment.yarn.AppClient;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.service.VMService;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class YarnVMClient extends VMClient {
  private HadoopProperties hadoopProperties ;
  private Configuration conf ;
  private VMConfig.ClusterEnvironment yarnEnv = VMConfig.ClusterEnvironment.YARN_MINICLUSTER ;
  
  public YarnVMClient(Registry registry, VMConfig.ClusterEnvironment yarnEnv, HadoopProperties hadoopProps) {
    super(registry);
    this.yarnEnv = yarnEnv;
    this.hadoopProperties = hadoopProps;
    conf = new Configuration() ;
    for(Map.Entry<String, String> entry : hadoopProps.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }
  
  public YarnVMClient(Registry registry, HadoopProperties hadoopProps, Configuration conf) {
    super(registry);
    this.hadoopProperties = hadoopProps;
    this.conf = conf ;
  }
  
  public Configuration getConfiguration() { return conf; }
  
  @Override
  public void createVMMaster(String localAppHome, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setVmId(name);
    vmConfig.addRoles("vm-master") ;
    vmConfig.setSelfRegistration(true) ;
    vmConfig.setRegistryConfig(getRegistry().getRegistryConfig());
    vmConfig.setVmApplication(VMServiceApp.class.getName()) ;
    vmConfig.addProperty("implementation:" + VMServicePlugin.class.getName(), YarnVMServicePlugin.class.getName()) ;
    configureEnvironment(vmConfig);

    AppClient appClient = new AppClient(vmConfig.getHadoopProperties()) ;
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    for(Map.Entry<String, String> entry : hadoopProperties.entrySet()) {
      yarnConf.set(entry.getKey(), entry.getValue());
    }
    String remoteAppHome = VMClient.APPLICATIONS + "/vm-master";
    appClient.uploadApp(localAppHome, remoteAppHome);
    
    vmConfig.setDfsAppHome(remoteAppHome);
    vmConfig.addVMResource("dfs-app-lib",  remoteAppHome + "/libs");
    vmConfig.addVMResource("dfs-app-conf", remoteAppHome + "/conf");
    appClient.run(vmConfig, yarnConf);
  }
  
  public void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setClusterEnvironment(yarnEnv);
    vmConfig.getHadoopProperties().putAll(hadoopProperties);
  }
  
  @Override
  public void uploadApp(String localAppHome, String appHome) throws Exception {
    AppClient appClient = new AppClient(hadoopProperties) ;
    appClient.uploadApp(localAppHome, appHome);
    System.out.println("YarnVMClient: upload " + localAppHome  + " to " + appHome);
  }
  
  public FileSystem getFileSystem() throws IOException {
    Configuration conf = HadoopConfigurationUtil.toConfiguration(hadoopProperties);
    return FileSystem.get(conf);
  }
}
