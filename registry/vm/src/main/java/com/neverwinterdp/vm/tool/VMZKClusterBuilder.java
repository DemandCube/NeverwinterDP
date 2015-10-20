package com.neverwinterdp.vm.tool;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class VMZKClusterBuilder extends VMClusterBuilder {
  private String baseDir = "./build/cluster";
  protected EmbededZKServer zookeeperServer ;
  
  public VMZKClusterBuilder() throws RegistryException {
    this(new LocalVMClient());
  }
  
  public VMZKClusterBuilder(VMClient vmClient) throws RegistryException {
    super(null, vmClient);
  }
  
  public VMZKClusterBuilder(String baseDir, VMClient vmClient) throws Exception {
    super(null, vmClient);
    this.baseDir = baseDir ;
  }
  
  @Override
  public void clean() throws Exception {
    super.clean(); 
    FileUtil.removeIfExist(baseDir, false);
  }
  
  @Override
  public void start() throws Exception {
    startZookeeper();
    super.start();
  }
  
  public void startZookeeper() throws Exception {
    zookeeperServer = new EmbededZKServer(baseDir + "/zookeeper-1", 2181);
    zookeeperServer.start();
  }
  
  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    zookeeperServer.shutdown();
  }
}
