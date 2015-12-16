package com.neverwinterdp.scribengin.tool;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class EmbededVMClusterBuilder extends VMClusterBuilder {
  private String baseDir = "./build/cluster";
  protected KafkaCluster kafkaCluster;
  
  public EmbededVMClusterBuilder() throws RegistryException {
    this(new LocalVMClient());
  }
  
  public EmbededVMClusterBuilder(VMClient vmClient) throws RegistryException {
    super(null, vmClient);
  }
  
  public EmbededVMClusterBuilder(String baseDir, VMClient vmClient) {
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
    startKafkaCluster() ;
    super.start();
  }
  
  public void startKafkaCluster() throws Exception {
    h1("Start kafka cluster");
    kafkaCluster = new KafkaCluster(baseDir, 1, 1);
    kafkaCluster.setNumOfPartition(5);
    kafkaCluster.start();
    Thread.sleep(1000);
  }
  
  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    kafkaCluster.shutdown();
  }
}
