package com.neverwinterdp.scribengin.dataflow.registry;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;

public class StreamRegistry {
  private Registry registry ;
  private String   dataflowPath ;
  
  private Node     streamsNode ;
  
  public StreamRegistry(Registry registry, String dataflowPath) throws RegistryException {
    this.registry       = registry;
    this.dataflowPath   = dataflowPath;
    this.streamsNode    = registry.get(dataflowPath + "/streams");
  }
  
  void create(Transaction transaction) throws RegistryException {
  }
  
  void initRegistry(Transaction transaction) throws RegistryException {
    transaction.create(streamsNode, null, NodeCreateMode.PERSISTENT);
  }
  
  public void create(String name, StorageConfig sConfig, List<PartitionConfig> pConfigs) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    String streamPath = streamsNode.getPath() + "/" + name;
    String inputPath  = streamPath + "/input";
    String outputPath = streamPath + "/output";
    transaction.create(streamPath, sConfig, NodeCreateMode.PERSISTENT);
    transaction.create(inputPath,  null, NodeCreateMode.PERSISTENT);
    transaction.create(outputPath, null, NodeCreateMode.PERSISTENT);
    for(int i = 0; i < pConfigs.size(); i++) {
      PartitionConfig pConfig = pConfigs.get(i);
      int pId = pConfig.getPartitionId();
      transaction.create(inputPath + "/partition-" + pId, pConfig, NodeCreateMode.PERSISTENT);
      transaction.create(outputPath + "/partition-" + pId, pConfig, NodeCreateMode.PERSISTENT);
    }
    transaction.commit();
  }
  
  public StorageConfig getStream(String name) throws RegistryException {
    Node stream = streamsNode.getChild(name);
    return stream.getDataAs(StorageConfig.class) ;
  }
  
  public List<PartitionConfig> getStreamInputPartitions(String name) throws RegistryException {
    Node streamInputNode = streamsNode.getDescendant(name + "/input");
    return streamInputNode.getChildrenAs(PartitionConfig.class) ;
  }
}
