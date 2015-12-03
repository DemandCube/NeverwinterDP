package com.neverwinterdp.storage.hdfs;

import java.util.List;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.storage.StorageConfig;

public class HDFSStorageRegistry {
  private Registry      registry;
  private StorageConfig storageConfig;
  private String        registryPath;
  
  private Node          rootNode;
  private Node          partitionsNode;
  
  public HDFSStorageRegistry(Registry registry, StorageConfig sconfig) throws RegistryException {
    this.registry     = registry;
    this.registryPath = sconfig.attribute(HDFSStorage.REGISTRY_PATH);;
    
    rootNode       = registry.get(registryPath);
    partitionsNode = rootNode.getChild("partitions");
    
    if(exists()) {
      this.storageConfig = rootNode.getDataAs(StorageConfig.class) ;
    } else {
      this.storageConfig = sconfig;
    }
  }

  public Registry getRegistry() { return registry ; }
  
  public String getRegistryPath() { return registryPath; }

  public StorageConfig getStorageConfig() { return this.storageConfig; }
  
  public boolean exists() throws RegistryException {
    return rootNode.exists();
  }

  public void create() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    create(transaction);
    transaction.commit();
  }
  
  public void create(Transaction transaction) throws RegistryException {
    if(rootNode.exists()) {
      throw new RegistryException(ErrorCode.NodeExists, "The registry is already initialized");
    }
    
    rootNode.getParentNode().createIfNotExists();;
    
    transaction.create(rootNode, storageConfig, NodeCreateMode.PERSISTENT);
    transaction.create(partitionsNode, null, NodeCreateMode.PERSISTENT);
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      String partitionRegistryPath = partitionsNode.getPath() + "/partition-" + i;
      SSMRegistry ssmRegistry = new SSMRegistry(registry, partitionRegistryPath);
      ssmRegistry.initRegistry(transaction);
    }
  }
  
  public void drop() throws RegistryException {
    Transaction transaction = registry.getTransaction();
    transaction.rdelete(registryPath);
    transaction.commit();
  }
  
  public List<String> getPartitions() throws RegistryException {
    return partitionsNode.getChildren();
  }
  
  public SSMRegistry getPartitionRegistry(int partitionId) throws RegistryException {
    String partitionRegPath = partitionsNode.getPath() + "/partition-" + partitionId ;
    SSMRegistry ssmRegistry = new SSMRegistry(registry, partitionRegPath);
    return ssmRegistry;
  }
  
  public SSMRegistry getPartitionRegistry(String partitionId) throws RegistryException {
    String partitionRegPath = partitionsNode.getPath() + "/" + partitionId ;
    SSMRegistry ssmRegistry = new SSMRegistry(registry, partitionRegPath);
    return ssmRegistry;
  }
}
