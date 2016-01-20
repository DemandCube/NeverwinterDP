package com.neverwinterdp.storage.hdfs;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SSMTagDescriptor;
import com.neverwinterdp.storage.StorageConfig;

public class HDFSStorageRegistry {
  private Registry          registry;
  private HDFSStorageConfig storageConfig;
  private String            registryPath;

  private Node rootNode;
  private Node partitionsNode;
  private Node tagsNode;
  
  public HDFSStorageRegistry(Registry registry, StorageConfig sconfig) throws RegistryException {
    this(registry, new HDFSStorageConfig(sconfig));
  }

  public HDFSStorageRegistry(Registry registry, HDFSStorageConfig sconfig) throws RegistryException {
    this.registry      = registry;
    this.storageConfig = sconfig;
    this.registryPath  = sconfig.getRegistryPath();;
    
    rootNode       = registry.get(registryPath);
    partitionsNode = rootNode.getChild("partitions");
    tagsNode       = rootNode.getChild("tags");
    
    if(exists()) {
      storageConfig = rootNode.getDataAs(HDFSStorageConfig.class) ;
    }
  }
  
  public Registry getRegistry() { return registry ; }
  
  public String getRegistryPath() { return registryPath; }

  public StorageConfig getStorageConfig() { return storageConfig; }
  
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
    transaction.create(tagsNode,       null, NodeCreateMode.PERSISTENT);
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
    String partitionRegPath = partitionsNode.getPath() + "/partition-" + partitionId ;
    SSMRegistry ssmRegistry = new SSMRegistry(registry, partitionRegPath);
    return ssmRegistry;
  }
  
  public void doManagement() throws RegistryException {
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      ssmRegistry.doManagement();
    }
  }
  
  public List<String> getTags() throws RegistryException {
    return tagsNode.getChildren();
  }
  
  public HDFSStorageTag getTagByName(String name) throws RegistryException {
    HDFSStorageTag tag = new HDFSStorageTag();
    tag.setTagDescription(tagsNode.getChild(name).getDataAs(HDFSStorageTag.TagDescription.class));
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      SSMTagDescriptor partitionTag = ssmRegistry.getTagByName(name);
      tag.add(i, partitionTag);
    }
    return tag;
  }
  
  public HDFSStorageTag findTagByDateTime(String name, String desc, Date datetime) throws RegistryException {
    HDFSStorageTag tag = new HDFSStorageTag();
    tag.getTagDescription().setName(name);
    tag.getTagDescription().setDescription(desc);
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      SSMTagDescriptor partitionTag = ssmRegistry.findTagByTime(datetime);
      partitionTag.setName(name);
      partitionTag.setDescription(desc);
      tag.add(i, partitionTag);
    }
    return tag;
  }
  
  public void createTag(HDFSStorageTag tag) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    Map<Integer, SSMTagDescriptor> ssmTags = tag.getPartitionTagDescriptors();
    for(Map.Entry<Integer, SSMTagDescriptor>  entry : ssmTags.entrySet()) {
      int partitionId = entry.getKey();
      SSMTagDescriptor partitionTag = entry.getValue();
      SSMRegistry ssmRegistry = getPartitionRegistry(partitionId);
      ssmRegistry.createTag(transaction, partitionTag);
    }
    transaction.createChild(tagsNode, tag.getTagDescription().getName(), tag.getTagDescription(), NodeCreateMode.PERSISTENT);
    transaction.commit();
  }
  
  public HDFSStorageTag getTagByRecordLastPosition(String name, String desc) throws RegistryException {
    HDFSStorageTag tag = new HDFSStorageTag(name, desc);
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      SSMTagDescriptor partitionTag = ssmRegistry.findTagByRecordLastPosition();
      partitionTag.setName(name);
      partitionTag.setDescription(desc);
      tag.add(i, partitionTag);
    }
    return tag;
  }
  
  public HDFSStorageTag findTagByPosition(String name, String desc, long pos) throws RegistryException {
    HDFSStorageTag tag = new HDFSStorageTag(name, desc);
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      SSMTagDescriptor partitionTag = ssmRegistry.findTagByRecordPosition(pos);
      partitionTag.setName(name);
      partitionTag.setDescription(desc);
      tag.add(i, partitionTag);
    }
    return tag;
  }
  
  public void deleteTag(String name) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    int numOfPartitions = storageConfig.getPartitionStream();
    for(int i = 0; i < numOfPartitions; i++) {
      SSMRegistry ssmRegistry = getPartitionRegistry(i);
      ssmRegistry.deleteTag(transaction, name);
    }
    transaction.deleteChild(tagsNode, name);
    transaction.commit();
  }
}