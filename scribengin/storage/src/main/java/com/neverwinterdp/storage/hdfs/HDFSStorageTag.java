package com.neverwinterdp.storage.hdfs;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.ssm.SSMTagDescriptor;

public class HDFSStorageTag {
  private TagDescription                 tagDescription = new TagDescription();
  private Map<Integer, SSMTagDescriptor> partitionTagDescriptors;
  
  public TagDescription getTagDescription() { return tagDescription; }
  public void setTagDescription(TagDescription tagDescription) { this.tagDescription = tagDescription; }
  
  public Map<Integer, SSMTagDescriptor> getPartitionTagDescriptors() { return partitionTagDescriptors; }
  public void setPartitionTagDescriptors(Map<Integer, SSMTagDescriptor> partitionTagDescriptors) {
    this.partitionTagDescriptors = partitionTagDescriptors;
  }
  
  public void add(int partitionId, SSMTagDescriptor ssmTagDescriptor) {
    if(partitionTagDescriptors == null) partitionTagDescriptors = new HashMap<>();
    partitionTagDescriptors.put(partitionId, ssmTagDescriptor);
  }
  
  static public class TagDescription {
    private String name;
    private String description;
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getDescription() { return description; }
    public void setDescription(String desc) { this.description = desc; }

  }
}
