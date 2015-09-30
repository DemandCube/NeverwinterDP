package com.neverwinterdp.scribengin.storage;

import java.util.Map;

public class PartitionDescriptor extends StorageDescriptor {

  public PartitionDescriptor() {
  }
  
  public PartitionDescriptor(String type, int id, String location) {
    setType(type);
    setId(id);
    setLocation(location);
  }
  
  public PartitionDescriptor(Map<String, String> props) {
    putAll(props);
  }
  
  public int  getId() { return intAttribute("id"); }
  public void setId(int id) { attribute("id", id); }
  
}
