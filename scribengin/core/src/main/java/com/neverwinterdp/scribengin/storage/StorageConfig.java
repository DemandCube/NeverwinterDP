package com.neverwinterdp.scribengin.storage;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("serial")
public class StorageConfig extends HashMap<String, String> {
  final static public String TYPE        = "type";
  final static public String LOCATION    = "location";
  final static public String PARTITION   = "partition";
  final static public String REPLICATION = "replication";
  
  public StorageConfig() { }
  
  public StorageConfig(String type) {
    setType(type);
  }
  
  public StorageConfig(String type, String location) {
    setType(type);
    setLocation(location);
  }
  
  @JsonIgnore
  public String getType() { return get(TYPE); }
  public void   setType(String type) { 
    put(TYPE, type); 
  }
 
  @JsonIgnore
  public String getLocation() { return get(LOCATION); }
  public void   setLocation(String location) { 
    put(LOCATION, location); 
  }
  
  @JsonIgnore
  public int  getPartition() { return intAttribute(PARTITION, 5); }
  public void setPartition(int partition) { 
    attribute(PARTITION, partition);
  }
  
  @JsonIgnore
  public int  getReplication() { return intAttribute(REPLICATION, 1); }
  public void setReplication(int replication) { 
    attribute(REPLICATION, replication);
  }
  
  public String attribute(String name) {
    return get(name);
  }
  
  public void attribute(String name, String value) {
    put(name, value);
  }
  
  public void attribute(String name, int value) {
    put(name, Integer.toString(value));
  }
  
  public int intAttribute(String name, int defaultValue) {
    String value = get(name);
    if(value == null) return defaultValue;
    return Integer.parseInt(value);
  }
}