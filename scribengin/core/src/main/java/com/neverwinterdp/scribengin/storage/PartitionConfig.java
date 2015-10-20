package com.neverwinterdp.scribengin.storage;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("serial")
public class PartitionConfig extends HashMap<String, String> {
  final static public String PARTITION_ID = "partitionId";
  final static public String LOCATION     = "location";
  
  public PartitionConfig() {
  }
  
  public PartitionConfig(int id) {
    setPartitionId(id);
  }
  
  public PartitionConfig(int id, String location) {
    setPartitionId(id);
    setLocation(location);
  }
  
  public PartitionConfig(Map<String, String> props) {
    putAll(props);
  }
  
  @JsonIgnore
  public int  getPartitionId() { return intAttribute(PARTITION_ID, 0); }
  public void setPartitionId(int id) { attribute(PARTITION_ID, id); }
  
  @JsonIgnore
  public String getLocation() { return get(LOCATION); }
  public void   setLocation(String location) { 
    put(LOCATION, location); 
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
