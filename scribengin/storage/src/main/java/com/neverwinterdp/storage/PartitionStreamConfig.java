package com.neverwinterdp.storage;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("serial")
public class PartitionStreamConfig extends HashMap<String, String> {
  final static public String PARTITION           = "partition";
  final static public String PARTITION_STREAM_ID = "partitionStreamId";
  final static public String LOCATION            = "location";
  
  public PartitionStreamConfig() {
  }
  
  public PartitionStreamConfig(int id) {
    setPartitionStreamId(id);
  }
  
  public PartitionStreamConfig(int id, String location) {
    setPartitionStreamId(id);
    setLocation(location);
  }
  
  public PartitionStreamConfig(Map<String, String> props) {
    putAll(props);
  }
  
  @JsonIgnore
  public String getPartition() { return get(PARTITION); }
  public void   setPartition(String partition) { 
    put(LOCATION, partition); 
  }
  
  @JsonIgnore
  public int  getPartitionStreamId() { return intAttribute(PARTITION_STREAM_ID, 0); }
  public void setPartitionStreamId(int id) { attribute(PARTITION_STREAM_ID, id); }
  
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
