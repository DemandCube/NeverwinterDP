package com.neverwinterdp.storage;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.StringUtil;

@SuppressWarnings("serial")
public class StorageConfig extends HashMap<String, String> {
  final static public String TYPE             = "type";
  final static public String LOCATION         = "location";
  final static public String PARTITION_STREAM = "partition-stream";
  final static public String REPLICATION      = "replication";

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
  public int  getPartitionStream() { return intAttribute(PARTITION_STREAM, 5); }
  public void setPartitionStream(int partition) { 
    attribute(PARTITION_STREAM, partition);
  }
  
  @JsonIgnore
  public int  getReplication() { return intAttribute(REPLICATION, 1); }
  public void setReplication(int replication) { 
    attribute(REPLICATION, replication);
  }
  
  public String attribute(String name) { return get(name); }
  public void attribute(String name, String value) { put(name, value); }

  public int intAttribute(String name, int defaultValue) {
    String value = get(name);
    if(value == null) return defaultValue;
    return Integer.parseInt(value);
  }
  
  public void attribute(String name, int value) { put(name, Integer.toString(value)); }

  public boolean booleanAttribute(String name, boolean defaultValue) {
    String value = get(name);
    if(value == null) return defaultValue;
    return Boolean.parseBoolean(value);
  }
  
  public void attribute(String name, boolean value) {
    put(name, Boolean.toString(value));
  }

  public String[] stringArrayAttribute(String name, String[] defaultArray) {
    String values = get(name);
    if(values != null) return StringUtil.toStringArray(values);
    return defaultArray;
  }
  
  public void attribute(String name, String[] array) {
    if(array == null || array.length == 0) return;
    put(name, StringUtil.joinStringArray(array));
  }
}