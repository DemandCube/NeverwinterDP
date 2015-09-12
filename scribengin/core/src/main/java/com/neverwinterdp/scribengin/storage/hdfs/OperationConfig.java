package com.neverwinterdp.scribengin.storage.hdfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.StringUtil;

public class OperationConfig {
  private long                startTime;
  private long                maxLockTime;
  private String              name;
  private String              description;
  private Map<String, String> attributes = new HashMap<>();
  private String              executor ;
  
  public OperationConfig() {}
  
  public OperationConfig(String name,  long maxLockTime) {
    this.name = name;
    this.startTime = System.currentTimeMillis();
    this.maxLockTime = maxLockTime;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public OperationConfig withName(String name) {
    this.name = name;
    return this;
  }
  
  public long getStartTime() { return startTime; }
  public void setStartTime(long startTime) { this.startTime = startTime; }

  public long getMaxLockTime() { return maxLockTime; }
  public void setMaxLockTime(long maxLockTime) {
    this.maxLockTime = maxLockTime;
  }

  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public OperationConfig withDescription(String desc) {
    this.description = desc;
    return this; 
  }
  
  public String getExecutor() { return executor; }

  public void setExecutor(String executor) {
    this.executor = executor;
  }

  public OperationConfig withExecutor(Class<?> type) {
    this.executor = type.getName();
    return this; 
  }

  
  public Map<String, String> getAttributes() { return attributes; }
  public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }

  public String getAttribute(String name) {
    return attributes.get(name);
  }
  
  public OperationConfig withAttribute(String name, String value) {
    attributes.put(name, value);
    return this;
  }
  
  public <T> T attribute(String name, Class<T> type) {
    String json = attributes.get(name);
    if(json == null) return null ;
    return JSONSerializer.INSTANCE.fromString(json, type);
  }
  
  public <T> OperationConfig withAttribute(String name, T value) {
    String json = JSONSerializer.INSTANCE.toString(value);
    attributes.put(name, json);
    return this;
  }
  
  public String withSource() { return attributes.get("source") ;}
  
  public OperationConfig withSource(String value) {
    attributes.put("source", value);
    return this;
  }
  
  public String[] withSources() { 
    String values = attributes.get("sources") ;
    if(values == null) return new String[0];
    return StringUtil.toStringArray(values);
  }
  
  public OperationConfig withSources(List<String> sources) {
    attributes.put("sources", StringUtil.join(sources, ","));
    return this;
  }
  
  public String getDestination() { return attributes.get("destination") ;}
  
  public OperationConfig withDestination(String value) {
    attributes.put("destination", value);
    return this;
  }
}
