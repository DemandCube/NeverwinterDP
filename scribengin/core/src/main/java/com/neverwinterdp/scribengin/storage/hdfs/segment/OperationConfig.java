package com.neverwinterdp.scribengin.storage.hdfs.segment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.util.text.StringUtil;

public class OperationConfig {
  private String              name;
  private String              description;
  private String              operationClass;
  private Map<String, String> attributes = new HashMap<>();
  
  public OperationConfig() {}
  
  public OperationConfig(String name, Class<?> opClass) {
    this.name = name;
    this.operationClass = opClass.getName();
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public OperationConfig withName(String name) {
    this.name = name;
    return this;
  }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public OperationConfig withDescription(String desc) {
    this.description = desc;
    return this; 
  }
  
  public String getOperationClass() { return operationClass; }
  public void setOperationClass(String operationClass) { this.operationClass = operationClass; }
  
  public OperationConfig withOperationClass(Class<?> type) {
    this.operationClass = type.getName();
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
