package com.neverwinterdp.vm;

import java.util.HashMap;

@SuppressWarnings("serial")
public class VMAppConfig extends HashMap<String, String> {
  public VMAppConfig() { }
  
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
  
  public long longAttribute(String name, long defaultVal) { 
    String val = get(name); 
    return val == null ? defaultVal : Long.parseLong(val);
  }
  
  public void attribute(String name, long value) {
    put(name, Long.toString(value));
  }
  
  public boolean booleanAttribute(String name, boolean defaultVal) { 
    String val = get(name); 
    return val == null ? defaultVal : Boolean.parseBoolean(val);
  }
  
  public void attribute(String name, boolean value) {
    put (name, Boolean.toString(value));
  }
}