package com.neverwinterdp.scribengin.dataflow.sample;

import java.util.HashMap;

import com.neverwinterdp.util.text.StringUtil;

public class WebEventAttributes extends HashMap<String, String> {
  
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
