package com.neverwinterdp.vm;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class HadoopProperties extends HashMap<String, String> {
  
  public void overrideConfiguration(Configuration aconf) {
    for(Map.Entry<String, String> entry : entrySet()) {
      aconf.set(entry.getKey(), entry.getValue()) ;
    }
  }
}
