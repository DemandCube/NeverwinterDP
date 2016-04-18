package com.neverwinterdp.vm;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfigurationUtil {
  static public Configuration toConfiguration(Map<String, String> props) {
    Configuration conf = new Configuration() ;
    overrideConfiguration(props, conf) ;
    return conf ;
  }
  
  static public void overrideConfiguration(Map<String, String> props, Configuration aconf) {
    for(Map.Entry<String, String> entry : props.entrySet()) {
      aconf.set(entry.getKey(), entry.getValue()) ;
    }
  }
}
