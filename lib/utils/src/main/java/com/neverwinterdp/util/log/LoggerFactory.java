package com.neverwinterdp.util.log;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;

public class LoggerFactory {
  private String prefix ;
  
  public LoggerFactory() {
    this.prefix = "";
  }
  
  public LoggerFactory(String prefix) {
    this.prefix = prefix ;
  }
  
  public Logger getLogger(String name) {
    return org.slf4j.LoggerFactory.getLogger(prefix + name) ;
  }
  
  public Logger getLogger(Class<?> type) {
    return org.slf4j.LoggerFactory.getLogger(prefix + type.getSimpleName()) ;
  }
  
  static public void log4jConfigure(URL confUrl) throws Exception {
    PropertyConfigurator.configure(confUrl);
  }
  
  static public void log4jConfigure(InputStream is) throws Exception {
    PropertyConfigurator.configure(is);
  }
  
  static public void log4jConfigure(Properties props) throws Exception {
    PropertyConfigurator.configure(props);
  }
  
  static public void log4jConfigure(Map<String, String> props) throws Exception {
    Properties properties = new Properties() ;
    properties.putAll(props);
    PropertyConfigurator.configure(properties);
  }
}
