package com.neverwinterdp.util.log;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.StringUtil;

public class LoggerConfiguration {
  
  private Map<String, Logger>       loggers       = new LinkedHashMap<String, Logger>();
  private Map<String, FileAppender> fileAppenders = new LinkedHashMap<String, FileAppender>();
  private Map<String, ESAppender>   esAppenders   = new LinkedHashMap<String, ESAppender>();

  public Logger createLogger(String name, String level, String ... appender) {
    Logger logger = new Logger(name, level, appender);
    loggers.put(name,  logger);
    return logger;
  }
  
  public FileAppender createFileAppender(String name, String filePath) {
    FileAppender appender = new FileAppender(name, filePath, 5, 10);
    fileAppenders.put(name, appender) ;
    return appender;
  }
  
  public ESAppender createESAppender(String name, String connects, String bufferDir, String indexName) {
    ESAppender appender = new ESAppender(name, connects, bufferDir, indexName);
    esAppenders.put(name, appender) ;
    return appender;
  }
  
  @JsonIgnore
  public Map<String, String> getLog4jConfiguration() {
    Map<String, String> props = new LinkedHashMap<String, String>();
    for (FileAppender sel : fileAppenders.values()) {
      String prefix = "log4j.appender." + sel.getName();
      props.put(prefix,             "org.apache.log4j.RollingFileAppender");
      props.put(prefix + ".layout", "org.apache.log4j.PatternLayout");
      props.put(prefix + ".layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
      props.put(prefix + ".File", sel.getFilePath());
      props.put(prefix + ".MaxFileSize", sel.getMaxSizeInMb() + "MB");
      props.put(prefix + ".MaxBackupIndex", sel.getMaxBackup() + "");
    }
    
    for (ESAppender sel : esAppenders.values()) {
      String prefix = "log4j.appender." + sel.getName();
      props.put(prefix, "com.neverwinterdp.es.log4j.ElasticSearchAppender");
      props.put(prefix + ".layout", "org.apache.log4j.PatternLayout");
      props.put(prefix + ".layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

      props.put(prefix + ".connects", sel.getConnects());
      props.put(prefix + ".indexName", sel.getIndexName());
      props.put(prefix + ".queueBufferDir", sel.getBufferDir());
    }
    
    for(Logger sel : loggers.values()) {
      props.put("log4j." + sel.getName(), sel.getLevel() + "," + StringUtil.joinStringArray(sel.getAppender(), ","));
    }
    return props ;
  }
  
  static public class Logger {
    private String   name;
    private String   level;
    private String[] appender;
    
    public Logger() {} 
    
    public Logger(String name, String level, String ... appender) {
      this.name = name;
      this.level = level ;
      this.appender = appender ;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String[] getAppender() { return appender; }
    public void setAppender(String[] appender) { this.appender = appender; }
  }
  
  static public class ESAppender {
    private String name;
    private String connects;
    private String bufferDir;
    private String indexName;
    
    public ESAppender() {} 
    
    public ESAppender(String name, String connects, String bufferDir, String indexName) {
      this.name = name;
      this.connects = connects;
      this.bufferDir = bufferDir;
      this.indexName = indexName;
    }
    
    
    public String getName() { return name; }
    public ESAppender setName(String name) { 
      this.name = name;
      return this;
    }
    
    public String getConnects() { return connects; }
    public ESAppender setConnects(String connects) {
      this.connects = connects;
      return this;
    }
    
    public String getBufferDir() { return bufferDir; }
    public ESAppender setBufferDir(String bufferDir) {
      this.bufferDir = bufferDir;
      return this;
    }
    
    public String getIndexName() { return indexName; }
    public ESAppender setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }
  }
  
  static public class FileAppender {
    private String name ;
    private String filePath; 
    private int    maxSizeInMb;
    private int    maxBackup;
    
    public FileAppender() {} 
    
    public FileAppender(String name, String filePath, int maxSizeInMb, int maxBackup) {
      this.name = name;
      this.filePath = filePath;
      this.maxSizeInMb = maxSizeInMb;
      this.maxBackup = maxBackup;
    }
    
    public String getName() { return name; }
    public FileAppender setName(String name) { 
      this.name = name; 
      return this ;
    }

    public String getFilePath() { return filePath; }
    public FileAppender setFilePath(String filePath) { 
      this.filePath = filePath; 
      return this;
    }
    
    public int getMaxSizeInMb() { return maxSizeInMb; }
    public FileAppender setMaxSizeInMb(int maxSizeInMb) { 
      this.maxSizeInMb = maxSizeInMb; 
      return this;
    }
    
    public int getMaxBackup() { return maxBackup; }
    public FileAppender setMaxBackup(int maxBackup) { 
      this.maxBackup = maxBackup; 
      return this ;
    }
  }
}