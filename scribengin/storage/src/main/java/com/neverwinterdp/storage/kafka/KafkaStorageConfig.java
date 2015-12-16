package com.neverwinterdp.storage.kafka;

import com.neverwinterdp.storage.StorageConfig;

public class KafkaStorageConfig extends StorageConfig {
  final static public String NAME        = "name";
  final static public String TOPIC       = "topic";
  final static public String ZK_CONNECT  = "zk.connect";
  
  public KafkaStorageConfig() { 
    setType("kafka");
  }
  
  public KafkaStorageConfig(StorageConfig config) { 
    putAll(config);
  }
  
  public KafkaStorageConfig(String name, String zkConnect, String topic) { 
    setName(name);
    setZKConnect(zkConnect);
    setTopic(topic);
  }
  
  public String getName() { return attribute(NAME); }
  public void   setName(String name) { attribute(NAME, name); }
 
  public String getZKConnect() { return attribute(ZK_CONNECT); }
  public void   setZKConnect(String zkConnect) { attribute(ZK_CONNECT, zkConnect); }
 
  
  public String getTopic() { return attribute(TOPIC); }
  public void   setTopic(String topic) { attribute(TOPIC, topic); }
  
}
