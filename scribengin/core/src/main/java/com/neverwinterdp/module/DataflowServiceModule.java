package com.neverwinterdp.module;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.inject.name.Names;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.vm.VMConfig;

@ModuleConfig(name = "DataflowServiceModule", autoInstall = false, autostart = false) 
public class DataflowServiceModule extends ServiceModule {
  final static public String NAME = "DataflowServiceModule" ;
  
  @Override
  protected void configure(Map<String, String> props) {  
    Names.bindProperties(binder(), props) ;
    Configuration conf = new Configuration();
    VMConfig.overrideHadoopConfiguration(props, conf);
    try {
      FileSystem fs = FileSystem.get(conf);
      bindInstance(FileSystem.class, fs);
      
      String kafkaZkConnects = props.get("kafka.zk.connects");
      KafkaClient kafkaClient = new KafkaClient("KafkaClient", kafkaZkConnects);
      bindInstance(KafkaClient.class, kafkaClient);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}