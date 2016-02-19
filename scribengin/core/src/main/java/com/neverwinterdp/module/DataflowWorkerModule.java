package com.neverwinterdp.module;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import com.google.inject.name.Names;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.vm.VMConfig;



@ModuleConfig(name = "DataflowWorkerModule", autoInstall = false, autostart = false) 
public class DataflowWorkerModule extends ServiceModule {
  final static public String NAME = "DataflowWorkerModule" ;
  
  @Override
  protected void configure(Map<String, String> props) {  
    Names.bindProperties(binder(), props) ;
    try {
      Configuration conf = new Configuration();
      VMConfig.overrideHadoopConfiguration(props, conf);
      
      FileSystem fs = FileSystem.get(conf);
      if(fs instanceof LocalFileSystem) {
        fs = ((LocalFileSystem)fs).getRaw();
      }
      bindInstance(FileSystem.class, fs);
      
      String kafkaZkConnects = props.get("kafka.zk.connects");
      KafkaTool kafkaTool = new KafkaTool("KafkaTool", kafkaZkConnects);
      bindInstance(KafkaTool.class, kafkaTool);
      
      S3Client s3Client = new S3Client();
      bindInstance(S3Client.class, s3Client);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}