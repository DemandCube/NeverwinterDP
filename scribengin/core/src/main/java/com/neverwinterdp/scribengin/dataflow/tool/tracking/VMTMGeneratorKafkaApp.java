package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMTMGeneratorKafkaApp extends VMApp {
  private Logger logger ;
  private TrackingGeneratorService service ;
  private KafkaClient kafkaClient;
  
  @Override
  public void run() throws Exception {
    logger = getVM().getLoggerFactory().getLogger(VMTMGeneratorKafkaApp.class);
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    String reportPath = vmConfig.getProperty("tracking.report-path", "/applications/tracking-message");
    int numOfWriter = vmConfig.getPropertyAsInt("tracking.num-of-writer", 3);
    service = new TrackingGeneratorService(registry, reportPath);
    service.withNumOfChunk(vmConfig.getPropertyAsInt("tracking.num-of-chunk", 3));
    service.withChunkSize(vmConfig.getPropertyAsInt("tracking.num-of-message-per-chunk", 1000));
    service.withBreakInPeriod(vmConfig.getPropertyAsLong("tracking.break-in-period", -1));
    service.withMessageSize(vmConfig.getPropertyAsInt("tracking.message-size", 512));
    service.withLogger(logger);
    
    String kafkaZkConnects     = vmConfig.getProperty("kafka.zk-connects", "zookeeper-1:2181");
    String kafkaTopic          = vmConfig.getProperty("kafka.topic", "tracking.input");
    int    kafkaNumOfPartition = vmConfig.getPropertyAsInt("kafka.num-of-partition", 5);
    int    kafkaReplication    = vmConfig.getPropertyAsInt("kafka.replication", 1);
    
   
    kafkaClient = new KafkaClient("KafkaClient", kafkaZkConnects);
    
    for(int i = 0; i < numOfWriter; i++) {
      service.addWriter(new KafkaTrackingMessageWriter(kafkaTopic));
    }
    
    if(!kafkaClient.getKafkaTool().topicExits(kafkaTopic)) {
      kafkaClient.getKafkaTool().createTopic(kafkaTopic, kafkaReplication, kafkaNumOfPartition);
    }
    service.start();
    service.awaitForTermination(7, TimeUnit.DAYS);
  }
  
  public class KafkaTrackingMessageWriter extends TrackingMessageWriter {
    private String            topic;
    private AckKafkaWriter    kafkaWriter;
    
    KafkaTrackingMessageWriter(String topic) {
      this.topic = topic;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      String kafkaConnects = kafkaClient.getKafkaBrokerList();
      kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaConnects) ;
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      kafkaWriter.close();
    }
    
    @Override
    public void write(TrackingMessage message) throws Exception {
      String json = JSONSerializer.INSTANCE.toString(message);
      kafkaWriter.send(topic, json, 30 * 1000);
    }
  }
}