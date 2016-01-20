package com.neverwinterdp.scribengin.dataflow.tracking;

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
    TrackingConfig trackingConfig = vmConfig.getVMAppConfigAs(TrackingConfig.class);
   
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    runGenerator(registry, trackingConfig);
  }
  
  public void runGenerator(Registry registry, TrackingConfig trackingConfig) {
    try {
      service = new TrackingGeneratorService(registry, trackingConfig);
      service.withLogger(logger);

      kafkaClient = new KafkaClient("KafkaClient", trackingConfig.getKafkaZKConnects());

      for(int i = 0; i < trackingConfig.getGeneratorNumOfWriter(); i++) {
        service.addWriter(new KafkaTrackingMessageWriter(trackingConfig.getKafkaInputTopic()));
      }

      if(!kafkaClient.getKafkaTool().topicExits(trackingConfig.getKafkaInputTopic())) {
        kafkaClient.
        getKafkaTool().
        createTopic(trackingConfig.getKafkaInputTopic(), trackingConfig.getKafkaNumOfReplication(), trackingConfig.getKafkaNumOfPartition());
      }
      service.start();
      service.awaitForTermination(7, TimeUnit.DAYS);
    } catch(Throwable t) {
      error("Error: ", t);
    }
  }
  
  void info(String message) {
    if(logger != null) logger.info(message);
    else System.out.println("VMTMValidatorKafkaApp: " + message);
  }
  
  void error(String message, Throwable error) {
    if(logger != null) {
      logger.error(message, error);
    } else {
      System.out.println("VMTMValidatorKafkaApp: " + message);
      error.printStackTrace();
    }
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