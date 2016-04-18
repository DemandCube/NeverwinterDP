package com.neverwinterdp.analytics.odyssey.generator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.analytics.odyssey.ActionEvent;
import com.neverwinterdp.kafka.KafkaAdminTool;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.util.JSONSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class OdysseyEventGeneratorServer {
  private Logger logger = LoggerFactory.getLogger(OdysseyEventGeneratorServer.class.getSimpleName());
  
  @Parameter(names = "--num-of-workers", description = "The number of the threads")
  private int    numOfThreads = 1;
  
  @Parameter(names = "--zk-connects", description = "The number of the threads")
  private String zkConnects = "127.0.0.1:2181";
  
  @Parameter(names = "--topic", description = "The kafka topic name")
  private String topic = "odyssey.input";
  
  @Parameter(names = "--replication", description = "The number of replication")
  private int    replication = 1;
  
  @Parameter(names = "--partition", description = "The number of partition")
  private int    partition   = 5;
  
  @Parameter(names = "--num-of-events", description = "The number of events")
  private int    numOfWebEvents   = 10000;

  private ExecutorService executorService;
  
  public OdysseyEventGeneratorServer() { }
  
  public OdysseyEventGeneratorServer(String[] args) { 
    new JCommander(this, args);
  }
  
  public void start() throws Exception {
    executorService = Executors.newFixedThreadPool(numOfThreads);
    for(int i = 0; i < numOfThreads; i++) {
      executorService.submit(new GeneratorWorker());
    }
    executorService.shutdown();
  }

  public void shutdown() throws Exception {
    executorService.shutdownNow();
  }
  
  public class GeneratorWorker implements Runnable {
    public void run() {
      try {
        doRun();
      } catch (Exception ex) {
        logger.error("Error: ", ex);
      }
    }
    
    public void doRun() throws Exception {
      KafkaAdminTool adminTool = new KafkaAdminTool("admin", zkConnects);
      if(!adminTool.topicExits(topic)) {
        adminTool.createTopic(topic, replication, partition);
      }
      
      KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnects);
      String kafkaBrokerConnects = kafkaTool.getKafkaBrokerList();
      Properties props = new Properties();
      props.put("metadata.broker.list", kafkaBrokerConnects);
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
      props.put("request.required.acks", "1");
      props.put("retry.backoff.ms", "1000");
      ProducerConfig producerConfig = new ProducerConfig(props);
      Producer<String, String> producer = new Producer<String, String>(producerConfig);
      ActionEventGenerator eventGenerator = new ActionEventGenerator();
      for(int i = 0; i < numOfWebEvents; i++) {
        ActionEvent event = eventGenerator.nextEvent();
        String message = JSONSerializer.INSTANCE.toString(event);
        producer.send(new KeyedMessage<String, String>(topic, event.getEventId(), message));
      }
      producer.close();
    }
  }
}