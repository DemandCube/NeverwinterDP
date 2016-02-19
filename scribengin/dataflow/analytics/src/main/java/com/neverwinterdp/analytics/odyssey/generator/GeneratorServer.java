package com.neverwinterdp.analytics.odyssey.generator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.analytics.odyssey.Event;
import com.neverwinterdp.kafka.KafkaAdminTool;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.util.JSONSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class GeneratorServer {
  private Logger logger = LoggerFactory.getLogger(GeneratorServer.class.getSimpleName());
  
  private int    numOfThreads = 1;
  private String zkConnects;
  private String topic;
  private int    topicReplication;
  private int    topicPartition;
  private int    numOfWebEvents;

  private ExecutorService executorService;
  
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
        adminTool.createTopic(topic, topicReplication, topicPartition);
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
      for(int i = 0; i < numOfWebEvents; i++) {
        String key = "odyssey-event-" + (i + 1);
        Event event = new Event();
        event.setEventId(key);
        String message = JSONSerializer.INSTANCE.toString(event);
        producer.send(new KeyedMessage<String, String>(topic, key, message));
      }
      producer.close();
    }
  }
}