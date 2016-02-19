package com.neverwinterdp.scribengin.dataflow.sample;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.kafka.KafkaAdminTool;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.util.JSONSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class WebEventGenerator {
  static String[] browserFamilies = { "Chrome", "Safari", "Firefox", "IE", "Crawler"};
  static String[] osFamilies = { "Mac OS", "Linux", "Window", "Window Phone", "IOS", "Android"};
  
  private AtomicLong idTracker = new AtomicLong();
  private Random random = new Random(System.nanoTime());
  
  public void runWebEventGenerator(String zkConnects, String topic, int topicReplication, int topicPartition, int numOfWebEvents) throws Exception {
    KafkaAdminTool adminTool = new KafkaAdminTool("admin", zkConnects);
    if(!adminTool.topicExits(topic)) {
      adminTool.createTopic(topic, topicReplication, topicPartition);
    }
    
    KafkaClient kafkaTool = new KafkaClient("KafkaTool", zkConnects);
    String kafkaBrokerConnects = kafkaTool.getKafkaBrokerList();
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaBrokerConnects);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("request.required.acks", "1");
    props.put("retry.backoff.ms", "1000");
    ProducerConfig producerConfig = new ProducerConfig(props);
    WebEventGenerator webEventGenerator = new WebEventGenerator();
    Producer<String, String> producer = new Producer<String, String>(producerConfig);
    for(int i = 0; i < numOfWebEvents; i++) {
      WebEvent webEvent = webEventGenerator.nextRandomWebEvent();
      String message    = JSONSerializer.INSTANCE.toString(webEvent);
      producer.send(new KeyedMessage<String, String>(topic, webEvent.getWebEventId(), message));
    }
    producer.close();
  }
  
  public WebEvent nextRandomWebEvent() {
    WebEvent webEvent = new WebEvent() ;
    webEvent.setName("user.click");
    webEvent.setWebEventId("web-event-id-" + idTracker.incrementAndGet());
    webEvent.setTimestamp(new Date());
    webEvent.getAttributes().attribute("browser.family", nextRandomBrowserFamily());
    webEvent.getAttributes().attribute("os.family",      nextRandomOSFamily());
    return webEvent;
  }
  
  String nextRandomBrowserFamily() {
    return browserFamilies[random.nextInt(browserFamilies.length)];
  }
  
  String nextRandomOSFamily() {
    return osFamilies[random.nextInt(osFamilies.length)];
  }
}
