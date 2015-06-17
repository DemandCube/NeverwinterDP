package com.neverwinterdp.kafka.consumer ;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class KafkaMessageConsumerConnector  {
  private ExecutorService executorService ;
  private Properties connectorProps;
  private  ConsumerConnector consumer;
  private Map<String, TopicMessageConsumers> topicConsumers = new ConcurrentHashMap<String, TopicMessageConsumers>();
  
  public KafkaMessageConsumerConnector(String group, String zkConnectUrls) {
    connectorProps = new Properties();
    connectorProps.put("group.id", group);
    connectorProps.put("zookeeper.connect", zkConnectUrls);
    connectorProps.put("zookeeper.session.timeout.ms", "3000");
    connectorProps.put("zookeeper.sync.time.ms", "200");
    connectorProps.put("auto.commit.interval.ms", "1000");
    connectorProps.put("auto.commit.enable", "true");
    connectorProps.put("auto.offset.reset", "smallest");
  }
  
  public KafkaMessageConsumerConnector withConsumerTimeoutMs(long timeout) {
    connectorProps.put("consumer.timeout.ms", Long.toString(timeout));
    return this;
  }
  
  public KafkaMessageConsumerConnector connect() {
    ConsumerConfig config = new ConsumerConfig(connectorProps);
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    return this;
  }

  public void consume(String topic, MessageConsumerHandler handler, int numOfThreads) throws IOException {
    consume(new String[] {topic}, handler, numOfThreads) ;
  }
  
  synchronized public void consume(String[] topic, MessageConsumerHandler handler, int numOfThreads) throws IOException {
    executorService = Executors.newFixedThreadPool(topic.length * numOfThreads);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    for(int i = 0; i < topic.length; i++) {
      topicCountMap.put(topic[i], numOfThreads);
    }
    
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    for(int k = 0; k < topic.length; k++) {
      List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic[k]);
      TopicMessageConsumer[] consumer = new TopicMessageConsumer[streams.size()] ;
      for (int i = 0; i < streams.size(); i++) {
        KafkaStream<byte[], byte[]> stream = streams.get(i) ;
        consumer[i] = new TopicMessageConsumer(topic[k], handler, stream) ; 
        executorService.submit(consumer[i]);
      }
      topicConsumers.put(topic[k], new TopicMessageConsumers(topic[k], consumer)) ;
    }
    executorService.shutdown();
  }
  
  synchronized public void remove(String topic) {
    TopicMessageConsumers topicConsumer = topicConsumers.get(topic) ;
    if(topicConsumer != null) {
      topicConsumers.remove(topic) ;
      topicConsumer.terminate(); 
    }
  }
  
  public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    executorService.awaitTermination(timeout, unit) ;
  }
  
  public void close() {
    executorService.shutdownNow() ;
    consumer.shutdown();
  }
  
  static public class TopicMessageConsumer implements Runnable {
    private String topic ;
    private MessageConsumerHandler handler ;
    private KafkaStream<byte[], byte[]> stream;
    private boolean terminate ;
    
    public TopicMessageConsumer(String topic, MessageConsumerHandler handler, KafkaStream<byte[], byte[]> stream) {
      this.topic = topic ;
      this.handler = handler ;
      this.stream = stream;
    }

    public void setTerminate() { this.terminate = true ; }
    
    public void run() {
      ConsumerIterator<byte[], byte[]> it = stream.iterator();
      try {
        while (true) {
          if(terminate) return ;
          boolean hasNext = it.hasNext();
          if(!hasNext) break ;

          MessageAndMetadata<byte[], byte[]> data = it.next() ;
          handler.onMessage(topic, data.key(), data.message()) ;
        }
      } catch(kafka.consumer.ConsumerTimeoutException ex) {
      }
    }
  }
  
  static public class TopicMessageConsumers {
    private String topic ;
    private TopicMessageConsumer[] consumers ;
    
    public TopicMessageConsumers(String topic, TopicMessageConsumer[] consumers) {
      this.topic = topic ;
      this.consumers = consumers ;
    }
    
    public String getTopic() { return this.topic ; }
    
    public void terminate() {
      for(TopicMessageConsumer sel : consumers) {
        sel.setTerminate();
      }
    }
  }
}