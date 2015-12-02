package com.neverwinterdp.storage.kafka.perftest;

import com.neverwinterdp.kafka.KafkaClient;

public class TopicPerfRunner implements Runnable {
  private TopicPerfConfig topicConfig ;
  private TopicPerfReporter reporter;
  
  public TopicPerfRunner(TopicPerfConfig topicConfig, TopicPerfReporter reporter) {
    this.topicConfig = topicConfig;
    this.reporter = reporter;
  }

  @Override
  public void run() {
    KafkaClient kafkaClient = null;
    try {
      long startTime = System.currentTimeMillis();
      kafkaClient = new KafkaClient("KafkaClient", topicConfig.zkConnect);
      TopicWriter topicWriter = new TopicWriter(kafkaClient, topicConfig, reporter);
      topicWriter.start();

      TopicReader topicReader = new TopicReader(kafkaClient, topicConfig.topic, reporter);
      topicReader.setReadPerReader(topicConfig.readerReadPerReader);
      if(topicConfig.readerRunDelay > 0) {
        Thread.sleep(topicConfig.readerRunDelay);
      }
      topicReader.start();
      
      topicWriter.waitForTermination(topicConfig.maxRunTime);

      long remainWaitTime = topicConfig.maxRunTime - (System.currentTimeMillis() - startTime);
      if(remainWaitTime > 0) topicReader.waitForTermination(remainWaitTime);
    
    } catch(Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        kafkaClient.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
