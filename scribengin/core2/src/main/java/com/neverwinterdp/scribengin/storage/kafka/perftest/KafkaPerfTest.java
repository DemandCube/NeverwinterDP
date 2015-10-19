package com.neverwinterdp.scribengin.storage.kafka.perftest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class KafkaPerfTest {
  @Parameter(names = "--topics", description = "The topic")
  private String topics ;
  
  @ParametersDelegate
  private TopicPerfConfig topicConfig = new TopicPerfConfig();
  
  
  public void run() throws Exception {
    String[] topic = topics.split(",");
    TopicPerfReporter reporter = new TopicPerfReporter();
    ExecutorService executorService = Executors.newFixedThreadPool(topic.length);
    for(String selTopic : topic) {
      TopicPerfConfig selTopicConfig = topicConfig.clone();
      selTopicConfig.topic = selTopic;
      TopicPerfRunner runner = new TopicPerfRunner(selTopicConfig, reporter);
      executorService.submit(runner);
    }
    executorService.shutdown();
    long stopTime = System.currentTimeMillis() + topicConfig.maxRunTime + 900000;
    while(!executorService.isTerminated()) {
      System.out.println(reporter.getFormattedText());
      if(System.currentTimeMillis() > stopTime) break;
      Thread.sleep(15000);
    }
    System.out.println(reporter.getFormattedText());
  }
  
  static public void main(String[] args) throws Exception {
    KafkaPerfTest test = new KafkaPerfTest();
    new JCommander(test, args);
    test.run();
  }
}
