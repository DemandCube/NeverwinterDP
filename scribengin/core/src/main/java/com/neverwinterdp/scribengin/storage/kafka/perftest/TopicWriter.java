package com.neverwinterdp.scribengin.storage.kafka.perftest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class TopicWriter {
  private KafkaClient kafkaClient;
  private String topic;
  private int    numOfPartitions   = 10;
  private int    numOfReplications = 1;
  private long   numOfMessages     = 10000;
  private int    writePerWriter    = 1000;
  
  private ExecutorService executorService;
  
  private AtomicLong idTracker = new AtomicLong();

  public TopicWriter(KafkaClient kafkaClient, String topic, long numOfMessages) {
    this.kafkaClient = kafkaClient;
    this.topic = topic ;
    this.numOfMessages = numOfMessages;
  }
  
  public TopicWriter setNumOfPartitions(int num) {
    this.numOfPartitions = num;
    return this;
  }
  
  public TopicWriter setWritePerWriter(int num) {
    this.writePerWriter = num;
    return this;
  }
  
  public TopicWriter setNumOfReplicatons(int num) {
    this.numOfReplications = num;
    return this;
  }
  
  public TopicWriter setNumOfMessages(long num) {
    this.numOfMessages = num;
    return this;
  }
  
  public long getTotalWrite() { return idTracker.get(); }
  
  public void start() throws Exception {
    if(kafkaClient.getKafkaTool().topicExits(topic)) {
      kafkaClient.getKafkaTool().deleteTopic(topic);
    }
    kafkaClient.getKafkaTool().createTopic(topic, numOfReplications, numOfPartitions);
    KafkaSink sink = new KafkaSink(kafkaClient, topic + ".writer", topic);
    executorService = Executors.newFixedThreadPool(numOfPartitions);
    for(int i = 0; i < numOfPartitions; i++) {
      SinkStream stream = sink.newStream();
      TopicPartitionWriter partitionWriter = new TopicPartitionWriter(stream);
      executorService.submit(partitionWriter);
    }
    executorService.shutdown();
  }
  
  public void shutdown() throws Exception {
    if(executorService == null) return;
    if(executorService.isTerminated()) return ;
    executorService.shutdownNow();
  }
  
  public void waitForTermination(long timeout) throws Exception {
    executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
  }
  
  public class TopicPartitionWriter implements Runnable {
    private SinkStream       stream;
    private SinkStreamWriter currentWriter = null;
    
    TopicPartitionWriter(SinkStream stream) {
      this.stream = stream ;
    }
    
    @Override
    public void run() {
      try {
        doRun() ;
      } catch(InterruptedException ex) {
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if(currentWriter != null) {
          try {
            currentWriter.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        System.out.println("Stream " + stream.getPartitionConfig().getId() + " finished");
      }
    }
    
    void doRun() throws Exception {
      byte[] data = new byte[512];
      while(idTracker.get() < numOfMessages) {
        currentWriter = stream.getWriter();
        for(int i = 0; i < writePerWriter; i++) {
          if(idTracker.get() >= numOfMessages) break;
          long id  = idTracker.incrementAndGet();
          String key = "message-" + id;
          DataflowMessage dflMessage = new DataflowMessage(key, data);
          currentWriter.append(dflMessage);
          if(id % 50000 == 0) {
            System.out.println("Write " + id + " messages");
          }
        }
        System.out.println("Write " + idTracker.get() + " messages");
        currentWriter.close();
      }
    }
  }
}