package com.neverwinterdp.storage.kafka.perftest;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;

public class TopicWriter {
  private KafkaClient kafkaClient;
  private TopicPerfConfig topicConfig;
  private TopicPerfReporter reporter;
  
  private ExecutorService executorService;
  
  private AtomicLong idTracker = new AtomicLong();

  public TopicWriter(KafkaClient kafkaClient, TopicPerfConfig topicConfig, TopicPerfReporter reporter) {
    this.kafkaClient = kafkaClient;
    this.topicConfig = topicConfig;
    this.reporter = reporter;
  }
  
  public long getTotalWrite() { return idTracker.get(); }
  
  public void start() throws Exception {
    if(kafkaClient.getKafkaTool().topicExits(topicConfig.topic)) {
      kafkaClient.getKafkaTool().deleteTopic(topicConfig.topic);
    }
    
    kafkaClient.
      getKafkaTool().
      createTopic(topicConfig.topic, topicConfig.topicNumOfReplications, topicConfig.topicNumOfPartitions);
    KafkaSink sink = new KafkaSink(kafkaClient, topicConfig.topic + ".writer", topicConfig.topic);
    executorService = Executors.newFixedThreadPool(topicConfig.topicNumOfPartitions);
    for(int i = 0; i < topicConfig.topicNumOfPartitions; i++) {
      SinkPartitionStream stream = sink.getPartitionStream(i);
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
    private SinkPartitionStream       stream;
    private SinkPartitionStreamWriter currentWriter = null;
    private Random                    random        = new Random();

    TopicPartitionWriter(SinkPartitionStream stream) {
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
      }
    }
    
    void doRun() throws Exception {
      while(idTracker.get() < topicConfig.topicNumOfMessages) {
        currentWriter = stream.getWriter();
        for(int i = 0; i < topicConfig.writerWritePerWriter; i++) {
          if(idTracker.get() >= topicConfig.topicNumOfMessages) break;
          long id  = idTracker.incrementAndGet();
          String key = "message-" + id;
          byte[] data = new byte[512];
          random.nextBytes(data);
          Record record = new Record(key, data);
          currentWriter.append(record);
          reporter.incrWrite(topicConfig.topic, 1);
          if(topicConfig.writerWriteBreakInPeriod > 0 && id % 1000 == 0) {
            Thread.sleep(topicConfig.writerWriteBreakInPeriod);
          }
        }
        currentWriter.close();
      }
    }
  }
}