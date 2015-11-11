package com.neverwinterdp.scribengin.storage.kafka.perftest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSourcePartition;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class TopicReader {
  private KafkaClient kafkaClient;
  private String      topic;
  private String      zkConnect;
  private int         readPerReader = 1000;
  
  private AtomicLong readCounter = new AtomicLong();
  private TopicPerfReporter reporter;
  
  private ExecutorService executorService;
  
  public TopicReader(KafkaClient kafkaClient, String topic, TopicPerfReporter reporter) throws Exception {
    this.kafkaClient = kafkaClient;
    this.topic = topic ;
    this.reporter = reporter;
  }
  
  public TopicReader setReadPerReader(int num) {
    this.readPerReader = num;
    return this;
  }
  
  public void start() throws Exception {
    KafkaSource source = new KafkaSource(kafkaClient, topic + ".reader",  topic);
    KafkaSourcePartition partition = (KafkaSourcePartition)source.getLatestSourcePartition() ;
    SourcePartitionStream[] streams = partition.getPartitionStreams() ;
    executorService = Executors.newFixedThreadPool(streams.length);
    for(int i = 0; i < streams.length; i++) {
      TopicPartitionReader partitionReader = new TopicPartitionReader(streams[i]);
      executorService.submit(partitionReader);
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
  
  public long getTotalRead() { return readCounter.get(); }
  
  public class TopicPartitionReader implements Runnable {
    SourcePartitionStream       stream;
    SourcePartitionStreamReader currentReader ;
    
    TopicPartitionReader(SourcePartitionStream stream) {
      this.stream = stream;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if(currentReader != null) {
          try {
            currentReader.commit();
            currentReader.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    void doRun() throws Exception {
      while(true) {
        currentReader = stream.getReader(topic + ".reader");
        for(int i = 0; i < readPerReader; i++) {
          Record record = currentReader.next(10000);
          if(record == null) {
            currentReader.commit();
            currentReader.close();
            return;
          }
          reporter.incrRead(topic, 1);
          readCounter.incrementAndGet();
        }
        currentReader.commit();
        currentReader.close();
      }
    }
  }
}
