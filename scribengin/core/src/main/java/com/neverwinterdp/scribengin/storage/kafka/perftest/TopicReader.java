package com.neverwinterdp.scribengin.storage.kafka.perftest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class TopicReader {
  private String topic;
  private String zkConnect;
  private int    readPerReader = 1000;
  
  private AtomicLong readCounter = new AtomicLong();
  
  private ExecutorService executorService;
  
  public TopicReader(String topic, String zkConnect) throws Exception {
    this.topic = topic ;
    this.zkConnect = zkConnect;
  }
  
  public TopicReader setReadPerReader(int num) {
    this.readPerReader = num;
    return this;
  }
  
  public void start() throws Exception {
    KafkaSource source = new KafkaSource(topic + ".reader", zkConnect,  topic) ;
    SourceStream[] streams = source.getStreams() ;
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
    SourceStream       stream;
    SourceStreamReader currentReader ;
    
    TopicPartitionReader(SourceStream stream) {
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
          DataflowMessage dflMessage = currentReader.next(10000);
          if(dflMessage == null) {
            currentReader.commit();
            currentReader.close();
            return;
          }
          readCounter.incrementAndGet();
        }
        currentReader.commit();
        currentReader.close();
      }
    }
  }
}
