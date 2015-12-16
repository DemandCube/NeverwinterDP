package com.neverwinterdp.storage.kafka.source;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.source.CommitPoint;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

import kafka.javaapi.PartitionMetadata;

public class KafkaSourceStreamReader implements SourcePartitionStreamReader {
  private PartitionStreamConfig partitionConfig;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public KafkaSourceStreamReader(KafkaClient kafkaClient, PartitionStreamConfig pConfig, PartitionMetadata pmd) throws Exception {
    this.partitionConfig = pConfig;
    String readerName = pConfig.attribute("name");
    this.partitionReader = new KafkaPartitionReader(readerName, kafkaClient, pConfig.attribute("topic"), pmd);
  }
  
  @Override
  public String getName() { return partitionConfig.attribute("name"); }

  @Override
  public Record next(long maxWait) throws Exception {
    return partitionReader.nextAs(Record.class, maxWait);
  }

  @Override
  public Record[] next(int size, long maxWait) throws Exception {
    throw new Exception("To implement") ;
  }

  public boolean isEndOfDataStream() { return false; }
  
  @Override
  public void rollback() throws Exception {
    partitionReader.rollback();
  }

  @Override
  public void prepareCommit() throws Exception {
    //TODO: implement 2 phases commit correctly
  }

  @Override
  public void completeCommit() throws Exception {
    //TODO: implement 2 phases commit correctly
    partitionReader.commit();
  }
  
  @Override
  public void commit() throws Exception {
    try {
      prepareCommit() ;
      completeCommit() ;
    } catch(Exception ex) {
      rollback();
      throw ex;
    }
  }
  
  public CommitPoint getLastCommitInfo() { return this.lastCommitInfo ; }
  
  @Override
  public void close() throws Exception {
    partitionReader.close();
  }
}
