package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStreamReader implements SourcePartitionStreamReader {
  private PartitionConfig partitionConfig;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public KafkaSourceStreamReader(KafkaClient kafkaClient, PartitionConfig pConfig, PartitionMetadata pmd) throws Exception {
    this.partitionConfig = pConfig;
    this.partitionReader = new KafkaPartitionReader(pConfig.attribute("name"), kafkaClient, pConfig.attribute("topic"), pmd);
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
