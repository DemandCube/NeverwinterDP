package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.PartitionConfig;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class KafkaSourceStreamReader implements SourcePartitionStreamReader {
  private PartitionConfig descriptor;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public KafkaSourceStreamReader(PartitionConfig descriptor, 
      PartitionMetadata partitionMetadata) throws Exception {
    this.descriptor = descriptor;
    this.partitionReader = 
        new KafkaPartitionReader(descriptor.attribute("name"), descriptor.attribute("zk.connect"), descriptor.attribute("topic"), partitionMetadata);
  }
  
  @Override
  public String getName() { return descriptor.attribute("name"); }

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
  }
 
}
