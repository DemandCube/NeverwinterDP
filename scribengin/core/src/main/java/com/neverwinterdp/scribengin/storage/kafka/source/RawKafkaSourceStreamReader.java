package com.neverwinterdp.scribengin.storage.kafka.source;

import java.nio.ByteBuffer;

import kafka.javaapi.PartitionMetadata;
import kafka.message.Message;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class RawKafkaSourceStreamReader implements SourceStreamReader {
  private StreamDescriptor descriptor;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public RawKafkaSourceStreamReader(StreamDescriptor descriptor, PartitionMetadata partitionMetadata) {
    this.descriptor = descriptor;
    this.partitionReader = 
        new KafkaPartitionReader(descriptor.attribute("dataflowName"), descriptor.attribute("zk.connect"), descriptor.attribute("topic"), partitionMetadata);
  }
  
  @Override
  public String getName() { return descriptor.attribute("dataflowName"); }

  @Override
  public Record next() throws Exception {
    Message message = partitionReader.nextMessage() ;
    if(message == null) return null ;
    ByteBuffer payload = message.payload();
    byte[] messageBytes = new byte[payload.limit()];
    payload.get(messageBytes);
    
    ByteBuffer key = message.key();
    byte[] keyBytes = new byte[key.limit()];
    key.get(keyBytes);
    Record record = new Record(new String(keyBytes), messageBytes) ;
    return record;
  }

  @Override
  public Record[] next(int size) throws Exception {
    throw new Exception("To implement") ;
  }

  @Override
  public void rollback() throws Exception {
    throw new Exception("To implement") ;
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
