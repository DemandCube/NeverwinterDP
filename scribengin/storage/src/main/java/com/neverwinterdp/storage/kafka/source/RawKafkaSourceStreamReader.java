package com.neverwinterdp.storage.kafka.source;

import java.nio.ByteBuffer;

import kafka.javaapi.PartitionMetadata;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.source.CommitPoint;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

public class RawKafkaSourceStreamReader implements SourcePartitionStreamReader {
  private PartitionStreamConfig partitionConfig;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public RawKafkaSourceStreamReader(KafkaClient kafkaClient, PartitionStreamConfig pConfig, PartitionMetadata pmd) throws Exception {
    partitionConfig = pConfig;
    partitionReader = 
        new KafkaPartitionReader(pConfig.attribute("name"), kafkaClient, pConfig.attribute("topic"), pmd);
  }
  
  @Override
  public String getName() { return partitionConfig.attribute("name"); }

  @Override
  public Record next(long maxWait) throws Exception {
    MessageAndOffset messageAndOffSet = partitionReader.nextMessageAndOffset(maxWait) ;
    if(messageAndOffSet == null) return null ;
    Message message = messageAndOffSet.message();
    ByteBuffer payload = message.payload();
    byte[] messageBytes = new byte[payload.limit()];
    payload.get(messageBytes);
    
    ByteBuffer key = message.key();
    byte[] keyBytes = new byte[key.limit()];
    key.get(keyBytes);
    Record dataflowMessage = new Record(new String(keyBytes), messageBytes) ;
    return dataflowMessage;
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
    if(partitionReader != null) {
      partitionReader.close();
    }
  }
}
