package com.neverwinterdp.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaPartitionReader {
  private String name;
  private String zkConnect;
  private String topic ;
  private PartitionMetadata partitionMetadata;
  private int fetchSize = 100000;
  private SimpleConsumer consumer;
  private long currentOffset;
  private ByteBufferMessageSet       currentMessageSet;
  private Iterator<MessageAndOffset> currentMessageSetIterator;

  public KafkaPartitionReader(String name, String zkConnect, String topic, PartitionMetadata partitionMetadata) throws Exception {
    this.name = name;
    this.zkConnect = zkConnect;
    this.topic = topic;
    this.partitionMetadata = partitionMetadata;
    reconnect() ;
    currentOffset = getLastCommitOffset();
  }
  
  public int getPartition() { return partitionMetadata.partitionId(); }
  
  public long getCurrentOffset() { return this.currentOffset ; }
  
  public void setFetchSize(int size) { this.fetchSize = size; }
  
  public void reconnect() throws Exception {
    if(consumer != null) consumer.close();
    Broker broker = partitionMetadata.leader();
    if(broker != null) {
      consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, name);
    } else {
      reconnect(3, 5000);
    }
  }
  
  public void reconnect(int retry, long retryDelay) throws Exception {
    if(consumer != null) consumer.close();
    Exception error = null ;
    for(int i = 0; i < retry; i++) {
      Thread.sleep(retryDelay);
      //Refresh the partition metadata
      try {
      KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnect);
      kafkaTool.connect();
      this.partitionMetadata = kafkaTool.findPartitionMetadata(topic, partitionMetadata.partitionId());
      kafkaTool.close();
      Broker broker = partitionMetadata.leader();
      if(broker != null) {
        consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, name);
        return;
      }
      } catch(Exception ex) {
        error = ex ;
      }
    }
    throw new Exception("Cannot connect after " + retry + " times", error);
  }
  
  public void commit() throws Exception {
    CommitOperation commitOp = new CommitOperation(currentOffset, (short) 0) ;
    execute(commitOp, 3, 500);
  }
  
  public void rollback() throws Exception  {
    currentOffset = getLastCommitOffset();
    currentMessageSet = null ;
    currentMessageSetIterator = null;
    //commit() ;
  }
  
  public void close() throws Exception {
    consumer.close();
  }
  
  public byte[] next(long maxWait) throws Exception {
    if(currentMessageSetIterator == null) nextMessageSet(maxWait);
    byte[] payload = getCurrentMessagePayload();
    if(payload != null) return payload;
    nextMessageSet(maxWait);
    return getCurrentMessagePayload();
  }

  public Message nextMessage(long maxWait) throws Exception {
    if(currentMessageSetIterator == null) nextMessageSet(maxWait);
    Message message = getCurrentMessage();
    if(message != null) return message;
    nextMessageSet(maxWait);
    return getCurrentMessage();
  }
  
  public <T> T nextAs(Class<T> type, long maxWait) throws Exception {
    byte[] data = next(maxWait);
    if(data == null) return null;
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }

  public List<byte[]> next(int maxRead, long maxWait) throws Exception {
    List<byte[]> holder = new ArrayList<>() ;
    for(int i = 0; i < maxRead; i++) {
      byte[] payload = next(maxWait) ;
      if(payload == null) return holder;
      holder.add(payload);
    }
    return holder;
  }
  
  public List<byte[]> fetch(int fetchSize, int maxRead, long maxWait) throws Exception {
    return fetch(fetchSize, maxRead, maxWait, 3) ;
  }
  
  public List<byte[]> fetch(int fetchSize, int maxRead, long maxWait, int numRetries) throws Exception {
    FetchOperation fetchOperation = new FetchOperation(fetchSize, maxRead, (int)maxWait);
    return execute(fetchOperation, numRetries, 500);
  }
  
  byte[] getCurrentMessagePayload() {
    Message message = getCurrentMessage() ;
    if(message == null) return null ;
    ByteBuffer payload = message.payload();
    byte[] bytes = new byte[payload.limit()];
    payload.get(bytes);
    return bytes;
  }
 
  Message getCurrentMessage() {
    while(currentMessageSetIterator.hasNext()) {
      MessageAndOffset messageAndOffset = currentMessageSetIterator.next();
      if (messageAndOffset.offset() < currentOffset) continue; //old offset, ignore
      Message message = messageAndOffset.message();
      currentOffset = messageAndOffset.nextOffset();
      return message;
    }
    return null;
  }
 
  
  void nextMessageSet(long maxWait) throws Exception {
    FetchRequest req = 
        new FetchRequestBuilder().
        clientId(name).
        addFetch(topic, partitionMetadata.partitionId(), currentOffset, fetchSize).
        minBytes(1).
        maxWait((int)maxWait).
        build();
    
    FetchResponse fetchResponse = consumer.fetch(req);
    if(fetchResponse.hasError()) {
      throw new Exception("TODO: handle the error, reset the consumer....");
    }
    
    currentMessageSet = fetchResponse.messageSet(topic, partitionMetadata.partitionId());
    currentMessageSetIterator = currentMessageSet.iterator();
  }
  
  long getLastCommitOffset() {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionMetadata.partitionId());
    List<TopicAndPartition> topicAndPartitions = new ArrayList<>();
    topicAndPartitions.add(topicAndPartition);
    OffsetFetchRequest oRequest = new OffsetFetchRequest(name, topicAndPartitions, (short) 0, 0, name);
    OffsetFetchResponse oResponse = consumer.fetchOffsets(oRequest);
    Map<TopicAndPartition, OffsetMetadataAndError> offsets = oResponse.offsets();
    OffsetMetadataAndError offset = offsets.get(topicAndPartition);
    long currOffset = offset.offset() ;
    if(currOffset < 0) currOffset = 0;
    return currOffset;
  }
  
  <T> T execute(Operation<T> op, int retry, long retryDelay) throws Exception {
    Exception error = null;
    for(int i = 0; i < retry; i++) {
      try {
        if(error != null) {
          reconnect(1, retryDelay);
        }
        return op.execute();
      } catch(Exception ex) {
        error = ex;
        //ex.printStackTrace();
      }
    }
    throw error;
  }
  
  static public interface Operation<T> {
    public T execute() throws Exception ;
  }
  
  class CommitOperation implements Operation<Short> {
    long offset;
    short errorCode;
    
    public CommitOperation(long offset, short errorCode) {
      this.offset = offset;
      this.errorCode = errorCode;
    }
    
    @Override
    public Short execute() throws Exception {
      short versionID = 0;
      int correlationId = 0;
      TopicAndPartition tp = new TopicAndPartition(topic, partitionMetadata.partitionId());
      OffsetAndMetadata offsetAndMeta = new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), errorCode);
      Map<TopicAndPartition, OffsetAndMetadata> mapForCommitOffset = new HashMap<TopicAndPartition, OffsetAndMetadata>();
      mapForCommitOffset.put(tp, offsetAndMeta);
      OffsetCommitRequest offsetCommitReq = new OffsetCommitRequest(name, mapForCommitOffset, correlationId, name, versionID);
      OffsetCommitResponse offsetCommitResp = consumer.commitOffsets(offsetCommitReq);
      return (Short) offsetCommitResp.errors().get(tp);
    }
  }

  class FetchOperation implements Operation<List<byte[]>> {
    int fetchSize;
    int maxRead;
    int maxWait;
    
    public FetchOperation(int fetchSize, int maxRead, int maxWait) {
      this.fetchSize = fetchSize;
      this.maxRead = maxRead ;
      this.maxWait = maxWait ;
    }
    
    public List<byte[]> execute() throws Exception {
      FetchRequest req = 
          new FetchRequestBuilder().
          clientId(name).
          addFetch(topic, partitionMetadata.partitionId(), currentOffset, fetchSize).
          minBytes(1).
          maxWait(maxWait).
          build();
      
      FetchResponse fetchResponse = consumer.fetch(req);
      if(fetchResponse.hasError()) {
        short errorCode = fetchResponse.errorCode(topic, partitionMetadata.partitionId());
        String msg = "Kafka error code = " + errorCode + ", Partition  " + partitionMetadata.partitionId() ;
        throw new Exception(msg);
      }
      List<byte[]> holder = new ArrayList<byte[]>();
      ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partitionMetadata.partitionId());
      int count = 0;
      for(MessageAndOffset messageAndOffset : messageSet) {
        if (messageAndOffset.offset() < currentOffset) continue; //old offset, ignore
        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        holder.add(bytes);
        currentOffset = messageAndOffset.nextOffset();
        count++;
        if(count == maxRead) break;
      }
      return holder ;
    }
  }
}