package com.neverwinterdp.scribengin.storage.s3;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.scribengin.storage.StorageInstruction;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.tool.message.MessageGenerator;
import com.neverwinterdp.util.JSONSerializer;

public class S3SourceGenerator {

  private DataflowMessageGenerator recordGenerator = new DataflowMessageGenerator();

  private int numStreams;
  private int numRecordsPerStream;

  private Stopwatch stopwatch = Stopwatch.createUnstarted();

  public void generateSource(S3Client s3Client, String bucketName, String folderPath, int numStreams, int numRecordsPerStream) throws Exception {
    stopwatch.start();
    System.out.println("generating test Data...");
    this.numStreams = numStreams;
    this.numRecordsPerStream = numRecordsPerStream;

    StorageConfig descriptor = new StorageConfig("s3", bucketName);
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderPath);

    Sink sink = new S3Sink(s3Client, descriptor);
    generateStream(sink);
  }

  void generateStream(Sink sink) throws Exception {
    for (int i = 0; i < numStreams; i++) {
      SinkPartitionStream stream = sink.newStream();
      SinkPartitionStreamWriter writer = stream.getWriter();
      for (int j = 0; j < numRecordsPerStream; j++) {
        String partition = Integer.toString(i);
        writer.append(recordGenerator.nextRecord(partition, 2));
      }
      writer.commit();
      writer.close();
    }
  }
  
  static public class DataflowMessageGenerator implements MessageGenerator {
    MessageGenerator defaultMessageGenerator = new MessageGenerator.DefaultMessageGenerator() ;
    static public AtomicLong idTracker = new AtomicLong() ;
    
    public byte[] nextMessage(String partition, int messageSize) {
      return JSONSerializer.INSTANCE.toBytes(nextRecord(partition, messageSize));
    }
    
    public byte[] eosMessage() {
      Record dflMessage = new Record(StorageInstruction.END_OF_DATASTREAM) ;
      return JSONSerializer.INSTANCE.toBytes(dflMessage);
    }
    
    public Record nextRecord(String partition, int messageSize) {
      byte[] messagePayload = defaultMessageGenerator.nextMessage(partition, messageSize);
      String key = "partition=" + partition + ",id=" + idTracker.getAndIncrement();
      return new Record(key, messagePayload);
    }
    
    @Override
    public int getCurrentSequenceId(String partition) {
      return defaultMessageGenerator.getCurrentSequenceId(partition);
    }
    
    @Override
    public Map<String, AtomicInteger> getMessageTrackers() { return defaultMessageGenerator.getMessageTrackers(); }

  }
}
