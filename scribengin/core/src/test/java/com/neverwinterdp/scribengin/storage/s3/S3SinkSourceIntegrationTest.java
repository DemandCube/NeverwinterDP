package com.neverwinterdp.scribengin.storage.s3;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3SourcePartition;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;


public class S3SinkSourceIntegrationTest {

  private static S3Client s3Client;

  protected String bucketName;
  protected String storageFolder;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client(); 
  }

  @AfterClass
  public static void afterClass() {
    s3Client.onDestroy();
  }
  
  @Before
  public void setup() throws Exception {
    bucketName = "s3-sink-source-test-" + UUID.randomUUID();
    storageFolder = "integration-test";
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    
    System.out.println("Creating bucket: "+bucketName);
    s3Client.createBucket(bucketName);
    System.out.println("Creating folder: "+storageFolder);
    s3Client.createS3Folder(bucketName, storageFolder);
    
  }
  
  @After
  public void teardown() throws Exception {
    System.out.println("Deleting bucket: "+bucketName);
    s3Client.deleteBucket(bucketName, true);
  }
  
  @Test
  public void testS3SourceSink() throws Exception{
    S3Storage storage = new S3Storage(bucketName, storageFolder);
    S3Sink sink = storage.getSink(s3Client) ;
    
    
    SinkPartitionStream[] streams = sink.getPartitionStreams();
    //Assert num of streams is set to the default, since we don't set it
    Assert.assertEquals(new StorageConfig().getPartitionStream(), streams.length);
    
    int NUM_OF_COMMIT = 5;
    int NUM_OF_RECORD_PER_COMMIT = 100;
    int NUM_OF_RECORDS = NUM_OF_COMMIT * NUM_OF_RECORD_PER_COMMIT; 
    //Only write to one partition
    SinkPartitionStream stream = sink.getParitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < NUM_OF_COMMIT; i++) {
      for(int j = 0; j < NUM_OF_RECORD_PER_COMMIT; j ++) {
        String key = "stream=" + stream.getPartitionStreamId() +",buffer=" + i + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      writer.commit();
    }
    
    S3Util.listStructure(s3Client, bucketName);
    writer.close();
    Assert.assertEquals(NUM_OF_RECORDS, count(storage));
  }
  
  
  @Test
  public void testRollback() throws Exception {
    S3Storage storage = new S3Storage(bucketName, storageFolder);
    S3Sink sink = storage.getSink(s3Client) ;
    SinkPartitionStream stream = sink.getParitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    int NUM_OF_RECORDS = 10;
    for(int i = 0; i < NUM_OF_RECORDS; i ++) {
      writer.append(Record.create("key-" + i, "record " + i));
    }
    
    Assert.assertEquals(0, count(storage));
    
    writer.rollback();
    writer.close();
    
    S3Util.listStructure(s3Client, bucketName);
    Assert.assertEquals(0, count(storage));
  }
  
  @Test
  public void testMultiThread() throws Exception {
    int NUM_OF_WRITER = 3;
    int NUM_OF_PARTITIONS_PER_WRITER = 10;
    int NUM_RECORDS_PER_WRITER = 100;
    int TOTAL_NUM_OF_MESSAGE = NUM_OF_WRITER * NUM_OF_PARTITIONS_PER_WRITER * NUM_RECORDS_PER_WRITER;
    S3Storage storage = new S3Storage(bucketName, storageFolder);
    S3Sink sink = storage.getSink(s3Client) ;
    
    SinkStreamWriterTask[] task = new SinkStreamWriterTask[NUM_OF_WRITER]; 
    ExecutorService service = Executors.newFixedThreadPool(task.length);
    for(int i = 0; i < task.length; i++) {
      service.execute(new SinkStreamWriterTask(sink, NUM_OF_PARTITIONS_PER_WRITER, NUM_RECORDS_PER_WRITER));
    }
    service.shutdown();
    while(!service.isTerminated()) {
      S3Util.listStructure(s3Client, bucketName);
      System.out.println("----------------------------------------");
      Thread.sleep(10000);
    }
    S3Util.listStructure(s3Client, bucketName);
    Assert.assertEquals(TOTAL_NUM_OF_MESSAGE, count(storage));
  }
  
  private int count(S3Storage storage) throws Exception {
    S3SourcePartition source = storage.getSource(s3Client);
    SourcePartitionStream[] sourceStreams = source.getPartitionStreams();

    int recordCount = 0 ;
    for (int i = 0; i < sourceStreams.length; i++) {
      SourcePartitionStream stream = sourceStreams[i];
      SourcePartitionStreamReader reader = stream.getReader(stream.getDescriptor().getLocation());
      while (reader.next(1000) != null) {
        recordCount++;
      }
      reader.close();
    }
    return recordCount;
  }
  
  
  public class SinkStreamWriterTask implements Runnable {
    private Sink sink ;
    private int  numOfPartitions ;
    private int numRecords;
    
    public SinkStreamWriterTask(Sink sink, int  numOfPartitions, int numRecords) {
      this.sink = sink ;
      this.numOfPartitions = numOfPartitions;
      this.numRecords = numRecords;
    }
    
    @Override
    public void run() {
      try {
        SinkPartitionStream stream = sink.getParitionStream(0);
        SinkPartitionStreamWriter writer = stream.getWriter();
        byte[] data = new byte[512];
        for(int i = 0; i < numOfPartitions; i++) {
          for(int j = 0; j < numRecords; j ++) {
            writer.append(Record.create("key-" + i, data));
          }
          writer.commit();
        }
        writer.close();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
