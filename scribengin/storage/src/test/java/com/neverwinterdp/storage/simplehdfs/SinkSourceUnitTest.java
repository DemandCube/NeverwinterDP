package com.neverwinterdp.storage.simplehdfs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.simplehdfs.SimpleHDFSStorage;
import com.neverwinterdp.storage.simplehdfs.Segment;
import com.neverwinterdp.storage.simplehdfs.sink.HDFSSink;
import com.neverwinterdp.storage.simplehdfs.source.HDFSSource;
import com.neverwinterdp.storage.simplehdfs.source.HDFSSourcePartition;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class SinkSourceUnitTest {
  static String DATA_DIRECTORY = "./build/hdfs/sink" ;
  
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    Segment.SMALL_DATASIZE_THRESHOLD =  8 *  1024 * 1024;
    Segment.MEDIUM_DATASIZE_THRESHOLD = 8 * Segment.SMALL_DATASIZE_THRESHOLD;
    Segment.LARGE_DATASIZE_THRESHOLD  = 8 * Segment.MEDIUM_DATASIZE_THRESHOLD;
    FileUtil.removeIfExist(DATA_DIRECTORY, false);
    fs = FileSystem.getLocal(new Configuration()) ;
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testSink() throws Exception {
    StorageConfig storageConfig = new StorageConfig("HDFS", DATA_DIRECTORY);
    storageConfig.setPartitionStream(1);
    SimpleHDFSStorage hdfsStorage = new SimpleHDFSStorage(fs, storageConfig);
    HDFSSink sink = hdfsStorage.getSink();
    SinkPartitionStream[] streams = sink.getPartitionStreams();
    Assert.assertEquals(1, streams.length);
    
    int NUM_OF_COMMIT = 5;
    int NUM_OF_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS = NUM_OF_COMMIT * NUM_OF_RECORD_PER_COMMIT; 
    SinkPartitionStream stream = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream.getWriter();
    for(int i = 0; i < NUM_OF_COMMIT; i++) {
      for(int j = 0; j < NUM_OF_RECORD_PER_COMMIT; j ++) {
        String key = "stream=" + stream.getPartitionStreamId() +",buffer=" + i + ",record=" + j;
        writer.append(Message.create(key, key));
      }
      writer.commit();
    }
    HDFSUtil.dump(fs, DATA_DIRECTORY);
    writer.close();
    
    Assert.assertEquals(NUM_OF_RECORDS, count(DATA_DIRECTORY));
   
    writer.close();
    stream.optimize();
    Assert.assertEquals(NUM_OF_RECORDS, count(DATA_DIRECTORY));
    System.out.println("\n\n") ;
    HDFSUtil.dump(fs, DATA_DIRECTORY);
  }
  
  @Test
  public void testRollback() throws Exception {
    StorageConfig storageConfig = new StorageConfig("HDFS", DATA_DIRECTORY);
    storageConfig.setPartitionStream(1);
    storageConfig.attribute("partitioner", "hourly");
    SimpleHDFSStorage hdfsStorage = new SimpleHDFSStorage(fs, storageConfig);
    HDFSSink sink = hdfsStorage.getSink();
    
    SinkPartitionStream stream0 = sink.getPartitionStream(0);
    SinkPartitionStreamWriter writer = stream0.getWriter();
    for(int i = 0; i < 10; i ++) {
      writer.append(Message.create("key-" + i, "record " + i));
    }
    writer.rollback();
    writer.close();
    HDFSUtil.dump(fs, DATA_DIRECTORY);
  }
  
  @Test
  public void testMultiThread() throws Exception {
    int NUM_OF_WRITER = 2;
    int NUM_OF_SEG_PER_WRITER = 50;
    int TOTAL_NUM_OF_MESSAGE = NUM_OF_WRITER * NUM_OF_SEG_PER_WRITER * 5000;
    
    StorageConfig storageConfig = new StorageConfig("HDFS", DATA_DIRECTORY);
    storageConfig.setPartitionStream(1);
    storageConfig.attribute("partitioner", "15min");
    SimpleHDFSStorage hdfsStorage = new SimpleHDFSStorage(fs, storageConfig);
    HDFSSink sink = hdfsStorage.getSink();
    
    SinkStreamWriterTask[] task = new SinkStreamWriterTask[NUM_OF_WRITER]; 
    ExecutorService service = Executors.newFixedThreadPool(task.length);
    for(int i = 0; i < task.length; i++) {
      service.execute(new SinkStreamWriterTask(sink, NUM_OF_SEG_PER_WRITER));
    }
    service.shutdown();
    while(!service.isTerminated()) {
      HDFSUtil.dump(fs, DATA_DIRECTORY);
      System.out.println("----------------------------------------");
      Thread.sleep(5000);
    }
    HDFSUtil.dump(fs, DATA_DIRECTORY);
    Assert.assertEquals(TOTAL_NUM_OF_MESSAGE, count(DATA_DIRECTORY));
  }
  
  public class SinkStreamWriterTask implements Runnable {
    private Sink sink ;
    private int  numOfSegment ;
    
    public SinkStreamWriterTask(Sink sink, int  numOfSegment) {
      this.sink = sink ;
      this.numOfSegment = numOfSegment;
    }
    
    @Override
    public void run() {
      try {
        SinkPartitionStream stream = sink.getPartitionStream(0);
        SinkPartitionStreamWriter writer = stream.getWriter();
        byte[] data = new byte[512];
        for(int i = 0; i < numOfSegment; i++) {
          for(int j = 0; j < 5000; j ++) {
            writer.append(Message.create("key-" + i, data));
          }
          writer.commit();
        }
        writer.close();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  private int count(String hdfsDir) throws Exception {
    StorageConfig storageConfig = new StorageConfig("HDFS", hdfsDir);
    storageConfig.setPartitionStream(1);
    SimpleHDFSStorage hdfsStorage = new SimpleHDFSStorage(fs, storageConfig);
    HDFSSource hdfsSource = hdfsStorage.getSource();
    HDFSSourcePartition sourcePartition = hdfsSource.getLatestSourcePartition();
    SourcePartitionStream[] stream = sourcePartition.getPartitionStreams();
    int count  = 0; 
    for(int  i = 0; i < stream.length; i++) {
      SourcePartitionStreamReader reader = stream[i].getReader("test") ;
      while(reader.next(1000) != null) {
        count++ ;
      }
      reader.close();
    }
    return count;
  }
}
