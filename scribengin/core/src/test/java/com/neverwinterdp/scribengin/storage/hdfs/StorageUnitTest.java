package com.neverwinterdp.scribengin.storage.hdfs;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class StorageUnitTest {
  static String STORAGE_DIR = "./build/hdfs/storage" ;

  private FileSystem fs ;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist(STORAGE_DIR, false);
    fs = FileSystem.getLocal(new Configuration()) ;
    fs.mkdirs(new Path(STORAGE_DIR));
  }

  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void test() throws Exception {
    int NUM_OF_WRITER             = 5;
    int NUM_OF_SEG_PER_WRITER     = 100;
    int NUM_OF_RECORD_PER_SEGMENT = 5000;
    int TOTAL = NUM_OF_WRITER * NUM_OF_SEG_PER_WRITER * NUM_OF_RECORD_PER_SEGMENT;
    Segment.SMALL_DATASIZE_THRESHOLD =  32 * 1024 * 1024;
    Segment.MEDIUM_DATASIZE_THRESHOLD = 8 * Segment.SMALL_DATASIZE_THRESHOLD;
    Segment.LARGE_DATASIZE_THRESHOLD  = 8 * Segment.MEDIUM_DATASIZE_THRESHOLD;
    AtomicInteger idTracker = new AtomicInteger() ;
    Storage<Record> storage = new Storage<>(fs, STORAGE_DIR, Record.class);
    ExecutorService writerService = Executors.newFixedThreadPool(3);
    for(int i = 0; i < NUM_OF_WRITER; i++) {
      StorageWriterRunner runner = 
        new StorageWriterRunner(storage, idTracker, NUM_OF_SEG_PER_WRITER, NUM_OF_RECORD_PER_SEGMENT, 512 /*recordSize*/) ;
      writerService.submit(runner);
    }
    writerService.shutdown();
    writerService.awaitTermination(30000, TimeUnit.SECONDS);
    Assert.assertEquals(TOTAL, countStorage());
    HDFSUtil.dump(fs, STORAGE_DIR);
    
    storage.refresh();
    storage.optimizeMediumSegments();
    
    HDFSUtil.dump(fs, STORAGE_DIR);
    int count = countStorage() ;
    Assert.assertEquals(TOTAL, countStorage());
    System.out.println("Storage Count: " + count);
  }

  int countStorage() throws Exception {
    StorageReader<Record> reader = new StorageReader<>("test", fs, STORAGE_DIR, Record.class);
    Record record = null ;
    int count = 0 ;
    while((record = reader.next(1000)) != null) {
      int currentId = record.getTrackId();
      count++ ;
    }
    reader.close();
    return count ;
  }
  
  static public class StorageWriterRunner implements Runnable {
    Storage<Record> storage ;
    AtomicInteger idTracker;
    int numOfSegment;
    int recordPerSegment;
    int recordSize;
    
    public StorageWriterRunner(Storage<Record> storage, AtomicInteger idTracker, int numOfSegment, int recordPerSegment, int recordSize) {
      this.storage = storage;
      this.idTracker = idTracker;
      this.numOfSegment = numOfSegment;
      this.recordPerSegment = recordPerSegment;
      this.recordSize = recordSize;
    }

    @Override
    public void run() {
      try {
        Random rand = new Random();
        StorageWriter<Record> writer = storage.getStorageWriter();
        byte[] data = new byte[recordSize];
        for(int i = 0; i < numOfSegment; i++) {
          for(int j = 0; j < recordPerSegment; j++) {
            Record record = new Record(idTracker.getAndIncrement(), data);
            writer.append(record);
          }
          writer.commit();
          if(rand.nextInt(5) == 1) {
            storage.optimizeBufferSegments();
          }
          if(rand.nextInt(20) == 1) {
            storage.optimizeSmallSegments();
          }
        }
        storage.optimizeSmallSegments();
      } catch(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
  
  static public class Record {
    private int    trackId;
    private byte[] data;
    
    public Record() {} 
    
    public Record(int trackId, byte[] data) {
      this.trackId = trackId;
      this.data = data;
    }
    
    public int getTrackId() { return trackId; }
    public void setTrackId(int trackId) { this.trackId = trackId; }
    
    public byte[] getData() { return data; }
    public void setData(byte[] data) { this.data = data; }
  }
}
