package com.neverwinterdp.nstorage.hdfs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.nstorage.NStorageConsistencyVerifier;
import com.neverwinterdp.nstorage.NStorageReader;
import com.neverwinterdp.nstorage.NStorageWriter;
import com.neverwinterdp.nstorage.SegmentConsistency;
import com.neverwinterdp.nstorage.test.RandomCommitRollbackTRGenerator;
import com.neverwinterdp.nstorage.test.TrackingRecord;
import com.neverwinterdp.nstorage.test.TrackingRecordService;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HDFSNStorageUnitTest {
  final static public String WORKING_DIR  = "build/working";
  final static public String STORAGE_NAME = "seg-storage";
  
  private EmbededZKServerSet zkCluster;
  private Registry           registry;
  private FileSystem         fs ;
  
  @BeforeClass
  static public void beforeClass() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
  }
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist(WORKING_DIR, false);
    zkCluster = new EmbededZKServerSet(WORKING_DIR + "/zookeeper", 2181, 1);
    zkCluster.start();
    registry = RegistryConfig.getDefault().newInstance().connect();
    fs = FileSystem.getLocal(new Configuration()).getRaw();
  }
  
  @After
  public void teardown() throws Exception {
    registry.shutdown();
    zkCluster.shutdown();
  }
  
  @Test
  public void testCommit() throws Exception {
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    RandomCommitRollbackTRGenerator storageWriter = new RandomCommitRollbackTRGenerator(storage.getWriter("test"), 1, 1000);
    storageWriter.writeWithCommit();
    storageWriter.writerCloseAndRemove();
    
    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    NStorageReader reader = storage.getReader("reader");
    byte[] record = null ;
    TrackingRecordService trackingService = new TrackingRecordService(1000);
    while((record = reader.nextRecord(1000)) != null) {
      TrackingRecord trackingRecord = JSONSerializer.INSTANCE.fromBytes(record, TrackingRecord.class);
      trackingService.log(trackingRecord);
    }
    trackingService.report();
  }
  
  @Test
  public void testRollback() throws Exception {
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    RandomCommitRollbackTRGenerator storageWriter = new RandomCommitRollbackTRGenerator(storage.getWriter("test"), 1, 100);
    storageWriter.writeWithCommit();
    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.OK, scVerifier.getMinCommitConsistency());

    storageWriter.writeWithRollback();
    
    storageWriter.writerCloseAndRemove();
    
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    NStorageReader reader = storage.getReader("reader");
    byte[] record = null ;
    int count = 0;
    while((record = reader.nextRecord(1000)) != null) {
      System.out.println("record: " + new String(record));
      count++;
    }
    System.out.println("read: " + count);
  }
  
  @Test
  public void testMultipleWriter() throws Exception {
    int NUM_OF_WRITERS = 3;
    int NUM_OF_COMMIT = 25;
    int NUM_RECORD_PER_COMMIT = 1000;
    
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    ExecutorService writerService = Executors.newFixedThreadPool(NUM_OF_WRITERS);
    for(int k = 0; k < NUM_OF_WRITERS; k++) {
      NStorageWriter writer = storage.getWriter("writer" + (k + 1));
      RandomCommitRollbackTRGenerator dataGenerator = 
          new RandomCommitRollbackTRGenerator(writer, NUM_OF_COMMIT, NUM_RECORD_PER_COMMIT).set1MBMaxSegmentSize();
      writerService.submit(dataGenerator);
    }
    writerService.shutdown();
    writerService.awaitTermination(10, TimeUnit.SECONDS);
    
    System.out.println("##Before close storage");
    storage.dump();
    System.out.println("\n\n");

    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    NStorageReader reader = storage.getReader("reader");
    TrackingRecordService trackingService = new TrackingRecordService(NUM_OF_COMMIT * NUM_RECORD_PER_COMMIT);
    byte[] record = null ;
    int    readCount = 0;
    while((record = reader.nextRecord(1000)) != null) {
      TrackingRecord trackingRecord = JSONSerializer.INSTANCE.fromBytes(record, TrackingRecord.class);
      trackingService.log(trackingRecord);
      readCount++ ;
      if(readCount % 500 == 0) {
        reader.prepareCommit();
        reader.completeCommit();
      }
    }
    reader.prepareCommit();
    reader.completeCommit();
    trackingService.report();
    
    System.out.println("##After close storage");
    storage.dump();
  }
}
