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
import com.neverwinterdp.nstorage.test.TrackingRecordGenerator;
import com.neverwinterdp.nstorage.test.TrackingRecordValidator;
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
    int NUM_OF_COMMIT = 1;
    int NUM_OF_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS = NUM_OF_COMMIT * NUM_OF_RECORD_PER_COMMIT;
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    TrackingRecordGenerator storageWriter = 
      new TrackingRecordGenerator(storage.getWriter("test"), NUM_OF_COMMIT, NUM_OF_RECORD_PER_COMMIT);
    storageWriter.writeWithCommit();
    storageWriter.writerCloseAndRemove();
    
    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    NStorageReader reader = storage.getReader("reader");
    TrackingRecordValidator validator = new TrackingRecordValidator(reader, NUM_OF_RECORDS, 500);
    validator.run();
    validator.report();
    //storage.dump();
  }
  
  @Test
  public void testRollback() throws Exception {
    int NUM_OF_COMMIT = 1;
    int NUM_OF_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS = NUM_OF_COMMIT * NUM_OF_RECORD_PER_COMMIT;
    
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    TrackingRecordGenerator storageWriter = 
      new TrackingRecordGenerator(storage.getWriter("test"), NUM_OF_COMMIT, NUM_OF_RECORD_PER_COMMIT);
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
    TrackingRecordValidator validator = new TrackingRecordValidator(reader, NUM_OF_RECORDS, 100);
    validator.run();
    validator.report();
    storage.dump();
  }
  
  @Test
  public void testConcurrentMultipleReadWrite() throws Exception {
    int NUM_OF_WRITERS = 5;
    int NUM_OF_COMMIT  = 1000;
    int NUM_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS_PER_WRITER = NUM_OF_COMMIT * NUM_RECORD_PER_COMMIT;
    
    int NUM_OF_READERS = 5;
    
    HDFSNStorage storage = new HDFSNStorage(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    
    long start = System.currentTimeMillis();
    ExecutorService writerService = Executors.newFixedThreadPool(NUM_OF_WRITERS);
    for(int i = 0; i < NUM_OF_WRITERS; i++) {
      NStorageWriter writer = storage.getWriter("writer" + (i + 1));
      TrackingRecordGenerator dataGenerator = 
          new TrackingRecordGenerator(writer, NUM_OF_COMMIT, NUM_RECORD_PER_COMMIT);
      dataGenerator.set25MBMaxSegmentSize();
      dataGenerator.setRandomRollbackRatio(0.25);
      writerService.submit(dataGenerator);
    }
    writerService.shutdown();
    Thread.sleep(500);
    
    ExecutorService readerService = Executors.newFixedThreadPool(NUM_OF_READERS);
    TrackingRecordValidator[] validator = new TrackingRecordValidator[NUM_OF_READERS];
    for(int i = 0; i < NUM_OF_READERS; i++) {
      NStorageReader reader = storage.getReader("reader-" + (i + 1));
      validator[i] = new TrackingRecordValidator(reader, NUM_OF_RECORDS_PER_WRITER, 500);
      validator[i].setRandomRollbackRatio(0.25);
      readerService.submit(validator[i]);
    }
    readerService.shutdown();
    
    System.err.println("before writerService.awaitTermination:" + (System.currentTimeMillis() - start) + "ms");
    writerService.awaitTermination(90, TimeUnit.SECONDS);
    System.err.println("after writerService.awaitTermination:"  + (System.currentTimeMillis() - start) + "ms");
    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    for(int i = 0; i < NUM_OF_READERS; i++) {
      validator[i].report();
    }
    
    System.err.println("before readerService.awaitTermination: " + (System.currentTimeMillis() - start) + "ms");
    readerService.awaitTermination(90, TimeUnit.SECONDS);
    System.err.println("after readerService.awaitTermination:"  + (System.currentTimeMillis() - start) + "ms");
    
    for(int i = 0; i < NUM_OF_READERS; i++) {
      validator[i].report();
    }
    
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    storage.dump();
  }
}
