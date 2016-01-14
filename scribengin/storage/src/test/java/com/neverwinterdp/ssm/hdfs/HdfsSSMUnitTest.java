package com.neverwinterdp.ssm.hdfs;

import java.io.IOException;
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

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMConsistencyVerifier;
import com.neverwinterdp.ssm.SSMReader;
import com.neverwinterdp.ssm.SSMWriter;
import com.neverwinterdp.ssm.SegmentConsistency;
import com.neverwinterdp.ssm.SSMTagDescriptor;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.ssm.test.TrackingRecordGenerator;
import com.neverwinterdp.ssm.test.TrackingRecordValidator;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HdfsSSMUnitTest {
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
    HdfsSSM storage = new HdfsSSM(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    TrackingRecordGenerator storageWriter = 
      new TrackingRecordGenerator(storage.getWriter("test"), NUM_OF_COMMIT, NUM_OF_RECORD_PER_COMMIT);
    storageWriter.writeWithCommit();
    storage.getRegistry().doManagement();
    storageWriter.writerCloseAndRemove();
    
    SSMConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    storage.getRegistry().doManagement();
    
    SSMTagDescriptor  posTag = storage.getRegistry().findTagByRecordPosition(500);
    posTag.setName("tag-500");
    storage.getRegistry().createTag(posTag);
    storage.dump();
    
    SSMReader reader = storage.getReader("reader");
    TrackingRecordValidator validator = new TrackingRecordValidator(reader, NUM_OF_RECORDS, 500).setRandomRollbackRatio(0);
    validator.run();
    validator.report();
    reader.closeAndRemove();
    storage.cleanReadSegmentByActiveReader();
    storage.dump();
  }
  
  @Test
  public void testRollback() throws Exception {
    int NUM_OF_COMMIT = 1;
    int NUM_OF_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS = NUM_OF_COMMIT * NUM_OF_RECORD_PER_COMMIT;
    
    HdfsSSM storage = new HdfsSSM(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    TrackingRecordGenerator storageWriter = 
      new TrackingRecordGenerator(storage.getWriter("test"), NUM_OF_COMMIT, NUM_OF_RECORD_PER_COMMIT);
    storageWriter.writeWithCommit();
    SSMConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
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
    
    SSMReader reader = storage.getReader("reader");
    TrackingRecordValidator validator = new TrackingRecordValidator(reader, NUM_OF_RECORDS, 100);
    validator.run();
    validator.report();
    storage.dump();
  }
  

  @Test
  public void testConcurrentReadWrite() throws Exception {
    final int NUM_OF_COMMIT = 10;
    final HdfsSSM storage = new HdfsSSM(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    Thread writer = new Thread() {
      public void run() {
        try {
          SSMWriter ssmWriter = storage.getWriter("writer");
          for(int j = 1; j <= NUM_OF_COMMIT; j++) {
            for(int i = 0; i < 1000; i++) {
              byte[] data = ("commit = " + j + ", record = " + i).getBytes();
              ssmWriter.write(data);
            }
            ssmWriter.commit();
            System.err.println("Write 1000");
            Thread.sleep(1000);
          }
          ssmWriter.close();
        } catch (RegistryException | IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    writer.start();
    
    Thread reader = new Thread() {
      public void run() {
        try {
          SSMReader ssmReader = storage.getReader("reader");
          int count = 0;
          byte[] data = null;
          while((data = ssmReader.nextRecord(2000)) != null) {
            count++;
            if(count % 1000 == 0) {
              ssmReader.prepareCommit();
              ssmReader.completeCommit();
              System.err.println("Read count = " + count);
            }
          }
          ssmReader.close();
        } catch(RegistryException | IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    reader.start();
    reader.join() ;
  }
  
  @Test
  public void testConcurrentMultipleReadWrite() throws Exception {
    int NUM_OF_WRITERS = 5;
    int NUM_OF_COMMIT  = 1000;
    int NUM_RECORD_PER_COMMIT = 1000;
    int NUM_OF_RECORDS_PER_WRITER = NUM_OF_COMMIT * NUM_RECORD_PER_COMMIT;
    
    int NUM_OF_READERS = 5;
    
    HdfsSSM storage = new HdfsSSM(fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    
    long start = System.currentTimeMillis();
    ExecutorService writerService = Executors.newFixedThreadPool(NUM_OF_WRITERS);
    for(int i = 0; i < NUM_OF_WRITERS; i++) {
      SSMWriter writer = storage.getWriter("writer" + (i + 1));
      TrackingRecordGenerator dataGenerator = 
          new TrackingRecordGenerator(writer, NUM_OF_COMMIT, NUM_RECORD_PER_COMMIT);
      dataGenerator.set1MBMaxSegmentSize();
      dataGenerator.setRandomRollbackRatio(0.25);
      writerService.submit(dataGenerator);
    }
    writerService.shutdown();
    Thread.sleep(500);
    
    ExecutorService readerService = Executors.newFixedThreadPool(NUM_OF_READERS);
    TrackingRecordValidator[] validator = new TrackingRecordValidator[NUM_OF_READERS];
    for(int i = 0; i < NUM_OF_READERS; i++) {
      SSMReader reader = storage.getReader("reader-" + (i + 1));
      validator[i] = new TrackingRecordValidator(reader, NUM_OF_RECORDS_PER_WRITER, 500);
      validator[i].setRandomRollbackRatio(0.25);
      readerService.submit(validator[i]);
    }
    readerService.shutdown();
    
    System.err.println("before writerService.awaitTermination:" + (System.currentTimeMillis() - start) + "ms");

    while(!writerService.awaitTermination(3, TimeUnit.SECONDS)) {
      storage.cleanReadSegmentByActiveReader();
    }
    System.err.println("after writerService.awaitTermination:"  + (System.currentTimeMillis() - start) + "ms");
    SSMConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    for(int i = 0; i < NUM_OF_READERS; i++) {
      validator[i].report();
    }
    
    System.err.println("before readerService.awaitTermination: " + (System.currentTimeMillis() - start) + "ms");
    while(!readerService.awaitTermination(3, TimeUnit.SECONDS)) {
      storage.cleanReadSegmentByActiveReader();
    }
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
