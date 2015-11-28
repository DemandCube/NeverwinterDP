package com.neverwinterdp.nstorage.segment.hdfs;

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

import com.neverwinterdp.nstorage.segment.SegmentConsistency;
import com.neverwinterdp.nstorage.segment.SegmentConsistencyVerifier;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HDFSSegmentStorageUnitTest {
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
    fs = FileSystem.getLocal(new Configuration());
  }
  
  @After
  public void teardown() throws Exception {
    registry.shutdown();
    zkCluster.shutdown();
  }
  
  @Test
  public void testCommit() throws Exception {
    HDFSSegmentStorage storage =  
        new HDFSSegmentStorage("writer", fs, WORKING_DIR + "/" + STORAGE_NAME, registry, "/" + STORAGE_NAME);
    HDFSSegmentWriter writer = (HDFSSegmentWriter) storage.newSegmentWriter();

    for(int j = 1; j <= 10; j++) {
      String message = "record " + j ;
      writer.write(message.getBytes());
    }
    writer.commit();
    
    writer.close();
    storage.close();
    
    SegmentConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    storage.close();
  }
  
  @Test
  public void testRollback() throws Exception {
    HDFSSegmentStorage storage =  
        new HDFSSegmentStorage("writer", fs, WORKING_DIR + "/" + STORAGE_NAME, registry, "/" + STORAGE_NAME);
    HDFSSegmentWriter writer = (HDFSSegmentWriter) storage.newSegmentWriter();

    for(int j = 1; j <= 10; j++) {
      String message = "record " + j ;
      writer.write(message.getBytes());
    }
    writer.commit();
    
    for(int j = 1; j <= 5; j++) {
      String message = "record " + j ;
      writer.write(message.getBytes());
    }
    writer.rollback();
    
    for(int j = 1; j <= 10; j++) {
      String message = "record " + j ;
      writer.write(message.getBytes());
    }
    writer.commit();
    
    
    writer.close();
    storage.close();
    
    SegmentConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    storage.close();
  }
  
  @Test
  public void testMultipleWriter() throws Exception {
    int NUM_OF_SEGMENTS = 3;
    HDFSSegmentStorage storage =  
        new HDFSSegmentStorage("verifier", fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    
    ExecutorService writerService = Executors.newFixedThreadPool(NUM_OF_SEGMENTS);
    for(int k = 0; k < NUM_OF_SEGMENTS; k++) {
      StorageWriter sWriter = new StorageWriter("writer-" + (k + 1), 10, 100);
      writerService.submit(sWriter);
    }
    writerService.shutdown();
    writerService.awaitTermination(10, TimeUnit.SECONDS);
    
    System.out.println("##Before close storage");
    storage.dump();
    System.out.println("\n\n");

    SegmentConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    System.out.println("##After close storage");
    storage.close();
    storage.dump();
  }
  
  public class StorageWriter implements Runnable {
    String name ;
    int    numOfRecordPerCommit = 10;
    int    numOfCommit          = 10;
    
    StorageWriter(String name, int numOfCommit, int numOfRecordPerCommit) {
      this.name                 = name;
      this.numOfCommit          = numOfCommit;
      this.numOfRecordPerCommit = numOfRecordPerCommit;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (RegistryException | IOException e) {
        e.printStackTrace();
      }
    }
    
    void doRun() throws RegistryException, IOException {
      HDFSSegmentStorage storage = new HDFSSegmentStorage(name, fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
      HDFSSegmentWriter writer = (HDFSSegmentWriter) storage.newSegmentWriter();
      for(int i = 1; i <= numOfCommit; i++) {
        for(int j = 1; j <= numOfRecordPerCommit; j++) {
          String message = "commit " + i + ", record " + j ;
          writer.write(message.getBytes());
        }
        writer.prepareCommit();
        writer.completeCommit();
      }
      writer.close();
      storage.close();
    }
  }
}
