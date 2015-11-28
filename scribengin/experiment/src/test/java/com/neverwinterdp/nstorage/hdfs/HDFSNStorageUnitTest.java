package com.neverwinterdp.nstorage.hdfs;

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

import com.neverwinterdp.nstorage.SegmentConsistency;
import com.neverwinterdp.nstorage.NStorageConsistencyVerifier;
import com.neverwinterdp.nstorage.hdfs.HDFSNStorage;
import com.neverwinterdp.nstorage.hdfs.HDFSSegmentWriter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
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
    StorageWriter storageWriter = new StorageWriter("writer", 1, 1000);
    storageWriter.writeWithCommit();
    storageWriter.close();
    
    NStorageConsistencyVerifier scVerifier = storageWriter.storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
  }
  
  @Test
  public void testRollback() throws Exception {
    StorageWriter storageWriter = new StorageWriter("writer", 1, 1000);
    storageWriter.writeWithCommit();
    NStorageConsistencyVerifier scVerifier = storageWriter.storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.OK, scVerifier.getMinCommitConsistency());

    storageWriter.writeWithRollback();
    storageWriter.close();
    
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
  }
  
  @Test
  public void testMultipleWriter() throws Exception {
    int NUM_OF_SEGMENTS = 3;
    
    ExecutorService writerService = Executors.newFixedThreadPool(NUM_OF_SEGMENTS);
    for(int k = 0; k < NUM_OF_SEGMENTS; k++) {
      StorageWriter sWriter = new StorageWriter("writer-" + (k + 1), 10, 1000);
      writerService.submit(sWriter);
    }
    writerService.shutdown();
    writerService.awaitTermination(10, TimeUnit.SECONDS);
    
    HDFSNStorage storage =  
        new HDFSNStorage("verifier", fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    System.out.println("##Before close storage");
    storage.dump();
    System.out.println("\n\n");

    NStorageConsistencyVerifier scVerifier = storage.getSegmentConsistencyVerifier();
    scVerifier.verify();
    System.out.println(scVerifier.getSegmentDescriptorTextReport());
    System.out.println(scVerifier.getSegmentConsistencyTextReport());
    Assert.assertEquals(SegmentConsistency.Consistency.GOOD, scVerifier.getMinCommitConsistency());
    
    System.out.println("##After close storage");
    storage.close();
    storage.dump();
  }
  
  public class StorageWriter implements Runnable {
    String name ;
    int    numOfRecordPerCommit = 10;
    int    numOfCommit          = 10;
    HDFSNStorage storage;
    HDFSSegmentWriter writer;
    
    StorageWriter(String name, int numOfCommit, int numOfRecordPerCommit) throws RegistryException, IOException {
      this.name                 = name;
      this.numOfCommit          = numOfCommit;
      this.numOfRecordPerCommit = numOfRecordPerCommit;
      storage = new HDFSNStorage(name, fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
      writer = (HDFSSegmentWriter) storage.newSegmentWriter();
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
      for(int i = 1; i <= numOfCommit; i++) {
        if(i % 2 == 0) {
          writeWithCommit();
        } else {
          writeWithRollback();
        }
      }
      writer.close();
      storage.close();
    }
    
    public void writeWithCommit() throws RegistryException, IOException {
      for(int j = 1; j <= numOfRecordPerCommit; j++) {
        String message = "writer " + name + ", record " + j ;
        writer.write(message.getBytes());
      }
      writer.prepareCommit();
      writer.completeCommit();
    }
    
    public void writeWithRollback() throws RegistryException, IOException {
      for(int j = 1; j <= numOfRecordPerCommit; j++) {
        String message = "writer " + name + ", record " + j ;
        writer.write(message.getBytes());
      }
      writer.rollback();
    }
    
    public void closeWriter() throws IOException, RegistryException {
      writer.close();
    }
    
    public void close() throws IOException, RegistryException {
      writer.close();
      storage.close();
    }
  }
}
