package com.neverwinterdp.nstorage.segment.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HDFSSegmentStorageUnitTest {
  final static public String WORKING_DIR = "build/working";
  
  private EmbededZKServerSet zkCluster;
  private Registry           registry;
  private FileSystem         fs ;
  
  
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
  public void testStorage() throws Exception {
    HDFSSegmentStorage storage =  
      new HDFSSegmentStorage("tester", fs, WORKING_DIR + "/seg-storage", registry, "/seg-storage");
    HDFSSegmentWriter writer = (HDFSSegmentWriter)storage.newSegmentWriter();
    for(int i = 0; i < 10; i++) {
      writer.write("test".getBytes());
    }
    writer.prepareCommit();
    writer.completeCommit();
    
    writer.close();
    
    System.out.println("##Before close storage");
    storage.dump();
    System.out.println("\n\n");
    
    System.out.println("##After close storage");
    storage.close();
    storage.dump();
  }
}
