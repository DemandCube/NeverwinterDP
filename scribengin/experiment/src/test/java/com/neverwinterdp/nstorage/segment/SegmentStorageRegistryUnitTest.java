package com.neverwinterdp.nstorage.segment;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class SegmentStorageRegistryUnitTest {
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
  public void testSegment() throws Exception {
    int NUM_OF_SEGMENTS = 3;
    SegmentRegistry segStorageReg = new SegmentRegistry(registry, "/seg-storage");
    segStorageReg.initRegistry();
    WriterDescriptor writer = segStorageReg.createWriter("test");
    for(int i = 0; i < NUM_OF_SEGMENTS; i++) {
      SegmentDescriptor segment = segStorageReg.newSegment(writer);
    }
    
    List<String> segments = segStorageReg.getSegments();
    Assert.assertEquals(NUM_OF_SEGMENTS, segments.size());
    
    SegmentDescriptor segment0 = segStorageReg.getSegmentById(0);
    Assert.assertNotNull(segment0);
  
    SegmentRegistryPrinter rPrinter = new SegmentRegistryPrinter(System.out, segStorageReg);
    rPrinter.print();
  }
}
