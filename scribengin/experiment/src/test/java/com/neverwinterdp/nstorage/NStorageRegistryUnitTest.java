package com.neverwinterdp.nstorage;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class NStorageRegistryUnitTest {
  final static public String WORKING_DIR = "build/working";
  
  private EmbededZKServerSet zkCluster;
  private Registry           registry;
  
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
  }
  
  @After
  public void teardown() throws Exception {
    registry.shutdown();
    zkCluster.shutdown();
  }
  
  @Test
  public void testSegment() throws Exception {
    int NUM_OF_SEGMENTS = 3;
    NStorageRegistry segStorageReg = new NStorageRegistry(registry, "/seg-storage");
    segStorageReg.initRegistry();
    NStorageWriterDescriptor writer = segStorageReg.createWriter("test");
    for(int i = 0; i < NUM_OF_SEGMENTS; i++) {
      SegmentDescriptor segment = segStorageReg.newSegment(writer);
    }
    
    List<String> segments = segStorageReg.getSegments();
    Assert.assertEquals(NUM_OF_SEGMENTS, segments.size());
    
    SegmentDescriptor segment0 = segStorageReg.getSegmentById(0);
    Assert.assertNotNull(segment0);
  
    NStorageRegistryPrinter rPrinter = new NStorageRegistryPrinter(System.out, segStorageReg);
    rPrinter.print();
  }
}
