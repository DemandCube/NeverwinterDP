package com.neverwinterdp.ssm;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.ssm.SSMReaderDescriptor;
import com.neverwinterdp.ssm.SSMRegistry;
import com.neverwinterdp.ssm.SSMRegistryPrinter;
import com.neverwinterdp.ssm.SSMWriterDescriptor;
import com.neverwinterdp.ssm.SegmentDescriptor;
import com.neverwinterdp.ssm.SegmentReadDescriptor;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class SSMRegistryUnitTest {
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
    SSMRegistry segStorageReg = new SSMRegistry(registry, "/seg-storage");
    segStorageReg.initRegistry();
    SSMWriterDescriptor writer = segStorageReg.createWriter("test");
    for(int i = 0; i < NUM_OF_SEGMENTS; i++) {
      SegmentDescriptor segment = segStorageReg.newSegment(writer);
    }
    
    List<String> segments = segStorageReg.getSegments();
    Assert.assertEquals(NUM_OF_SEGMENTS, segments.size());
    
    SegmentDescriptor segment0 = segStorageReg.getSegmentById(0);
    Assert.assertNotNull(segment0);
  
    SSMRegistryPrinter rPrinter = new SSMRegistryPrinter(System.out, segStorageReg);
    rPrinter.print();
  }
  
  @Test
  public void testReader() throws Exception {
    SSMRegistry storageRegistry = new SSMRegistry(registry, "/seg-storage");
    storageRegistry.initRegistry();
    SSMWriterDescriptor writer = storageRegistry.createWriter("writer-1");
    SegmentDescriptor segment0 = storageRegistry.newSegment(writer);
    SegmentDescriptor segment1 = storageRegistry.newSegment(writer);

    SSMReaderDescriptor reader = storageRegistry.getOrCreateReader("reader-1");
    SegmentReadDescriptor segment0ReadDescriptor = storageRegistry.createSegmentReadDescriptor(reader, segment0);
    SegmentReadDescriptor segment1ReadDescriptor = storageRegistry.createSegmentReadDescriptor(reader, segment1);
    
    SSMRegistryPrinter rPrinter = new SSMRegistryPrinter(System.out, storageRegistry);
    rPrinter.print();
  }
}
