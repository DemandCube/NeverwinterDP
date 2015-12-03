package com.neverwinterdp.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HDFSStorageUnitTest {
  final static public String WORKING_DIR  = "build/working";
  final static public String HDFS_DIR     = WORKING_DIR + "/hdfs";
  
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
  public void testHDFSStorage() throws Exception {
    StorageConfig storageConfig = new StorageConfig("hdfs", HDFS_DIR);
    storageConfig.attribute(HDFSStorage.REGISTRY_PATH, "/storage/hdfs/test");
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(5);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    registry.get("/").dump(System.out);
  }
}
