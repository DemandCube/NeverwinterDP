package com.neverwinterdp.hqueue;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.nstorage.NStorageConfig;
import com.neverwinterdp.nstorage.NStorageContext;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

public class HQueueUnitTest {
  private EmbededZKServerSet zkCluster;
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/hqueue", false);
    zkCluster = new EmbededZKServerSet("build/hqueue/zookeeper", 2181, 1);
    zkCluster.start();
    
    fs = FileSystem.getLocal(new Configuration()) ;
    Path path = new Path("build/hqueue/fs");
  }
  
  @After
  public void teardown() throws Exception {
    zkCluster.shutdown();
  }
  
  @Test
  public void testHQueue() throws Exception {
    NStorageConfig<Message> hqueue = 
      new NStorageConfig<Message>("test", "/hqueue/test", "build/hqueue/test", Message.class);
    hqueue.setNumOfPartition(5);
    Registry registry = newRegistry() ;
    
    NStorageContext<Message> hqueueContext = new NStorageContext<>(registry, hqueue);
    Assert.assertEquals(5, hqueueContext.getPartitions().size());
    
    registry.get("/").dump(System.out);
  }
  
  private Registry newRegistry() throws Exception {
    RegistryConfig config = RegistryConfig.getDefault();
    return config.newInstance().connect() ;
  }
}
