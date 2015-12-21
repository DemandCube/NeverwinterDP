package com.neverwinterdp.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.storage.hdfs.sink.HDFSSinkPartitionStream;
import com.neverwinterdp.storage.hdfs.sink.HDFSSinkPartitionStreamWriter;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.storage.source.SourcePartitionStream;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
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
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("test", "/storage/hdfs/test", HDFS_DIR);
    storageConfig.setReplication(2);
    storageConfig.setPartitionStream(5);
    HDFSStorage storage = new HDFSStorage(registry,fs, storageConfig);
    storage.create();
    registry.get("/").dump(System.out);
    
    HDFSSink sink = storage.getSink();
    HDFSSinkPartitionStream[] sinkStream = sink.getPartitionStreams() ;
    Assert.assertEquals(5, sinkStream.length);
    for(int i = 0; i < sinkStream.length; i++) {
      HDFSSinkPartitionStreamWriter writer = sinkStream[i].getWriter();
      for(int j = 0; j < 1000; j++) {
        writer.append(Message.create("key-" + j, "record " + j));
      }
      writer.commit();
      writer.close();
    }
    
    HDFSSource source = storage.getSource();
    SourcePartitionStream[] sourceStream = source.getLatestSourcePartition().getPartitionStreams();
    for(int i = 0; i < sourceStream.length; i++) {
      SourcePartitionStreamReader reader = sourceStream[i].getReader("reader-for-stream-" + i);
      Message message = null;
      int count = 0;
      while((message = reader.next(3000)) != null) {
        count++;
      }
      System.out.println(reader.getName() + ": " + count);
    }
  }
}
