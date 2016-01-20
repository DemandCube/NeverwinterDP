package com.neverwinterdp.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.storage.hdfs.sink.HDFSSinkPartitionStream;
import com.neverwinterdp.storage.hdfs.sink.HDFSSinkPartitionStreamWriter;
import com.neverwinterdp.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStream;
import com.neverwinterdp.storage.hdfs.source.HDFSSourcePartitionStreamReader;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServerSet;

abstract public class HDFSStorageTest {
  final static public String WORKING_DIR  = "build/working";
  final static public String HDFS_DIR     = WORKING_DIR + "/hdfs";
  
  protected EmbededZKServerSet zkCluster;
  protected Registry           registry;
  protected FileSystem         fs ;
  
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
  
  protected void write(HDFSStorage storage, int numOfMessagePerPartition) throws Exception {
    HDFSSink sink = storage.getSink();
    HDFSSinkPartitionStream[] sinkStream = sink.getPartitionStreams() ;
    Assert.assertEquals(5, sinkStream.length);
    for(int i = 0; i < sinkStream.length; i++) {
      HDFSSinkPartitionStreamWriter writer = sinkStream[i].getWriter();
      for(int j = 0; j < numOfMessagePerPartition; j++) {
        writer.append(Message.create("key-" + j, "record " + j));
      }
      writer.commit();
      writer.close();
    }
  }
  
  protected int count(HDFSStorage storage) throws Exception {
    return count(storage, null);
  }
  
  protected int count(HDFSStorage storage, HDFSStorageTag fromTag) throws Exception {
    int totalCount = 0;
    HDFSSource source = storage.getSource();
    HDFSSourcePartitionStream[] sourceStream = source.getLatestSourcePartition().getPartitionStreams();
    for(int i = 0; i < sourceStream.length; i++) {
      HDFSSourcePartitionStreamReader reader = null;
      if(fromTag == null) reader = sourceStream[i].getReader("reader-for-stream-" + i);
      else reader = sourceStream[i].getReader("reader-for-stream-" + i, fromTag);
      Message message = null;
      int count = 0;
      while((message = reader.next(1000)) != null) {
        count++;
      }
      totalCount += count;
    }
    return totalCount;
  }
}
