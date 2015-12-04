package com.neverwinterdp.storage.simplehdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DFSFileSystemHsyncExperimentTest {
  static String TEST_DIR = "/tmp/dfs-test" ;
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp");
    
    Configuration conf = new Configuration();
    conf.set("HADOOP_USER_NAME", "neverwinterdp");
    conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
    fs = FileSystem.get(conf) ;
    if(fs.exists(new Path(TEST_DIR))) {
      fs.delete(new Path(TEST_DIR), true);
    }
    fs.mkdirs(new Path(TEST_DIR));
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testHsync() throws Exception {
    Path path = new Path(TEST_DIR + "/file.txt");
    FSDataOutputStream os = fs.create(path);
    FSDataInputStream  is = fs.open(path);
    byte[] data = new byte[1024];
    for(int i = 0 ; i < 1024; i++) {
      for(int j = 0; j < 128; j++) {
        os.write(data);
      }
      os.hsync();
      for(int j = 0; j < 128; j++) {
        is.readFully(data);
      }
    }
  }
}