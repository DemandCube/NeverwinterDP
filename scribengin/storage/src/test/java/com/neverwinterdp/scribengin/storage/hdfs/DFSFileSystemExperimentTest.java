package com.neverwinterdp.scribengin.storage.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.io.IOUtil;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class DFSFileSystemExperimentTest {
  static String TEST_DIR = "/tmp/dfs-test" ;
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp");
    
    Configuration conf = new Configuration();
    conf.set("HADOOP_USER_NAME", "neverwinterdp");
    conf.set("fs.default.name", "hdfs://159.203.71.183:9000");
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
  public void testWritePos() throws Exception {
    String TEXT = "hello" ;
    byte[] data = TEXT.getBytes();
    Path testPath = new Path(TEST_DIR + "/test.txt"); 
    FSDataOutputStream os = fs.create(testPath) ;
    os.write(data);
    os.write(data);
    os.close();
    System.err.println("pass 1");
    
    fs.truncate(testPath, data.length);
    System.err.println("pass 2");
    
    FSDataInputStream is = fs.open(testPath);
    String text = IOUtil.getStreamContentAsString(is, "UTF-8");
    Assert.assertEquals(TEXT, text);
    System.err.println("pass 3");
  }
  
  @Test
  public void testConcat() throws Exception {
    Path[] path = new Path[10];
    for(int i = 0; i < path.length; i++) {
      path[i] = new Path("./build/hdfs/file-" + i + ".txt");
      String TEXT = "file content " + i ;
      FSDataOutputStream os = fs.create(path[i]) ;
      os.write(TEXT.getBytes());
      os.close();
    }
    
    Path concatPath = new Path("./build/hdfs/concat.txt");
    try {
      fs.concat(concatPath, path);
    } catch(UnsupportedOperationException ex) {
      //TODO
      System.err.println("TODO: test concat method with real HDFS");
    }
    
    HDFSUtil.concat(fs, concatPath, path);
  }
}