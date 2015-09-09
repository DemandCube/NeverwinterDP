package com.neverwinterdp.scribengin.storage.hdfs;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSequenceFileUnitTest {
  private FileSystem fs ;
  private Configuration conf ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    conf = new Configuration();
    fs = FileSystem.getLocal(conf) ;
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testConcat() throws Exception {
    IntWritable key = new IntWritable();
    Text value = new Text();
    int count = 0;
    List<Path> seqPathHolder = new ArrayList<Path>();
    for(int j = 0; j < 3; j++) {
      Path path = new Path("./build/hdfs/seqfiles/test-file-" + j + ".seq");
      seqPathHolder.add(path);
      SequenceFile.Writer writer = createSequenceFileWriter(path, IntWritable.class, Text.class);
      for(int i = 0; i < 10; i++) {
        String line = "line " + count ;
        key.set(count);
        value.set(line);
        writer.append(key, value);
        count++ ;
      }
      writer.close();
    }
    
    Path[] allSeqPath = seqPathHolder.toArray(new Path[seqPathHolder.size()]);
    Path all = new Path("./build/hdfs/seqfiles/all.seq") ;
    HDFSUtil.concat(fs, all, allSeqPath, true);
  
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(all));
    while (reader.next(key, value)) {
      System.err.println(key + "\t" + value);
    }
    reader.close();
  }
  
  SequenceFile.Writer createSequenceFileWriter(Path path, Class<?> keyType, Class<?> valueType) throws Exception {
    SequenceFile.Writer.Option[] options = {
        Writer.file(path), 
        Writer.keyClass(keyType),
        Writer.valueClass(valueType), 
        Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
        Writer.replication(fs.getDefaultReplication(path)),
        Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
        Writer.progressable(null),
        Writer.metadata(new Metadata())
    };
    SequenceFile.Writer writer = SequenceFile.createWriter(conf, options);
    return writer;
  }
}
