package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.StorageReader;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourceStreamReader extends StorageReader<DataflowMessage> implements SourceStreamReader {
  public HDFSSourceStreamReader(String name, FileSystem fs, StreamDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    super(name, fs, descriptor.getLocation(), DataflowMessage.class);
  }
}