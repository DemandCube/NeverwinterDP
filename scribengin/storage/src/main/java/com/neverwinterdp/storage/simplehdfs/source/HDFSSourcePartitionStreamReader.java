package com.neverwinterdp.storage.simplehdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.simplehdfs.SegmentStorageReader;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartitionStreamReader extends SegmentStorageReader<Message> implements SourcePartitionStreamReader {
  public HDFSSourcePartitionStreamReader(String name, FileSystem fs, PartitionStreamConfig descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    super(name, fs, descriptor.getLocation(), Message.class);
  }
}