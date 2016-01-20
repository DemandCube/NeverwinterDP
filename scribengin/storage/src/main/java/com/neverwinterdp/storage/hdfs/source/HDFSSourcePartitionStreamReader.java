package com.neverwinterdp.storage.hdfs.source;

import java.io.IOException;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.hdfs.HdfsSSMReader;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.source.CommitPoint;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartitionStreamReader implements SourcePartitionStreamReader {
  private String                name;
  private PartitionStreamConfig partitionConfig;
  private HdfsSSMReader         partitionReader;

  public HDFSSourcePartitionStreamReader(String name, HdfsSSMReader partitionReader, PartitionStreamConfig pConfig) throws RegistryException, IOException {
    this.name             = name;
    this.partitionConfig  = pConfig ;
    this.partitionReader  = partitionReader;
  }
  
  @Override
  public String getName() { return name; }
  
  public PartitionStreamConfig getPartitionStreamConfig() { return partitionConfig; }
  
  @Override
  public Message next(long maxWait) throws Exception {
    byte[] data = partitionReader.nextRecord(maxWait);
    if(data == null) {
      return null;
    }
    return JSONSerializer.INSTANCE.fromBytes(data, Message.class);
  }

  @Override
  public Message[] next(int size, long maxWait) throws Exception {
    throw new Exception("TODO: implement this method");
  }

  @Override
  public boolean isEndOfDataStream() throws Exception {
    throw new Exception("TODO: implement this method");
  }

  @Override
  public void rollback() throws Exception {
    partitionReader.rollback();
  }

  @Override
  public void prepareCommit() throws Exception {
    partitionReader.prepareCommit();
  }

  @Override
  public void completeCommit() throws Exception {
    partitionReader.completeCommit();
  }

  @Override
  public void commit() throws Exception {
    partitionReader.prepareCommit();
    partitionReader.completeCommit();
  }

  @Override
  public CommitPoint getLastCommitInfo() {
    return null;
  }

  @Override
  public void close() throws Exception {
    partitionReader.close();
  }
}