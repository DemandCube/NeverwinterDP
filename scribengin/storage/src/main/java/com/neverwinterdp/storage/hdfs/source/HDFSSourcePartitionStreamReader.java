package com.neverwinterdp.storage.hdfs.source;

import java.io.IOException;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMReader;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.source.CommitPoint;
import com.neverwinterdp.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourcePartitionStreamReader implements SourcePartitionStreamReader {
  private String                       name;
  private HdfsSSM                      partitionStorage;
  private StorageConfig                storageConfig;
  private PartitionStreamConfig        partitionConfig;
  private SSMReader                    partitionReader;

  public HDFSSourcePartitionStreamReader(String name, HdfsSSM pStorage, StorageConfig sConfig, PartitionStreamConfig pConfig) throws RegistryException, IOException {
    this.name             = name;
    this.partitionStorage = pStorage;
    this.storageConfig    = sConfig;
    this.partitionConfig  = pConfig ;
    partitionReader = pStorage.getReader(name);
  }
  
  @Override
  public String getName() { return name; }

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