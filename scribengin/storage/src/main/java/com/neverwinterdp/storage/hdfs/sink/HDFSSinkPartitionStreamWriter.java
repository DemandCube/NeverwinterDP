package com.neverwinterdp.storage.hdfs.sink;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SSMWriter;
import com.neverwinterdp.ssm.hdfs.HdfsSSM;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Record;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class HDFSSinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  private String                name;
  private HdfsSSM               partitionStorage;
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionConfig;
  private SSMWriter             writer;
  
  public HDFSSinkPartitionStreamWriter(String name, HdfsSSM pStorage, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException, RegistryException {
    this.name = name;
    this.partitionStorage = pStorage;
    this.storageConfig    = sConfig;
    this.partitionConfig  = pConfig ;
    writer = partitionStorage.getWriter(name);
  }
  
  public PartitionStreamConfig getPartitionConfig() { return partitionConfig; }

  @Override
  public void append(Record record) throws Exception {
    writer.write(JSONSerializer.INSTANCE.toBytes(record));
  }

  @Override
  public void commit() throws Exception {
    writer.prepareCommit();
    writer.completeCommit();
  }

  @Override
  public void close() throws Exception {
    writer.closeAndRemove();
  }

  @Override
  public void rollback() throws Exception {
    writer.rollback();
  }

  @Override
  public void prepareCommit() throws Exception {
    writer.prepareCommit();
  }

  @Override
  public void completeCommit() throws Exception {
    writer.completeCommit();
  }
}