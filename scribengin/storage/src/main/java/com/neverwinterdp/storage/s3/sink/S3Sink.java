package com.neverwinterdp.storage.s3.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.sink.Sink;
import com.neverwinterdp.storage.sink.SinkPartitionStream;

public class S3Sink implements Sink {
  private S3Client      s3Client ;
  private StorageConfig storageConfig;

  public S3Sink(S3Client s3Client, StorageConfig sConfig) {
    this.s3Client = s3Client;
    this.storageConfig  = sConfig;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  public List<PartitionStreamConfig> getPartitionStreamConfigs() throws Exception {
    int numOfPartitionStream = storageConfig.getPartitionStream();
    List<PartitionStreamConfig> holder = new ArrayList<>();
    for(int i = 0; i < numOfPartitionStream; i++) {
      PartitionStreamConfig config = new PartitionStreamConfig(i, null);
      holder.add(config);
    }
    return holder;
  }

  @Override
  synchronized public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    PartitionStreamConfig streamConfig = new PartitionStreamConfig(partitionId, null) ;
    S3SinkPartitionStream stream = new S3SinkPartitionStream(s3Client, storageConfig, streamConfig);
    return stream;
  }
  
  @Override
  synchronized public SinkPartitionStream getPartitionStream(PartitionStreamConfig pconfig) throws Exception {
    S3SinkPartitionStream stream = new S3SinkPartitionStream(s3Client, storageConfig, pconfig);
    return stream;
  }

  @Override
  synchronized public SinkPartitionStream[] getPartitionStreams() throws Exception {
    int numOfPartitionStream = storageConfig.getPartitionStream();
    SinkPartitionStream[] array = new SinkPartitionStream[numOfPartitionStream];
    for(int i = 0; i < numOfPartitionStream; i++) {
      array[i] = getParitionStream(i);
    }
    return array;
  }

  @Override
  public void close() throws Exception {
  }
}
