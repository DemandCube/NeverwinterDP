package com.neverwinterdp.scribengin.storage.s3.sink;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;

public class S3Sink implements Sink {
  private StorageConfig storageConfig;
  private S3Storage     storage;
  private S3Client      s3Client;
  private S3Folder      sinkFolder;

  public S3Sink(StorageConfig descriptor) {
    this.storageConfig = descriptor;
    this.storage = new S3Storage(descriptor);
    this.s3Client = storage.getS3Client();
    init();
  }

  public S3Sink(S3Client s3Client, StorageConfig descriptor) {
    this.storage = new S3Storage(descriptor);
    this.s3Client = s3Client;
    init();
  }
  
  private void init() {
    String bucketName = storage.getBucketName();
    String storageFolder = storage.getStorageFolder();
    if (!s3Client.hasKey(bucketName, storageFolder)) {
      sinkFolder = s3Client.createS3Folder(bucketName, storageFolder);
    } else {
      sinkFolder = s3Client.getS3Folder(bucketName, storageFolder);
    }
  }

  public S3Folder getSinkFolder() { return this.sinkFolder; }

  @Override
  public StorageConfig getDescriptor() { return storage.getStorageDescriptor(); }

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
  synchronized public SinkPartitionStream getPartitionStream(PartitionStreamConfig pconfig) throws Exception {
    S3SinkPartitionStream stream = new S3SinkPartitionStream(sinkFolder, storage.getStorageDescriptor(), pconfig);
    return stream;
  }

  @Override
  synchronized public SinkPartitionStream getParitionStream(int partitionId) throws Exception {
    PartitionStreamConfig pConfig = storage.createPartitionConfig(partitionId);
    S3SinkPartitionStream stream = new S3SinkPartitionStream(sinkFolder, storage.getStorageDescriptor(), pConfig);
    return stream;
  }

  
  @Override
  synchronized public SinkPartitionStream[] getPartitionStreams() throws Exception {
    int numOfPartitionStream = storage.getStorageDescriptor().getPartitionStream();
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
