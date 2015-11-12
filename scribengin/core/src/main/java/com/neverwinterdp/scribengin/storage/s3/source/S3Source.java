package com.neverwinterdp.scribengin.storage.s3.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;

public class S3Source implements Source {
  private S3Client      s3Client;
  private StorageConfig storageConfig;
  
  public S3Source(S3Client s3Client, StorageConfig sConfig) {
    this.s3Client = s3Client;
    this.storageConfig = sConfig;
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  @Override
  public S3SourcePartition getLatestSourcePartition() throws Exception {
    List<String> partitionKeys = getPartitionNames();
    String latestPartitionName = partitionKeys.get(partitionKeys.size() -1);
    return new S3SourcePartition(s3Client, storageConfig, latestPartitionName);
  }

  @Override
  public List<S3SourcePartition> getSourcePartitions() throws Exception {
    List<String> partitionKeys = getPartitionNames();
    List<S3SourcePartition> holder = new ArrayList<>();
    for(int i = 0; i < partitionKeys.size(); i++) {
      holder.add(new S3SourcePartition(s3Client, storageConfig, partitionKeys.get(i)));
    }
    return holder;
  }
  
  List<String> getPartitionNames() throws Exception {
    String bucketName          = storageConfig.attribute(S3Storage.BUCKET_NAME);
    String storageFolderPath   = storageConfig.attribute(S3Storage.STORAGE_PATH);
    S3Folder storageFolder     = s3Client.getS3Folder(bucketName, storageFolderPath);
    List<String> childrenNames = storageFolder.getChildrenNames() ;
    Collections.sort(childrenNames);
    return childrenNames;
  }
}
