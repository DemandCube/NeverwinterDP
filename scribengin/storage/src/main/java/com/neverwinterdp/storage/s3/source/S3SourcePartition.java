package com.neverwinterdp.storage.s3.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.s3.S3Folder;
import com.neverwinterdp.storage.s3.S3Storage;
import com.neverwinterdp.storage.source.SourcePartition;

/**
 * @author Tuan Nguyen
 */
public class S3SourcePartition implements SourcePartition {
  private S3Client      s3Client;
  private StorageConfig storageConfig;
  private String        partitionName;

  public S3SourcePartition(S3Client s3Client, StorageConfig sConfig, String partitionName) throws Exception {
    this.s3Client       = s3Client;
    this.storageConfig  = sConfig;
    this.partitionName  = partitionName;
  }

  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  public String getPartitionName() { return this.partitionName ; }
 
  @Override
  public S3SourcePartitionStream getPartitionStream(int partitionStreamId) { 
    PartitionStreamConfig streamConfig = new PartitionStreamConfig(partitionStreamId, null);
    return new S3SourcePartitionStream(s3Client, storageConfig, partitionName, streamConfig);
  }

  @Override
  public S3SourcePartitionStream getPartitionStream(PartitionStreamConfig streamConfig) { 
    return new S3SourcePartitionStream(s3Client, storageConfig, partitionName, streamConfig);
  }

  @Override
  public S3SourcePartitionStream[] getPartitionStreams() {
    String bucketName    = storageConfig.attribute(S3Storage.BUCKET_NAME);
    String storagePath   = storageConfig.attribute(S3Storage.STORAGE_PATH);
    String partitionPath = storagePath + "/" + partitionName;
    S3Folder pFolder = s3Client.getS3Folder(bucketName, partitionPath);
    List<String> streamNames = pFolder.getChildrenNames();
    S3SourcePartitionStream[] stream = new S3SourcePartitionStream[streamNames.size()];
    for(int i = 0; i < streamNames.size(); i++) {
      String streamName = streamNames.get(i);
      int dashIdx = streamName.lastIndexOf('-');
      int id = Integer.parseInt(streamName.substring(dashIdx + 1)) ;
      stream[i] = getPartitionStream(id);
    }
    Arrays.sort(stream, S3SourcePartitionStream.COMPARATOR);
    return stream;
  }

  public void delete() throws Exception {
    String bucketName  = storageConfig.attribute(S3Storage.BUCKET_NAME);
    String storagePath = storageConfig.attribute(S3Storage.STORAGE_PATH);
    String partitionPath = storagePath + "/" + partitionName;
    s3Client.deleteS3Folder(bucketName, partitionPath);
  }
  
  public void close() throws Exception {
  }
}
