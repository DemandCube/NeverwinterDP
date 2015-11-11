package com.neverwinterdp.scribengin.storage.s3.source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

/**
 * @author Anthony Musyoki
 * 
 * For Consistency with S3Sink a Source is a folder in a bucket, a SourceStream is a file in the folder, 
 * a SourceStreamReader read the file
 */
public class S3SourcePartition implements SourcePartition {
  private S3Storage storage ;
  private S3Folder sourceFolder;
  private StorageConfig descriptor;
  private Map<Integer, S3SourcePartitionStream> streams = new LinkedHashMap<>();

  public S3SourcePartition(S3Client s3Client, StorageConfig sConfig) throws Exception {
    this.descriptor = sConfig;
    storage = new S3Storage(sConfig);
    sourceFolder = s3Client.getS3Folder(storage.getBucketName(), storage.getStorageFolder());
    List<S3ObjectSummary> streamNames = sourceFolder.getChildren();
    for (S3ObjectSummary streamName : streamNames) {
      PartitionStreamConfig pConfig = storage.createPartitionConfig(streamName.getKey());
      S3SourcePartitionStream stream = new S3SourcePartitionStream(s3Client, sConfig, pConfig);
      streams.put(stream.getDescriptor().getPartitionStreamId(), stream);
    }
  }

  public StorageConfig getStorageConfig() { return descriptor; }

  public SourcePartitionStream getPartitionStream(int id) { return streams.get(id); }

  public SourcePartitionStream getPartitionStream(PartitionStreamConfig descriptor) { return streams.get(descriptor.getPartitionStreamId()); }

  public SourcePartitionStream[] getPartitionStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  public void close() throws Exception {
  }
}
