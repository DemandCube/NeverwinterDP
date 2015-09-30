package com.neverwinterdp.scribengin.storage.s3.source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

/**
 * @author Anthony Musyoki
 * 
 * For Consistency with S3Sink a Source is a folder in a bucket, a SourceStream is a file in the folder, 
 * a SourceStreamReader read the file
 */
public class S3Source implements Source {
  private S3Storage storage ;
  private S3Folder sourceFolder;
  private StorageDescriptor descriptor;
  private Map<Integer, S3SourcePartitionStream> streams = new LinkedHashMap<>();

  public S3Source(S3Client s3Client, PartitionDescriptor streamDescriptor) throws Exception {
    this(s3Client, getSourceDescriptor(streamDescriptor));
  }

  public S3Source(S3Client s3Client, StorageDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    storage = new S3Storage(descriptor);
    sourceFolder = s3Client.getS3Folder(storage.getBucketName(), storage.getStorageFolder());
    List<S3ObjectSummary> streamNames = sourceFolder.getChildren();
    for (S3ObjectSummary streamName : streamNames) {
      PartitionDescriptor streamDescriptor = storage.createStreamDescriptor(streamName.getKey());
      S3SourcePartitionStream stream = new S3SourcePartitionStream(s3Client, streamDescriptor);
      streams.put(stream.getDescriptor().getId(), stream);
    }
  }

  public StorageDescriptor getDescriptor() { return descriptor; }

  public SourcePartitionStream getStream(int id) { return streams.get(id); }

  public SourcePartitionStream getStream(PartitionDescriptor descriptor) { return streams.get(descriptor.getId()); }

  public SourcePartitionStream[] getStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[streams.size()];
    return streams.values().toArray(array);
  }

  public void close() throws Exception {
  }

  static StorageDescriptor getSourceDescriptor(PartitionDescriptor streamDescriptor) {
    return streamDescriptor;
  }
}
