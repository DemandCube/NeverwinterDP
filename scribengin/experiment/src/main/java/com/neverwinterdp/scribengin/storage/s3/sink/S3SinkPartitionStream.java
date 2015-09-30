package com.neverwinterdp.scribengin.storage.s3.sink;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStream;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;

public class S3SinkPartitionStream implements SinkPartitionStream {
  private S3Folder s3StreamFolder ;
  private PartitionDescriptor descriptor;
  
  public S3SinkPartitionStream(S3Folder s3SinkFolder, PartitionDescriptor descriptor) {
    this.descriptor = descriptor;
    S3Storage storage = new S3Storage(descriptor);
    String streamKey = storage.getStreamKey(descriptor);
    if(s3SinkFolder.hasChild(streamKey)) {
      s3StreamFolder = s3SinkFolder.getS3Folder(streamKey); 
    } else {
      s3StreamFolder = s3SinkFolder.createFolder(streamKey) ;
    }
  }
  
  @Override
  public PartitionDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkPartitionStreamWriter getWriter() throws Exception {
    return new S3SinkPartitionStreamWriter(s3StreamFolder);
  }

  public void optimize() throws Exception {
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("S3SinkStream [streamS3Folder=");
    builder.append(s3StreamFolder);
    builder.append(", descriptor=");
    builder.append(descriptor);
    builder.append("]");
    return builder.toString();
  }
}