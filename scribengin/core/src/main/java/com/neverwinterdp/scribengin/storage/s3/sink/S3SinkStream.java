package com.neverwinterdp.scribengin.storage.s3.sink;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SinkStream implements SinkStream {
  private S3Folder s3StreamFolder ;
  private StreamDescriptor descriptor;
  
  public S3SinkStream(S3Folder s3SinkFolder, StreamDescriptor descriptor) {
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
  public StreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new S3SinkStreamWriter(s3StreamFolder);
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