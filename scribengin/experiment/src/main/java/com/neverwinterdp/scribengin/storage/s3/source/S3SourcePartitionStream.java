
package com.neverwinterdp.scribengin.storage.s3.source;

import com.neverwinterdp.scribengin.storage.PartitionDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;

public class S3SourcePartitionStream implements SourcePartitionStream {
  private S3Client s3Client ;
  private PartitionDescriptor descriptor ;
  
  public S3SourcePartitionStream(S3Client s3Client, PartitionDescriptor descriptor) {
    this.s3Client= s3Client;
    this.descriptor = descriptor;
  }

  public PartitionDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourcePartitionStreamReader getReader(String name) throws Exception {
    return new S3SourcePartitionStreamReader(name, s3Client, descriptor) ;
  }
}
