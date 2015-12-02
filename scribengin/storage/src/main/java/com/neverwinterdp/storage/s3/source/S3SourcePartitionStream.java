
package com.neverwinterdp.storage.s3.source;

import java.util.Comparator;

import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.s3.S3Client;
import com.neverwinterdp.storage.source.SourcePartitionStream;

public class S3SourcePartitionStream implements SourcePartitionStream {
  static public Comparator<S3SourcePartitionStream> COMPARATOR = new Comparator<S3SourcePartitionStream>() {
    public int compare(S3SourcePartitionStream s1, S3SourcePartitionStream s2) {
      return s1.getPartitionStreamConfig().getPartitionStreamId() - s2.getPartitionStreamConfig().getPartitionStreamId();
    }
  };
  
  private S3Client              s3Client ;
  private StorageConfig         storageConfig;
  private String                partitionName;
  private PartitionStreamConfig partitionStreamConfig ;
  
  public S3SourcePartitionStream(S3Client s3Client, StorageConfig sConfig, 
                                 String partitionName, PartitionStreamConfig pConfig) {
    this.s3Client              = s3Client;
    this.storageConfig         = sConfig;
    this.partitionName         = partitionName;
    this.partitionStreamConfig = pConfig;
  }

  public PartitionStreamConfig getPartitionStreamConfig() { return partitionStreamConfig ; }
  
  @Override
  public S3SourcePartitionStreamReader getReader(String name) throws Exception {
    return new S3SourcePartitionStreamReader(name, s3Client, storageConfig, partitionName, partitionStreamConfig) ;
  }
}
