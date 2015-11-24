package com.neverwinterdp.scribengin.storage.s3.sink;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectWriter;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.s3.S3StoragePartitioner;
import com.neverwinterdp.scribengin.storage.sink.SinkPartitionStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class S3SinkPartitionStreamWriter implements SinkPartitionStreamWriter {
  static final private String WORKER_ID = UUID.randomUUID().toString();
  static final private AtomicInteger SEGMENT_ID_TRACKER = new AtomicInteger();
  static final private int    TIMEOUT = 90 * 1000;
  
  private S3Client              s3Client;
  private StorageConfig         storageConfig;
  private PartitionStreamConfig partitionStreamConfig;
  private S3StoragePartitioner  partitioner ;
  
  private SegmentWriter         currentWriter ;

  public S3SinkPartitionStreamWriter(S3Client s3Client, StorageConfig sConfig, PartitionStreamConfig pConfig) throws IOException {
    this.s3Client              = s3Client;
    this.storageConfig         = sConfig;
    this.partitionStreamConfig = pConfig;
    this.partitioner           = getPartitioner(sConfig);
  }
  
  @Override
  public void append(Record dataflowMessage) throws Exception {
    if(currentWriter == null) {
      String bucketName          = storageConfig.attribute(S3Storage.BUCKET_NAME);
      String storageFolder       = storageConfig.attribute(S3Storage.STORAGE_PATH);
      String partition           = partitioner.getCurrentPartition();
      int    partitionStreamId   = partitionStreamConfig.getPartitionStreamId();
      
      String partitionPath = storageFolder + "/" + partition;
      if(!s3Client.hasKey(bucketName, partitionPath)) {
        s3Client.createS3Folder(bucketName, partitionPath);
      }
      String streamPath = partitionPath + "/partition-stream-" + partitionStreamId;
      currentWriter = new SegmentWriter(s3Client, bucketName, streamPath);
    }
    currentWriter.append(dataflowMessage);
  }

  @Override
  public void prepareCommit() throws Exception {
    if(currentWriter == null) return ;
    currentWriter.prepareCommit();
  }

  @Override
  public void completeCommit() throws Exception {
    if(currentWriter == null) return ;
    currentWriter.completeCommit(); 
    currentWriter = null ;
  }

  @Override
  public void commit() throws Exception {
    if(currentWriter == null) return ;  
    currentWriter.prepareCommit();
    currentWriter.completeCommit();
  }

  @Override
  public void rollback() throws Exception {
    if(currentWriter == null) return;
    currentWriter.rollback() ;
    currentWriter = null ;
  }

  @Override
  public void close() throws Exception {
    if(currentWriter != null) {
      currentWriter.close();
      currentWriter = null ;
    }
  }
  
  S3StoragePartitioner getPartitioner(StorageConfig sConfig) {
    S3StoragePartitioner partitioner = new S3StoragePartitioner.Default();
    String name = sConfig.attribute("partitioner");
    if("hourly".equals(name)) {
      partitioner = new S3StoragePartitioner.Hourly();
    } else if("15min".equals(name)) {
      partitioner = new S3StoragePartitioner.Every15Min();
    }
    return partitioner;
  }
  
  static public class SegmentWriter {
    private String         currentSegmentName;
    private S3ObjectWriter currentWriter ;
    private S3Folder       streamS3Folder ;
    
    public SegmentWriter(S3Client s3Client, String bucketName, String folder) throws IOException {
      if(!s3Client.hasKey(bucketName, folder)) {
        streamS3Folder = s3Client.createS3Folder(bucketName, folder);
      } else {
        streamS3Folder = s3Client.getS3Folder(bucketName, folder);
      }
    }
    
    public void append(Record record) throws Exception {
      if(currentWriter == null) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("transaction", "prepare");
        currentSegmentName = "segment-" + WORKER_ID + "-" + SEGMENT_ID_TRACKER.incrementAndGet();
        currentWriter      = streamS3Folder.createObjectWriter(currentSegmentName, metadata);
      }
      byte[] bytes = JSONSerializer.INSTANCE.toBytes(record);
      currentWriter.write(bytes);
    }

    public void prepareCommit() throws Exception {
      if(currentWriter == null) return ;
      currentWriter.waitAndClose(TIMEOUT);
    }

    public void completeCommit() throws Exception {
      if(currentWriter == null) return ;
      ObjectMetadata metadata = currentWriter.getObjectMetadata();
      metadata.addUserMetadata("transaction", "complete");
      streamS3Folder.updateObjectMetadata(currentSegmentName, metadata);
      currentWriter = null;
    }

    public void commit() throws Exception {
      prepareCommit();
      completeCommit();
    }

    public void rollback() throws Exception {
      if(currentWriter == null) return;
      currentWriter.forceClose() ;
      streamS3Folder.deleteChild(currentSegmentName);
      currentWriter = null ;
    }
    
    public void close() throws Exception {
      if(currentWriter != null) rollback() ;
    }
  }
}