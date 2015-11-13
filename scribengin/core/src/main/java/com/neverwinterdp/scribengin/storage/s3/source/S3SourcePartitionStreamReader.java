package com.neverwinterdp.scribengin.storage.s3.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectReader;
import com.neverwinterdp.scribengin.storage.s3.S3Storage;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;

public class S3SourcePartitionStreamReader implements SourcePartitionStreamReader {
  private String                name;
  private S3Client              s3Client;
  private StorageConfig         storageConfig;
  private String                partitionName;
  private PartitionStreamConfig partitionStreamConfig;

  private S3Folder     streamFolder ;
  private List<String> segments = new ArrayList<String>();
  
  private int currentSegmentPos = -1;
  private boolean endOfStream = false;
  
  private S3ObjectReader currentSegmenttReader;
  
  private int commitPoint;
  private int currPosition;
  private CommitPoint lastCommitInfo;

  public S3SourcePartitionStreamReader(String name, S3Client client, StorageConfig sConfig, 
                                       String partitionName, PartitionStreamConfig pConfig) throws Exception {
    this.name = name;
    this.s3Client = client;
    this.storageConfig = sConfig;
    this.partitionName = partitionName;
    this.partitionStreamConfig = pConfig;
    
    String bucketName  = storageConfig.attribute(S3Storage.BUCKET_NAME);
    String storagePath = storageConfig.attribute(S3Storage.STORAGE_PATH);
    
    String streamPath = storagePath + "/" + partitionName + "/partition-stream-" + pConfig.getPartitionStreamId();
    if(s3Client.hasKey(bucketName, streamPath)) {
      streamFolder = s3Client.getS3Folder(bucketName, streamPath);
      segments = streamFolder.getChildrenNames();
    }
  }

  public String getName() { return name; }

  public Record next(long maxWait) throws Exception {
    if(streamFolder == null) return null ;
    if(currentSegmenttReader == null) {
      currentSegmenttReader = nextSegmentReader();
    }
    
    if(currentSegmenttReader == null) return null ;
    
    if(currentSegmenttReader.hasNext()) {
      byte[] data = currentSegmenttReader.next();
      return JSONSerializer.INSTANCE.fromBytes(data, Record.class);
    } else {
      currentSegmenttReader.close();
      currentSegmenttReader = null ;
      return next(maxWait);
    }
  }

  public Record[] next(int size, long maxWait) throws Exception {
    List<Record> holder = new ArrayList<Record>();
    Record[] array = new Record[holder.size()];
    for (int i = 0; i < size; i++) {
      Record dataflowMessage = next(maxWait);
      if (dataflowMessage != null) holder.add(dataflowMessage);
      else break;
    }
    holder.toArray(array);
    return array;
  }

  public boolean isEndOfDataStream() { return endOfStream; }
  
  public void rollback() throws Exception {
    currPosition = commitPoint;
  }

  @Override
  public void prepareCommit() {
    
  }

  @Override
  public void completeCommit() {
   
  }

  public void commit() throws Exception {
    lastCommitInfo = new CommitPoint(name, commitPoint, currPosition);
    this.commitPoint = currPosition;
  }

  public CommitPoint getLastCommitInfo() {
    return this.lastCommitInfo;
  }

  public void close() throws Exception {
    if(currentSegmenttReader != null) {
      currentSegmenttReader.close();
    }
  }

  private S3ObjectReader nextSegmentReader() throws IOException {
    currentSegmentPos++;
    if (currentSegmentPos >= segments.size()) {
      endOfStream = true;
      return null;
    }
    String segment = segments.get(currentSegmentPos);
    S3Object s3Object = streamFolder.getS3Object(segment);
    S3ObjectReader reader = new S3ObjectReader(s3Object);
    return reader;
  }
}