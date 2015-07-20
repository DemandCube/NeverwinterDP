package com.neverwinterdp.scribengin.storage.s3.sink;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectWriter;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class S3SinkStreamWriter implements SinkStreamWriter {
  private S3Folder streamS3Folder;
  private String currentSegmentName;
  private S3ObjectWriter currentWriter
  ;

  private final int TIMEOUT = 5 * 60 * 1000;

  public S3SinkStreamWriter(S3Folder streamS3Folder) throws IOException {
    this.streamS3Folder = streamS3Folder;
  }
  
  @Override
  public void append(Record record) throws Exception {
    if(currentWriter == null) {
      currentWriter = createNewWriter();
    }
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(record);
    currentWriter.write(bytes);
  }

  @Override
  public void prepareCommit() throws Exception {
    if(currentWriter == null) return ;
    currentWriter.waitAndClose(TIMEOUT);
  }

  @Override
  public void completeCommit() throws Exception {
    if(currentWriter == null) return ;
    ObjectMetadata metadata = currentWriter.getObjectMetadata();
    metadata.addUserMetadata("transaction", "complete");
    streamS3Folder.updateObjectMetadata(currentSegmentName, metadata);
    currentWriter = null;
  }

  @Override
  public void commit() throws Exception {
    try {
      prepareCommit();
      completeCommit();
    } catch (Exception ex) {
      rollback();
      throw ex;
    }
  }

  @Override
  public void rollback() throws Exception {
    if(currentWriter == null) return;
    currentWriter.forceClose() ;
    streamS3Folder.deleteObject(currentSegmentName);
  }

  private S3ObjectWriter createNewWriter() throws IOException {
    currentSegmentName = "segment-" + UUID.randomUUID().toString();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.addUserMetadata("transaction", "prepare");
    currentWriter = streamS3Folder.createObjectWriter(currentSegmentName, metadata);
    return currentWriter;
  }

  @Override
  public void close() throws Exception {
    if(currentWriter != null) rollback() ;
  }
}
