package com.neverwinterdp.scribengin.storage.s3.sink;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectWriter;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;

public class S3SinkStreamWriter implements SinkStreamWriter {
  static private final int TIMEOUT = 10 * 60 * 1000;
  
  private S3Folder streamS3Folder;
  private String currentSegmentName;
  private S3ObjectWriter currentWriter ;

  

  public S3SinkStreamWriter(S3Folder streamS3Folder) throws IOException {
    this.streamS3Folder = streamS3Folder;
  }
  
  @Override
  synchronized public void append(DataflowMessage dataflowMessage) throws Exception {
    if(currentWriter == null) {
      currentWriter = createNewWriter();
    }
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(dataflowMessage);
    currentWriter.write(bytes);
  }

  @Override
  synchronized public void prepareCommit() throws Exception {
    if(currentWriter == null) return ;
    currentWriter.waitAndClose(TIMEOUT);
  }

  @Override
  synchronized  public void completeCommit() throws Exception {
    if(currentWriter == null) return ;
    ObjectMetadata metadata = currentWriter.getObjectMetadata();
    metadata.addUserMetadata("transaction", "complete");
    streamS3Folder.updateObjectMetadata(currentSegmentName, metadata);
    currentWriter = null;
  }

  @Override
  synchronized  public void commit() throws Exception {
    try {
      prepareCommit();
      completeCommit();
    } catch (Exception ex) {
      rollback();
      throw ex;
    }
  }

  @Override
  synchronized  public void rollback() throws Exception {
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
  synchronized public void close() throws Exception {
    if(currentWriter != null) rollback() ;
  }
}
