package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.Segment;
import com.neverwinterdp.scribengin.storage.hdfs.Storage;
import com.neverwinterdp.scribengin.storage.hdfs.StorageWriter;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class HDFSSinkStreamWriter extends StorageWriter<DataflowMessage> implements SinkStreamWriter {
  private StreamDescriptor descriptor ;
  private long             smallDataSizeAccumulate  = 0;
  private long             mediumDataSizeAccumulate = 0;
  
  public HDFSSinkStreamWriter(Storage<DataflowMessage> storage, StreamDescriptor descriptor) throws IOException {
    super(storage);
    this.descriptor = descriptor ;
    smallDataSizeAccumulate  = storage.getBufferSegments().dataSize();
    mediumDataSizeAccumulate = storage.getSmallSegments().dataSize();
  }

  public StreamDescriptor getDescriptor() { return descriptor; }
  
  @Override
  public void append(DataflowMessage obj) throws Exception {
    super.append(obj);
    long dataSize = obj.getData().length + obj.getKey().length() ;
    smallDataSizeAccumulate  += dataSize ;
    mediumDataSizeAccumulate += dataSize ;
  }
  
  @Override
  public void completeCommit() throws Exception {
    super.completeCommit();
    if(smallDataSizeAccumulate >= Segment.SMALL_DATASIZE_THRESHOLD) {
      getStorage().optimizeBufferSegments();
      smallDataSizeAccumulate = 0 ;
    }
    if(mediumDataSizeAccumulate >= Segment.MEDIUM_DATASIZE_THRESHOLD) {
      getStorage().optimizeSmallSegments();
      mediumDataSizeAccumulate = 0 ;
    }
  }
}