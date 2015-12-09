package com.neverwinterdp.ssm;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SSMWriter {
  protected SSMRegistry         registry;
  protected SSMWriterDescriptor writerDescriptor;
  private   SegmentWriter       currentSegWriter;
  private   long                maxSegmentSize = 128 * 1024 * 1024;
  private   long                maxBufferSize  =  32  * 1024 * 1024;
  private   boolean             closed = false;
  
  public SSMWriter(String clientId, SSMRegistry registry) throws RegistryException {
    this.registry         = registry;
    this.writerDescriptor = registry.createWriter(clientId);
  }
  
  public String getWriterId() { return writerDescriptor.getId(); }
  
  public SSMWriter setMaxSegmentSize(long size) {
    this.maxSegmentSize = size;
    return this;
  }
  
  public SSMWriter setMaxBufferSize(long size) {
    this.maxBufferSize = size;
    return this;
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
    if(currentSegWriter == null || currentSegWriter.isClosed()) {
      currentSegWriter = newSegmentWriter();
    } 
    currentSegWriter.write(data);
  }

  public void prepareCommit() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    if(currentSegWriter == null) return;
    currentSegWriter.prepareCommit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    if(currentSegWriter == null) return;
    currentSegWriter.completeCommit();
    if(currentSegWriter.isFull()) {
      currentSegWriter.close();
      currentSegWriter = null;
    }
  }

  public void commit() throws IOException, RegistryException {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    if(currentSegWriter == null) return;
    currentSegWriter.rollback();
  }
  
  public void close() throws IOException, RegistryException {
    if(currentSegWriter != null) {
      currentSegWriter.close();
      currentSegWriter = null;
    }
    closed = true;
  }
  
  public void remove() throws IOException, RegistryException {
    registry.removeWriter(writerDescriptor.getId());
  }
  
  public void closeAndRemove() throws IOException, RegistryException {
    close();
    remove();
  }
  
  public SegmentWriter newSegmentWriter() throws RegistryException, IOException  {
    SegmentDescriptor segment = registry.newSegment(writerDescriptor);
    SegmentWriter     segWriter = createSegmentWriter(writerDescriptor, segment) ;
    segWriter.setMaxSegmentSize(maxSegmentSize);
    segWriter.setMaxBufferSize(maxBufferSize);
    return segWriter;
  }
  
  abstract protected SegmentWriter createSegmentWriter(SSMWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException;

  void checkValidCurrentSegmentWriter() throws IOException {
    if(closed) {
      throw new IOException("The writer has been closed");
    }
  }
}
