package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class NStorageWriter {
  protected NStorageRegistry         registry;
  protected NStorageWriterDescriptor writerDescriptor;
  private   SegmentWriter            currentSegWriter;
  
  public NStorageWriter(String clientId, NStorageRegistry registry) throws RegistryException {
    this.registry         = registry;
    this.writerDescriptor = registry.createWriter(clientId);
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
    if(currentSegWriter == null || currentSegWriter.isClosed()) {
      currentSegWriter = newSegmentWriter();
    } else if(currentSegWriter.isFull()) {
      currentSegWriter.commit();
      currentSegWriter.close();
      currentSegWriter = newSegmentWriter();
    }
    currentSegWriter.write(data);
  }

  public void prepareCommit() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    currentSegWriter.prepareCommit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    currentSegWriter.completeCommit();
  }

  public void commit() throws IOException, RegistryException {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    currentSegWriter.rollback();
  }
  
  public void close() throws IOException, RegistryException {
    checkValidCurrentSegmentWriter();
    currentSegWriter.close();
  }
  
  public SegmentWriter newSegmentWriter() throws RegistryException, IOException  {
    SegmentDescriptor segment = registry.newSegment(writerDescriptor);
    SegmentWriter     segWriter = createSegmentWriter(writerDescriptor, segment) ;
    return segWriter;
  }
  
  abstract protected SegmentWriter createSegmentWriter(NStorageWriterDescriptor writer, SegmentDescriptor segment) throws RegistryException, IOException;

  void checkValidCurrentSegmentWriter() throws IOException {
    if(currentSegWriter == null || currentSegWriter.isClosed()) {
      throw new IOException("The writer has been closed");
    }
  }
}
