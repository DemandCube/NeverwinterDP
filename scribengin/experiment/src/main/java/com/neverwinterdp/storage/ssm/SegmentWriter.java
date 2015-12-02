package com.neverwinterdp.storage.ssm;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentWriter {
  protected SSMRegistry         registry;
  protected SSMWriterDescriptor writer;
  protected SegmentDescriptor        segment;
  private   boolean                  closed = false;
  private   long                     numberOfWrittenRecords;
  private   long                     maxSegmentSize;
  private   long                     maxBufferSize ;
  private   long                     numberOfUncommitRecords;
  
  public SegmentWriter(SSMRegistry registry, SSMWriterDescriptor writer, SegmentDescriptor segment) {
    this.registry = registry;
    this.writer   = writer;
    this.segment  = segment;
  }

  public void setMaxSegmentSize(long size) { this.maxSegmentSize = size; }
  
  public void setMaxBufferSize(long size) { this.maxBufferSize = size; }
  
  public boolean isFull() throws IOException, RegistryException {
    return bufferGetSegmentSize() > maxSegmentSize;
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
    if(bufferGetUncommitSize() > maxBufferSize) {
      throw new IOException("Cannot buffer more than " + maxBufferSize + " bytes. Call commit");
    }
    bufferWrite(data);
    numberOfWrittenRecords++;
    numberOfUncommitRecords++;
  }
  
  public void commit() throws IOException, RegistryException {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws IOException, RegistryException {
    bufferRollback();
    numberOfWrittenRecords = numberOfWrittenRecords - numberOfUncommitRecords ;
    numberOfUncommitRecords = 0;
  }
  
  
  public void prepareCommit() throws IOException, RegistryException {
    bufferPrepareCommit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    bufferCompleteCommit();
    segment.setDataSegmentNumOfRecords(bufferGetNumberOfWrittenRecords());
    segment.setDataSegmentLastCommitPos(bufferGetSegmentSize());
    segment.setDataSegmentCommitCount(segment.getDataSegmentCommitCount() + 1);
    registry.commit(writer, segment);
    numberOfUncommitRecords = 0;
  }
  
  public boolean isClosed() { return closed ; }
  
  public void close() throws IOException, RegistryException {
    bufferClose();
    segment.setFinishedTime(System.currentTimeMillis());
    segment.setStatus(SegmentDescriptor.Status.COMPLETE);
    registry.finish(writer, segment);
    closed = true;
  }
  
  protected long bufferGetNumberOfWrittenRecords() { return numberOfWrittenRecords; }

  protected long bufferGetNumberOfUncommitRecords() { return 0; }
  
  abstract protected long bufferGetSegmentSize() ;
  abstract protected long bufferGetUncommitSize() ;
  
  abstract protected void bufferWrite(byte[] data) throws IOException, RegistryException ;
  
  abstract protected void bufferPrepareCommit() throws IOException ;
  abstract protected void bufferCompleteCommit() throws IOException ;
  abstract protected void bufferRollback() throws IOException ;
  abstract protected void bufferClose() throws IOException ;
}
