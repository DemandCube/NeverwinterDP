package com.neverwinterdp.nstorage.segment;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;

abstract public class SegmentWriter {
  protected SegmentRegistry segReg;
  protected WriterDescriptor       writer;
  protected SegmentDescriptor      segment;
  
  public SegmentWriter(SegmentRegistry segReg, WriterDescriptor writer, SegmentDescriptor segment) {
    this.segReg  = segReg;
    this.writer  = writer;
    this.segment = segment;
  }

  public boolean isFull() throws IOException, RegistryException {
    return isBufferFull();
  }
  
  public void write(byte[] data) throws IOException, RegistryException {
    bufferWrite(data);
  }
  
  public void commit() throws IOException, RegistryException {
    prepareCommit();
    completeCommit();
  }

  public void rollback() throws IOException, RegistryException {
    bufferRollback();
  }
  
  public void close() throws IOException, RegistryException {
    bufferClose();
    segment.setFinishedTime(System.currentTimeMillis());
    segment.setStatus(SegmentDescriptor.Status.COMPLETE);
    segReg.finish(writer, segment);
  }
  
  public void prepareCommit() throws IOException, RegistryException {
    bufferPrepareCommit();
  }
  
  public void completeCommit() throws IOException, RegistryException {
    bufferCompleteCommit();
    segment.setDataSegmentNumOfRecords(bufferGetNumberOfWrittenRecords());
    segment.setDataSegmentLastCommitPos(bufferGetCurrentPosistion());
    segment.setDataSegmentCommitCount(segment.getDataSegmentCommitCount() + 1);
    segReg.commit(writer, segment);
  }
  
  
  abstract protected long  bufferGetNumberOfWrittenRecords() ;
  abstract protected long bufferGetCurrentPosistion() ;
  
  abstract protected boolean isBufferFull() throws IOException, RegistryException;
  
  abstract protected void bufferWrite(byte[] data) throws IOException, RegistryException ;
  
  abstract protected void bufferPrepareCommit() throws IOException ;
  abstract protected void bufferCompleteCommit() throws IOException ;
  abstract protected void bufferRollback() throws IOException ;
  abstract protected void bufferClose() throws IOException ;
}
