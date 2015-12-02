package com.neverwinterdp.ssm;

import java.io.IOException;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.ssm.SegmentDescriptor.Status;

abstract public class SegmentReader {
  static public enum DataAvailability { YES, WAITING, EOS }
  
  private SSMRegistry         registry;
  private SSMReaderDescriptor readerDescriptor;
  private SegmentDescriptor        segment;
  private SegmentReadDescriptor    segmentReadDescriptor;
  private boolean                  complete = false;
  private long                     readRecordIndex;
  
  public SegmentReader(SSMRegistry registry,SSMReaderDescriptor readerDescriptor, SegmentDescriptor segment, SegmentReadDescriptor segmentReadDescriptor) {
    this.registry              = registry;
    this.readerDescriptor      = readerDescriptor;
    this.segment               = segment;
    this.segmentReadDescriptor = segmentReadDescriptor;
    this.readRecordIndex       = segmentReadDescriptor.getCommitReadRecordIndex() ;
  }
  
  public SegmentDescriptor getSegmentDescriptor() { return segment; }
  
  public SegmentReadDescriptor getSegmentReadDescriptor() { return segmentReadDescriptor; }
  
  public SSMReaderDescriptor getReaderDescriptor() { return readerDescriptor; }
  
  public boolean isComplete() { return this.complete ; }
  
  public boolean hasAvailableData() {
    return getCurrentReadPosition() < segment.getDataSegmentLastCommitPos();
  }
  
  public void updateSegmentDescriptor() throws RegistryException {
    segment = registry.getSegmentBySegmentId(segment.getSegmentId());
  }
  
  public DataAvailability updateAndGetDataAvailability() throws IOException {
    long currentReadPos = getCurrentReadPosition();
    long lastCommitPos  = segment.getDataSegmentLastCommitPos();
    if(currentReadPos < lastCommitPos) return DataAvailability.YES;
    
    Status status = segment.getStatus();
    if(status == Status.COMPLETE) {
      if(currentReadPos == lastCommitPos) return DataAvailability.EOS;
    } else if(status == Status.WRITING) {
      if(currentReadPos == lastCommitPos) return DataAvailability.WAITING;
    }
    throw new IOException("Unknown condition: status = " + status + ", current read position = " + currentReadPos + ", last commit pos= " + lastCommitPos);
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    byte[] record = dataNextRecord();
    readRecordIndex++ ;
    return record;
  }
  
  abstract protected byte[] dataNextRecord() throws IOException;
  
  public void prepareCommit(Transaction trans) throws IOException, RegistryException {
    segmentReadDescriptor.setCommitReadDataPosition(readRecordIndex);
    segmentReadDescriptor.setCommitReadDataPosition(getCurrentReadPosition());
    registry.commit(trans, readerDescriptor, segment, segmentReadDescriptor, complete);
  }
  
  public void completeCommit(Transaction trans) throws IOException {
    if(segment.getStatus() == SegmentDescriptor.Status.COMPLETE) {
      if(segment.getDataSegmentLastCommitPos() == segmentReadDescriptor.getCommitReadDataPosition()) {
        complete = true;
      }
    }
  }
  
  public void rollback(Transaction trans) throws IOException {
    readRecordIndex = segmentReadDescriptor.getCommitReadRecordIndex();
    rollback(readRecordIndex, segmentReadDescriptor.getCommitReadDataPosition());
  }
  
  abstract protected void rollback(long readRecordIndex, long pos) throws IOException;
  
  abstract protected long getCurrentReadPosition() ;
  
  public String toString() { 
    StringBuilder b = new StringBuilder();
    b.append("reader: \n").
      append("  ").append(readerDescriptor.toString()).append("\n").
      append("  ").append(segment.toString()).append("\n").
      append("  ").append(segmentReadDescriptor.toString());
    return b.toString();
  }
}