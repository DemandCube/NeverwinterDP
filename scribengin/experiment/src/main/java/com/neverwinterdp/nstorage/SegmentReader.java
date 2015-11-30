package com.neverwinterdp.nstorage;

import java.io.IOException;

import com.neverwinterdp.nstorage.SegmentDescriptor.Status;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

abstract public class SegmentReader {
  static public enum DataAvailability { YES, WAITING, EOS }
  
  private NStorageRegistry         registry;
  private NStorageReaderDescriptor readerDescriptor;
  private SegmentDescriptor        segment;
  private SegmentReadDescriptor    segmentReadDescriptor;
  private DataAvailability         lastCheckDataAvailability;
  private boolean                  complete = false;
  
  public SegmentReader(NStorageRegistry registry,NStorageReaderDescriptor readerDescriptor, SegmentDescriptor segment, SegmentReadDescriptor segmentReadDescriptor) {
    this.registry              = registry;
    this.readerDescriptor      = readerDescriptor;
    this.segment               = segment;
    this.segmentReadDescriptor = segmentReadDescriptor;
  }
  
  public SegmentDescriptor getSegmentDescriptor() { return segment; }
  
  public SegmentReadDescriptor getSegmentReadDescriptor() { return segmentReadDescriptor; }
  
  public NStorageReaderDescriptor getReaderDescriptor() { return readerDescriptor; }
  
  public boolean isComplete() { return this.complete ; }
  
  public DataAvailability getDataAvailability() throws IOException, RegistryException {
    if(lastCheckDataAvailability != null && lastCheckDataAvailability == DataAvailability.WAITING) {
      //refresh the segment information
      segment = registry.getSegmentBySegmentId(segment.getSegmentId());
    }
    lastCheckDataAvailability = updateDataAvailability();
    return lastCheckDataAvailability;
  }
  
  DataAvailability updateDataAvailability() throws IOException {
    long currentReadPos = getCurrentReadPosition();
    long lastCommitPos  = segment.getDataSegmentLastCommitPos();
    if(currentReadPos < lastCommitPos) {
      return DataAvailability.YES;
    } 
    
    Status status = segment.getStatus();
    if(status == Status.COMPLETE) {
      if(currentReadPos == lastCommitPos) {
        return DataAvailability.EOS;
      }
    } else if(status == Status.WRITING) {
      if(currentReadPos == lastCommitPos) {
        return DataAvailability.WAITING;
      }
    }
    throw new IOException("Unknown condition: status = " + status + ", current read position = " + currentReadPos + ", last commit pos= " + lastCommitPos);
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    return dataNextRecord();
  }
  
  abstract protected byte[] dataNextRecord() throws IOException;
  
  public void prepareCommit(Transaction trans) throws IOException, RegistryException {
    segmentReadDescriptor.setCommitReadDataPosition(getCurrentReadPosition());
    if(segment.getStatus() == SegmentDescriptor.Status.COMPLETE) {
      if(segment.getDataSegmentLastCommitPos() == segmentReadDescriptor.getCommitReadDataPosition()) {
        complete = true;
      }
    }
    registry.commit(trans, readerDescriptor, segment, segmentReadDescriptor, complete);
  }
  
  public void completeCommit(Transaction trans) throws IOException {
  }
  
  public void rollback(Transaction trans) throws IOException {
  }
  
  abstract protected long getCurrentReadPosition() ;
}